// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ollama

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"os/exec"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/singleton"
)

const (
	bopFieldServerAddress = "server_address"
	bopFieldModel         = "model"
)

type commandOutput struct {
	logger *service.Logger
	buffer []byte
	mu     sync.Mutex
}

func (co *commandOutput) Write(b []byte) (int, error) {
	co.mu.Lock()
	defer co.mu.Unlock()
	co.buffer = append(co.buffer, b...)
	for {
		idx := bytes.IndexByte(co.buffer, '\n')
		if idx == -1 {
			break
		}
		line := co.buffer[:idx]
		if len(line) > 0 {
			co.logger.Debugf("%s", line)
		}
		co.buffer = co.buffer[idx+1:]
	}
	return len(b), nil
}

type key int

const loggerKey key = key(1)

var ollamaProcess = singleton.New(singleton.Config[*exec.Cmd]{
	Constructor: func(ctx context.Context) (*exec.Cmd, error) {
		serverPath, err := exec.LookPath("ollama")
		if errors.Is(err, exec.ErrNotFound) {
			return nil, errors.New("ollama binary not found in PATH")
		} else if err != nil {
			return nil, err
		}
		proc := exec.Command(serverPath, "serve")
		logger, ok := ctx.Value(loggerKey).(*service.Logger)
		if ok {
			proc.Stdout = &commandOutput{logger: logger}
			proc.Stderr = &commandOutput{logger: logger}
		}
		if err = proc.Start(); err != nil {
			return nil, err
		}
		return proc, nil
	},
	Destructor: func(ctx context.Context, cmd *exec.Cmd) error {
		if cmd.Process == nil {
			return nil
		}
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
		return cmd.Wait()
	}})

type baseOllamaProcessor struct {
	model  string
	ticket singleton.Ticket
	client *api.Client
	logger *service.Logger
}

func newBaseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p *baseOllamaProcessor, err error) {
	p = &baseOllamaProcessor{}
	p.logger = mgr.Logger()
	p.model, err = conf.FieldString(bopFieldModel)
	if err != nil {
		return
	}
	if conf.Contains(bopFieldServerAddress) {
		var a string
		a, err = conf.FieldString(bopFieldServerAddress)
		if err != nil {
			return
		}
		var u *url.URL
		u, err = url.Parse(a)
		if err != nil {
			return
		}
		p.client = api.NewClient(u, http.DefaultClient)
	} else {
		ctx := context.Background()
		ctx = context.WithValue(ctx, loggerKey, mgr.Logger())
		_, p.ticket, err = ollamaProcess.Acquire(ctx)
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				_ = p.Close(context.Background())
			}
		}()
		p.client, err = api.ClientFromEnvironment()
		if err != nil {
			return
		}
	}
	if err = p.waitForServer(context.Background()); err != nil {
		return
	}
	if err = p.pullModel(context.Background()); err != nil {
		return
	}
	return
}

func (o *baseOllamaProcessor) waitForServer(ctx context.Context) error {
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-timeout:
			return errors.New("timed out waiting for server to start")
		case <-tick.C:
			if err := o.client.Heartbeat(ctx); err == nil {
				return nil // server has started
			}
		}
	}
}

func (o *baseOllamaProcessor) pullModel(ctx context.Context) error {
	pr := api.PullRequest{
		Model: o.model,
	}
	return o.client.Pull(ctx, &pr, func(resp api.ProgressResponse) error {
		o.logger.Infof("Pulling %q: %s [%s/%s]", o.model, resp.Status, humanize.Bytes(uint64(resp.Completed)), humanize.Bytes(uint64(resp.Total)))
		return nil
	})
}

func (o *baseOllamaProcessor) Close(ctx context.Context) error {
	return ollamaProcess.Close(ctx, o.ticket)
}
