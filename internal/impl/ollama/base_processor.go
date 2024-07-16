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
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	bopFieldServerAddress = "server_address"
	bopFieldOllamaDir     = "ollama_directory"
	bopFieldModel         = "model"
)

func extractEmbeddedServer(fsys *service.FS, serverPath string) error {
	_, err := fsys.Stat(serverPath)
	if err == nil {
		return nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		file, err := fsys.OpenFile(serverPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return err
		}
		writer, isw := file.(io.Writer)
		if !isw {
			return errors.New("failed to write binary")
		}
		if _, err = writer.Write(embeddedServer); err != nil {
			return err
		}
		return file.Close()
	}
	return err
}

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

type baseOllamaProcessor struct {
	model  string
	cmd    *exec.Cmd
	client *api.Client
	logger *service.Logger
}

func (o *baseOllamaProcessor) startServer(fs *service.FS, tmpDir string) (*exec.Cmd, error) {
	serverPath, err := exec.LookPath("ollama")
	if errors.Is(err, exec.ErrNotFound) {
		if embeddedServer == nil {
			return nil, errors.New("embedded ollama not supported for this platform - please specify a server_address")
		}
		serverPath = path.Join(tmpDir, "ollama")
		if err = extractEmbeddedServer(fs, serverPath); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	proc := exec.Command(serverPath, "serve")
	proc.Stdout = &commandOutput{logger: o.logger}
	proc.Stderr = &commandOutput{logger: o.logger}
	if err = proc.Start(); err != nil {
		return nil, err
	}
	return proc, nil
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
		tmpDir := os.TempDir()
		if conf.Contains(bopFieldOllamaDir) {
			tmpDir, err = conf.FieldString(bopFieldOllamaDir)
			if err != nil {
				return
			}
		}
		p.cmd, err = p.startServer(mgr.FS(), tmpDir)
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
	if o.cmd == nil || o.cmd.Process == nil {
		return nil
	}
	if err := o.cmd.Process.Kill(); err != nil {
		return err
	}
	return o.cmd.Wait()
}
