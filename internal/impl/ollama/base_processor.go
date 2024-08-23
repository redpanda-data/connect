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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/ollama/ollama/api"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/singleton"
)

const (
	bopFieldServerAddress  = "server_address"
	bopFieldModel          = "model"
	bopFieldCacheDirectory = "cache_directory"
	bopFieldDownloadURL    = "download_url"
)

func commonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(bopFieldServerAddress).
			Description("The address of the Ollama server to use. Leave the field blank and the processor starts and runs a local Ollama server or specify the address of your own local or remote server.").
			Example("http://127.0.0.1:11434").
			Optional(),
		service.NewStringField(bopFieldCacheDirectory).
			Description("If `" + bopFieldServerAddress + "` is not set - the directory to download the ollama binary and use as a model cache.").
			Example("/opt/cache/connect/ollama").
			Advanced().
			Optional(),
		service.NewStringField(bopFieldDownloadURL).
			Description("If `" + bopFieldServerAddress + "` is not set - the URL to download the ollama binary from. Defaults to the offical Ollama GitHub release for this platform.").
			Advanced().
			Optional(),
	}
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
			if bytes.HasPrefix([]byte("[GIN]"), line) {
				co.logger.Debugf("%s", line)
			} else {
				co.logger.Infof("%s", line)
			}
		}
		co.buffer = co.buffer[idx+1:]
	}
	return len(b), nil
}

type key int

const (
	configKey key = key(1)
)

type runOllamaConfig struct {
	logger      *service.Logger
	fs          *service.FS
	cacheDir    string
	downloadURL string
}

// lookPath is very similar to exec.LookPath, except that it uses service.FS filesystem
// abstractions.
func (c *runOllamaConfig) lookPath(file string) (string, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return "", errors.ErrUnsupported
	}
	path := os.Getenv("PATH")
	for _, dir := range filepath.SplitList(path) {
		if !filepath.IsAbs(path) {
			continue
		}
		path := filepath.Join(dir, file)
		p, err := c.fs.Stat(path)
		if err != nil || p.IsDir() {
			continue
		}
		// Check that the file is executable
		if err := syscall.Access(path, 0x1); err == nil {
			return path, nil
		}
	}
	return "", exec.ErrNotFound
}

func (c *runOllamaConfig) downloadOllama(ctx context.Context, path string) error {
	var url string
	if c.downloadURL == "" {
		const baseURL string = "https://github.com/ollama/ollama/releases/download/v0.3.6/ollama"
		switch runtime.GOOS {
		case "darwin":
			// They ship an universal executable for darwin
			url = baseURL + "-darwin"
		case "linux":
			url = fmt.Sprintf("%s-%s-%s", baseURL, runtime.GOOS, runtime.GOARCH)
		default:
			return fmt.Errorf("automatic download of ollama is not supported on %s, please download ollama manually", runtime.GOOS)
		}
	} else {
		url = c.downloadURL
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to download ollama binary: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download ollama binary: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to download ollama binary: status_code=%d", resp.StatusCode)
	}
	ollama, err := c.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		return fmt.Errorf("unable to create file for ollama binary download: %w", err)
	}
	defer ollama.Close()
	w, ok := (ollama).(io.Writer)
	if !ok {
		return errors.New("unable to download ollama binary to filesystem")
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

var ollamaProcess = singleton.New(singleton.Config[*exec.Cmd]{
	Constructor: func(ctx context.Context) (*exec.Cmd, error) {
		cfg, ok := ctx.Value(configKey).(runOllamaConfig)
		if !ok {
			return nil, errors.New("missing config")
		}
		serverPath, err := cfg.lookPath("ollama")
		if errors.Is(err, exec.ErrNotFound) {
			serverPath = path.Join(cfg.cacheDir, "ollama")
			if err := os.MkdirAll(cfg.cacheDir, 0777); err != nil {
				return nil, err
			}
			if _, err = os.Stat(serverPath); errors.Is(err, os.ErrNotExist) {
				err = backoff.Retry(func() error {
					cfg.logger.Infof("downloading ollama to %s", serverPath)
					return cfg.downloadOllama(ctx, serverPath)
				}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
			}
			if err != nil {
				return nil, err
			}
			cfg.logger.Info("ollama download complete")
		} else if err != nil {
			return nil, err
		}
		cfg.logger.Tracef("starting ollama subprocess at %s", serverPath)
		proc := exec.Command(serverPath, "serve")
		proc.Env = append(os.Environ(), "OLLAMA_MODELS="+cfg.cacheDir)
		proc.Stdout = &commandOutput{logger: cfg.logger}
		proc.Stderr = &commandOutput{logger: cfg.logger}
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
		var cacheDir string
		if conf.Contains(bopFieldCacheDirectory) {
			cacheDir, err = conf.FieldString(bopFieldCacheDirectory)
			if err != nil {
				return
			}
		} else {
			cacheDir, err = os.UserCacheDir()
			if err != nil {
				cacheDir = os.TempDir()
			}
			cacheDir = path.Join(cacheDir, "ollama")
		}
		var downloadURL string
		if conf.Contains(bopFieldDownloadURL) {
			downloadURL, err = conf.FieldString(bopFieldDownloadURL)
			if err != nil {
				return
			}
		}
		ctx := context.Background()
		ctx = context.WithValue(ctx, configKey, runOllamaConfig{
			logger:      mgr.Logger(),
			cacheDir:    cacheDir,
			fs:          mgr.FS(),
			downloadURL: downloadURL,
		})
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
