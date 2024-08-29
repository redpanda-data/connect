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
	"encoding/json"
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

	bopFieldRunner = "runner"
	// Runner fields
	bopFieldContextSize = "context_size"
	bopFieldBatchSize   = "batch_size"
	bopFieldGPULayers   = "gpu_layers"
	bopFieldThreads     = "threads"
	bopFieldLowVRAM     = "low_vram"
	bopFieldUseMMap     = "use_mmap"
	bopFieldUseMLock    = "use_mlock"
)

func commonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewObjectField(
			bopFieldRunner,
			service.NewIntField(bopFieldContextSize).
				Optional().
				Description("Sets the size of the context window used to generate the next token. Using a larger context window uses more memory and takes longer to processor."),
			service.NewIntField(bopFieldBatchSize).
				Optional().
				Description("The maximum number of requests to process in parallel."),
			service.NewIntField(bopFieldGPULayers).
				Optional().
				Advanced().
				Description("This option allows offloading some layers to the GPU for computation. This generally results in increased performance. By default, the runtime decides the number of layers dynamically."),
			service.NewIntField(bopFieldThreads).
				Optional().
				Advanced().
				Description("Set the number of threads to use during generation. For optimal performance, it is recommended to set this value to the number of physical CPU cores your system has. By default, the runtime decides the optimal number of threads."),
			service.NewBoolField(bopFieldUseMMap).
				Optional().
				Advanced().
				Description("Map the model into memory. This is only support on unix systems and allows loading only the necessary parts of the model as needed."),
			service.NewBoolField(bopFieldUseMLock).
				Optional().
				Advanced().
				Description("Lock the model in memory, preventing it from being swapped out when memory-mapped. This option can improve performance but reduces some of the advantages of memory-mapping because it uses more RAM to run and can slow down load times as the model loads into RAM."),
		).Optional().Description(`Options for the model runner that are used when the model is first loaded into memory.`),
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

func extractOptions(conf *service.ParsedConfig) (map[string]any, error) {
	opts := api.Options{}
	if conf.Contains(ocpFieldMaxTokens) {
		v, err := conf.FieldInt(ocpFieldMaxTokens)
		if err != nil {
			return nil, err
		}
		opts.NumPredict = v
	}
	if conf.Contains(ocpFieldTemp) {
		v, err := conf.FieldFloat(ocpFieldTemp)
		if err != nil {
			return nil, err
		}
		opts.Temperature = float32(v)
	}
	if conf.Contains(ocpFieldNumKeep) {
		v, err := conf.FieldInt(ocpFieldNumKeep)
		if err != nil {
			return nil, err
		}
		opts.NumKeep = v
	}
	if conf.Contains(ocpFieldSeed) {
		v, err := conf.FieldInt(ocpFieldSeed)
		if err != nil {
			return nil, err
		}
		opts.Seed = v
	}
	if conf.Contains(ocpFieldTopK) {
		v, err := conf.FieldInt(ocpFieldTopK)
		if err != nil {
			return nil, err
		}
		opts.TopK = v
	}
	if conf.Contains(ocpFieldTopP) {
		v, err := conf.FieldFloat(ocpFieldTopP)
		if err != nil {
			return nil, err
		}
		opts.TopP = float32(v)
	}
	if conf.Contains(ocpFieldRepeatPenalty) {
		v, err := conf.FieldFloat(ocpFieldTopP)
		if err != nil {
			return nil, err
		}
		opts.RepeatPenalty = float32(v)
	}
	if conf.Contains(ocpFieldFrequencyPenalty) {
		v, err := conf.FieldFloat(ocpFieldFrequencyPenalty)
		if err != nil {
			return nil, err
		}
		opts.FrequencyPenalty = float32(v)
	}
	if conf.Contains(ocpFieldPresencePenalty) {
		v, err := conf.FieldFloat(ocpFieldPresencePenalty)
		if err != nil {
			return nil, err
		}
		opts.PresencePenalty = float32(v)
	}
	if conf.Contains(ocpFieldStop) {
		v, err := conf.FieldStringList(ocpFieldStop)
		if err != nil {
			return nil, err
		}
		opts.Stop = v
	}
	if conf.Contains(bopFieldRunner, bopFieldContextSize) {
		v, err := conf.FieldInt(ocpFieldStop, bopFieldContextSize)
		if err != nil {
			return nil, err
		}
		opts.Runner.NumCtx = v
	}
	if conf.Contains(bopFieldRunner, bopFieldBatchSize) {
		v, err := conf.FieldInt(ocpFieldStop, bopFieldBatchSize)
		if err != nil {
			return nil, err
		}
		opts.Runner.NumBatch = v
	}
	if conf.Contains(bopFieldRunner, bopFieldGPULayers) {
		v, err := conf.FieldInt(ocpFieldStop, bopFieldGPULayers)
		if err != nil {
			return nil, err
		}
		opts.Runner.NumGPU = v
	}
	if conf.Contains(bopFieldRunner, bopFieldThreads) {
		v, err := conf.FieldInt(ocpFieldStop, bopFieldThreads)
		if err != nil {
			return nil, err
		}
		opts.Runner.NumThread = v
	}
	if conf.Contains(bopFieldRunner, bopFieldUseMMap) {
		v, err := conf.FieldBool(ocpFieldStop, bopFieldUseMMap)
		if err != nil {
			return nil, err
		}
		opts.Runner.UseMMap = &v
	}
	if conf.Contains(bopFieldRunner, bopFieldUseMLock) {
		v, err := conf.FieldBool(ocpFieldStop, bopFieldUseMLock)
		if err != nil {
			return nil, err
		}
		opts.Runner.UseMLock = v
	}
	// The API for some reason doesn't use the strongly typed option, but a generic map,
	// so roundtrip it via JSON so we don't have to manually map the names to their JSON fields.
	b, err := json.Marshal(&opts)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize model options to JSON: %w", err)
	}
	serializedOpts := make(map[string]any)
	if err = json.Unmarshal(b, &serializedOpts); err != nil {
		return nil, fmt.Errorf("unable to serialize model options to JSON: %w", err)
	}
	return serializedOpts, nil
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
	opts   map[string]any
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
	p.opts, err = extractOptions(conf)
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
