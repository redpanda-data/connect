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
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"sync"
	"time"

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
		v, err := conf.FieldInt(bopFieldRunner, bopFieldContextSize)
		if err != nil {
			return nil, err
		}
		opts.NumCtx = v
	}
	if conf.Contains(bopFieldRunner, bopFieldBatchSize) {
		v, err := conf.FieldInt(bopFieldRunner, bopFieldBatchSize)
		if err != nil {
			return nil, err
		}
		opts.NumBatch = v
	}
	if conf.Contains(bopFieldRunner, bopFieldGPULayers) {
		v, err := conf.FieldInt(bopFieldRunner, bopFieldGPULayers)
		if err != nil {
			return nil, err
		}
		opts.NumGPU = v
	}
	if conf.Contains(bopFieldRunner, bopFieldThreads) {
		v, err := conf.FieldInt(bopFieldRunner, bopFieldThreads)
		if err != nil {
			return nil, err
		}
		opts.NumThread = v
	}
	if conf.Contains(bopFieldRunner, bopFieldUseMMap) {
		v, err := conf.FieldBool(bopFieldRunner, bopFieldUseMMap)
		if err != nil {
			return nil, err
		}
		opts.UseMMap = &v
	}
	if conf.Contains(bopFieldRunner, bopFieldUseMLock) {
		v, err := conf.FieldBool(bopFieldRunner, bopFieldUseMLock)
		if err != nil {
			return nil, err
		}
		opts.UseMLock = v
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
			if bytes.HasPrefix(line, []byte("[GIN]")) {
				co.logger.Tracef("%s", line)
			} else {
				co.logger.Debugf("%s", line)
			}
		}
		co.buffer = co.buffer[idx+1:]
	}
	return len(b), nil
}

type baseOllamaProcessor struct {
	model  string
	opts   map[string]any
	ticket singleton.Ticket
	client *api.Client
	logger *service.Logger
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

func newBaseProcessor(conf *service.ParsedConfig, mgr *service.Resources) (p *baseOllamaProcessor, err error) {
	p = &baseOllamaProcessor{}
	p.logger = mgr.Logger()
	p.model, err = conf.FieldString(bopFieldModel)
	if err != nil {
		return
	}
	p.opts, err = extractOptions(conf)
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
		if ollamaProcess == nil {
			err = fmt.Errorf("running a local ollama process is not supported on %s, please specify a `%s`", runtime.GOOS, bopFieldServerAddress)
			return
		}
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
	p.logger.Infof("Pulling %q", p.model)
	if err = p.pullModel(context.Background()); err != nil {
		return
	}
	p.logger.Infof("Finished pulling %q", p.model)
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
		o.logger.Tracef("Pulling %q: %s [%s/%s]", o.model, resp.Status, humanize.Bytes(uint64(resp.Completed)), humanize.Bytes(uint64(resp.Total)))
		return nil
	})
}

func (o *baseOllamaProcessor) Close(ctx context.Context) error {
	if ollamaProcess == nil {
		return nil
	}
	return ollamaProcess.Close(ctx, o.ticket)
}
