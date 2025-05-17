// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redpanda

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	dtpFieldModulePath     = "module_path"
	dtpFieldInputKey       = "input_key"
	dtpFieldOutputKey      = "output_key"
	dtpFieldInputHeaders   = "input_headers"
	dtpFieldOutputMetadata = "output_metadata"
	dtpFieldTimestamp      = "timestamp"
	dtpFieldTimeout        = "timeout"
	dtpFieldMaxMemoryPages = "max_memory_pages"
	wasmPageSize           = 64 * humanize.KiByte
	dtpDefaultMaxMemory    = 100 * humanize.MiByte
)

func dataTransformProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Summary("Executes a Redpanda Data Transform as a processor").
		Description(`
This processor executes a Redpanda Data Transform WebAssembly module, calling OnRecordWritten for each message being processed.

You can find out about how transforms work here: https://docs.redpanda.com/current/develop/data-transforms/how-transforms-work/[https://docs.redpanda.com/current/develop/data-transforms/how-transforms-work/^]
`).
		Field(service.NewStringField(dtpFieldModulePath).
			Description("The path of the target WASM module to execute.")).
		Field(service.NewInterpolatedStringField(dtpFieldInputKey).
			Description("An optional key to populate for each message.").Optional()).
		Field(service.NewStringField(dtpFieldOutputKey).
			Description("An optional name of metadata for an output message key.").Optional()).
		Field(service.NewMetadataFilterField(dtpFieldInputHeaders).
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewMetadataFilterField(dtpFieldOutputMetadata).
			Description("Determine which (if any) message headers should be added to the output as metadata.").
			Optional()).
		Field(service.NewInterpolatedStringField(dtpFieldTimestamp).
			Description("An optional timestamp to set for each message. When left empty, the current timestamp is used.").
			Example(`${! timestamp_unix() }`).
			Example(`${! metadata("kafka_timestamp_ms") }`).
			Optional().
			Advanced()).
		Field(service.NewDurationField(dtpFieldTimeout).
			Description("The maximum period of time for a message to be processed").
			Default("10s").
			Advanced()).
		Field(service.NewIntField(dtpFieldMaxMemoryPages).
			Description("The maximum amount of wasm memory pages (64KiB) that an individual wasm module instance can use").
			Default(dtpDefaultMaxMemory / wasmPageSize).
			Advanced()).
		Version("4.31.0")
}

func init() {
	err := service.RegisterBatchProcessor(
		"redpanda_data_transform", dataTransformProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newDataTransformProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type dataTransformConfig struct {
	inputKey       *service.InterpolatedString
	outputKeyField *string
	timestamp      *service.InterpolatedString
	inputMetadata  *service.MetadataFilter
	outputMetadata *service.MetadataFilter

	timeout        time.Duration
	maxMemoryPages int
}

//------------------------------------------------------------------------------

type dataTransformEnginePool struct {
	log           *service.Logger
	wasmBinary    wazero.CompiledModule
	runtimeConfig wazero.RuntimeConfig
	modulePool    sync.Pool
	cfg           dataTransformConfig
}

func newDataTransformProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*dataTransformEnginePool, error) {
	pathStr, err := conf.FieldString(dtpFieldModulePath)
	if err != nil {
		return nil, err
	}

	file, err := mgr.FS().Open(pathStr)
	if err != nil {
		return nil, err
	}
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var cfg dataTransformConfig

	if conf.Contains(dtpFieldInputKey) {
		inputKey, err := conf.FieldInterpolatedString(dtpFieldInputKey)
		if err != nil {
			return nil, err
		}
		cfg.inputKey = inputKey
	}

	if conf.Contains(dtpFieldOutputKey) {
		inputKey, err := conf.FieldString(dtpFieldOutputKey)
		if err != nil {
			return nil, err
		}
		cfg.outputKeyField = &inputKey
	}

	if conf.Contains(dtpFieldInputHeaders) {
		inputMetadata, err := conf.FieldMetadataFilter(dtpFieldInputHeaders)
		if err != nil {
			return nil, err
		}
		cfg.inputMetadata = inputMetadata
	}

	if conf.Contains(dtpFieldOutputMetadata) {
		outputMetadata, err := conf.FieldMetadataFilter(dtpFieldOutputMetadata)
		if err != nil {
			return nil, err
		}
		cfg.outputMetadata = outputMetadata
	}

	if conf.Contains(dtpFieldTimestamp) {
		ts, err := conf.FieldInterpolatedString(dtpFieldTimestamp)
		if err != nil {
			return nil, err
		}
		cfg.timestamp = ts
	}

	timeout, err := conf.FieldDuration(dtpFieldTimeout)
	if err != nil {
		return nil, err
	}
	cfg.timeout = timeout

	maxMemoryPages, err := conf.FieldInt(dtpFieldMaxMemoryPages)
	if err != nil {
		return nil, err
	}
	cfg.maxMemoryPages = maxMemoryPages

	return newDataTransformProcessor(fileBytes, cfg, mgr)
}

func newDataTransformProcessor(wasmBinary []byte, cfg dataTransformConfig, mgr *service.Resources) (*dataTransformEnginePool, error) {
	ctx := context.Background()
	runtimeCfg := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithCompilationCache(wazero.NewCompilationCache()).
		WithMemoryLimitPages(uint32(cfg.maxMemoryPages))
	r := wazero.NewRuntimeWithConfig(ctx, runtimeCfg)
	cm, err := r.CompileModule(ctx, wasmBinary)
	if err != nil {
		// Still cleanup but ignore errors as it would mask the compilation failure
		_ = r.Close(ctx)
		return nil, err
	}
	err = r.Close(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Validate more ABI contract than just memory
	_, ok := cm.ExportedMemories()["memory"]
	if !ok {
		return nil, errors.New("missing exported Wasm memory")
	}
	proc := &dataTransformEnginePool{
		log:           mgr.Logger(),
		modulePool:    sync.Pool{},
		runtimeConfig: runtimeCfg,
		wasmBinary:    cm,
		cfg:           cfg,
	}
	// Ensure we can create at least one module runner.
	modRunner, err := proc.newModule()
	if err != nil {
		return nil, err
	}

	proc.modulePool.Put(modRunner)
	return proc, nil
}

func (p *dataTransformEnginePool) newModule() (engine *dataTransformEngine, err error) {
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, p.runtimeConfig)
	engine = &dataTransformEngine{
		log:       p.log,
		cfg:       &p.cfg,
		runtime:   r,
		hostChan:  make(chan any),
		guestChan: make(chan any),
		procErr:   nil,
	}
	defer func() {
		if err != nil {
			engine.runtime.Close(context.Background())
		}
	}()

	builder := r.NewHostModuleBuilder("redpanda_transform")
	for name, ctor := range transformHostFunctions {
		builder = builder.NewFunctionBuilder().WithFunc(ctor(engine)).Export(name)
	}
	if _, err = builder.Instantiate(ctx); err != nil {
		return
	}

	if _, err = wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return
	}
	cfg := wazero.NewModuleConfig().
		WithStartFunctions().
		WithArgs("transform").
		WithName("transform").
		WithEnv("REDPANDA_INPUT_TOPIC", "benthos")
	for i := 0; i < 8; i += 1 {
		cfg = cfg.WithEnv(fmt.Sprintf("REDPANDA_OUTPUT_TOPIC_%d", i), fmt.Sprintf("output_%d", i))
	}
	if engine.mod, err = r.InstantiateModule(ctx, p.wasmBinary, cfg); err != nil {
		return
	}
	start := engine.mod.ExportedFunction("_start")
	if start == nil {
		err = errors.New("_start function is required")
		engine.mod.Close(ctx)
		return
	}
	go func() {
		_, err := start.Call(context.Background())
		if !engine.mod.IsClosed() {
			_ = engine.mod.Close(context.Background())
		}
		if err == nil {
			err = sys.NewExitError(0)
		}
		engine.procErr = err
		close(engine.hostChan)
	}()

	// Wait for the engine to start
	select {
	case <-engine.hostChan:
	case <-time.After(p.cfg.timeout):
		_ = engine.mod.Close(ctx)
		drainChannel(engine.hostChan) // Wait for goroutine to exit
	}
	return engine, engine.procErr
}

func (p *dataTransformEnginePool) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var modRunner *dataTransformEngine
	var err error
	if modRunnerPtr := p.modulePool.Get(); modRunnerPtr != nil {
		modRunner = modRunnerPtr.(*dataTransformEngine)
	} else {
		if modRunner, err = p.newModule(); err != nil {
			return nil, err
		}
	}

	res, err := modRunner.Run(ctx, batch)
	if err != nil {
		_ = modRunner.Close(ctx)
		return nil, err
	}
	p.modulePool.Put(modRunner)
	return []service.MessageBatch{res}, nil
}

func (p *dataTransformEnginePool) Close(ctx context.Context) error {
	for {
		mr := p.modulePool.Get()
		if mr == nil {
			return p.wasmBinary.Close(ctx)
		}
		if err := mr.(*dataTransformEngine).Close(ctx); err != nil {
			return err
		}
	}
}

//------------------------------------------------------------------------------

type dataTransformEngine struct {
	log *service.Logger
	cfg *dataTransformConfig

	runtime wazero.Runtime
	mod     api.Module

	inputBatch  []transformMessage
	outputBatch service.MessageBatch
	targetIndex int

	procErr   error
	hostChan  chan any
	guestChan chan any
}

func (r *dataTransformEngine) newTransformMessage(message *service.Message) (tmsg transformMessage, err error) {
	tmsg.value, err = message.AsBytes()
	if err != nil {
		return
	}
	if r.cfg.inputKey != nil {
		if tmsg.key, err = r.cfg.inputKey.TryBytes(message); err != nil {
			return
		}
	}
	if r.cfg.timestamp != nil {
		var tsStr string
		if tsStr, err = r.cfg.timestamp.TryString(message); err != nil {
			err = fmt.Errorf("timestamp interpolation error: %w", err)
			return
		}
		if tmsg.timestamp, err = strconv.ParseInt(tsStr, 10, 64); err != nil {
			err = fmt.Errorf("failed to parse timestamp: %w", err)
			return
		}
	} else {
		tmsg.timestamp = time.Now().UnixMilli()
	}
	err = r.cfg.inputMetadata.Walk(message, func(key, value string) error {
		tmsg.headers = append(tmsg.headers, transformHeader{key, []byte(value)})
		return nil
	})
	return
}

func (r *dataTransformEngine) convertTransformMessage(message transformMessage) (*service.Message, error) {
	msg := service.NewMessage(message.value)
	if r.cfg.outputMetadata != nil {
		for _, hdr := range message.headers {
			if r.cfg.outputMetadata.Match(hdr.key) {
				msg.MetaSetMut(hdr.key, hdr.value)
			}
		}
	}
	if r.cfg.outputKeyField != nil {
		msg.MetaSetMut(*r.cfg.outputKeyField, message.key)
	}
	if message.outputTopic != nil {
		msg.MetaSetMut("data_transform_output_topic", *message.outputTopic)
	}
	return msg, nil
}

func (r *dataTransformEngine) reset() {
	r.inputBatch = nil
	r.targetIndex = 0
	r.outputBatch = nil
}

func (r *dataTransformEngine) Run(ctx context.Context, batch service.MessageBatch) (service.MessageBatch, error) {
	if r.procErr != nil {
		return nil, r.procErr
	}
	defer r.reset()
	r.inputBatch = make([]transformMessage, len(batch))
	r.targetIndex = 0
	for i, msg := range batch {
		tm, err := r.newTransformMessage(msg)
		if err != nil {
			return nil, err
		}
		r.inputBatch[i] = tm
	}
	// Notify the guest that it has data to process
	r.guestChan <- nil
	// Wait for the guest to process everything
	select {
	case <-r.hostChan:
	case <-time.After(r.cfg.timeout):
		_ = r.mod.Close(ctx)
		drainChannel(r.hostChan)
	}
	return r.outputBatch, r.procErr
}

func (r *dataTransformEngine) Close(ctx context.Context) error {
	close(r.guestChan)
	_ = r.mod.Close(ctx)
	drainChannel(r.hostChan) // Wait for goroutine to exit
	err := r.runtime.Close(ctx)
	return err
}

func drainChannel(ch <-chan any) {
	for {
		_, ok := <-ch
		if !ok {
			break
		}
	}
}
