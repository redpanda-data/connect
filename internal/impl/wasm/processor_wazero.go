package wasm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	"github.com/benthosdev/benthos/v4/public/service"
)

func wazeroAllocProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Utility").
		Summary("Executes a function exported by a WASM module for each message.").
		Description(`
This processor uses [Wazero](https://github.com/tetratelabs/wazero) to execute a WASM module (with support for WASI), calling a specific function for each message being processed. From within the WASM module it is possible to query and mutate the message being processed via a suite of functions exported to the module.

This ecosystem is delicate as WASM doesn't have a single clearly defined way to pass strings back and forth between the host and the module. In order to remedy this we're gradually working on introducing libraries and examples for multiple languages which can be found in [the codebase](https://github.com/benthosdev/benthos/tree/main/public/wasm/README.md).

These examples, as well as the processor itself, is a work in progress.

### Parallelism

It's not currently possible to execute a single WASM runtime across parallel threads with this processor. Therefore, in order to support parallel processing this processor implements pooling of module runtimes. Ideally your WASM module shouldn't depend on any global state, but if it does then you need to ensure the processor [is only run on a single thread](/docs/configuration/processing_pipelines).
`).
		Field(service.NewStringField("module_path").
			Description("The path of the target WASM module to execute.")).
		Field(service.NewStringField("function").
			Default("process").
			Description("The name of the function exported by the target WASM module to run for each message.")).
		Version("4.11.0")
}

func init() {
	err := service.RegisterBatchProcessor(
		"wasm", wazeroAllocProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newWazeroAllocProcessorFromConfig(conf, mgr)
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type wazeroAllocProcessor struct {
	log          *service.Logger
	functionName string
	wasmBinary   []byte
	modulePool   sync.Pool
}

func newWazeroAllocProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*wazeroAllocProcessor, error) {
	function, err := conf.FieldString("function")
	if err != nil {
		return nil, err
	}

	pathStr, err := conf.FieldString("module_path")
	if err != nil {
		return nil, err
	}

	fileBytes, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, err
	}

	return newWazeroAllocProcessor(function, fileBytes, mgr)
}

func newWazeroAllocProcessor(functionName string, wasmBinary []byte, mgr *service.Resources) (*wazeroAllocProcessor, error) {
	proc := &wazeroAllocProcessor{
		log:        mgr.Logger(),
		modulePool: sync.Pool{},

		functionName: functionName,
		wasmBinary:   wasmBinary,
	}

	// Ensure we can create at least one module runner.
	modRunner, err := proc.newModule()
	if err != nil {
		return nil, err
	}

	proc.modulePool.Put(modRunner)
	return proc, nil
}

func (p *wazeroAllocProcessor) newModule() (mod *moduleRunner, err error) {
	ctx := context.Background()

	r := wazero.NewRuntime(ctx)
	mod = &moduleRunner{
		log:     p.log,
		runtime: r,
	}
	defer func() {
		if err != nil {
			mod.runtime.Close(context.Background())
		}
	}()

	builder := r.NewHostModuleBuilder("benthos_wasm")
	for name, ctor := range moduleRunnerFunctionCtors {
		builder = builder.NewFunctionBuilder().WithFunc(ctor(mod)).Export(name)
	}
	if _, err = builder.Instantiate(ctx); err != nil {
		return
	}

	if _, err = wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return
	}

	if mod.mod, err = r.Instantiate(ctx, p.wasmBinary); err != nil {
		return
	}

	mod.process = mod.mod.ExportedFunction(p.functionName)
	mod.goMalloc = mod.mod.ExportedFunction("malloc")
	mod.goFree = mod.mod.ExportedFunction("free")
	mod.rustAlloc = mod.mod.ExportedFunction("allocate")
	mod.rustDealloc = mod.mod.ExportedFunction("deallocate")

	return mod, nil
}

func (p *wazeroAllocProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var modRunner *moduleRunner
	var err error
	if modRunnerPtr := p.modulePool.Get(); modRunnerPtr != nil {
		modRunner = modRunnerPtr.(*moduleRunner)
	} else {
		if modRunner, err = p.newModule(); err != nil {
			return nil, err
		}
	}
	defer func() {
		p.modulePool.Put(modRunner)
	}()

	res, err := modRunner.Run(ctx, batch)
	if err != nil {
		return nil, err
	}
	return []service.MessageBatch{res}, nil
}

func (p *wazeroAllocProcessor) Close(ctx context.Context) error {
	for {
		mr := p.modulePool.Get()
		if mr == nil {
			return nil
		}
		if err := mr.(*moduleRunner).Close(ctx); err != nil {
			return err
		}
	}
}

//------------------------------------------------------------------------------

type moduleRunner struct {
	log *service.Logger

	runtime wazero.Runtime
	mod     api.Module

	runBatch        service.MessageBatch
	targetMessage   *service.Message
	targetIndex     int
	afterProcessing []func()
	procErr         error

	process     api.Function
	goMalloc    api.Function
	goFree      api.Function
	rustAlloc   api.Function
	rustDealloc api.Function
}

func (r *moduleRunner) reset() {
	r.runBatch = nil
	r.targetMessage = nil
	r.targetIndex = 0
	r.procErr = nil
	r.afterProcessing = nil
}

func (r *moduleRunner) funcErr(err error) {
	r.procErr = err
	r.log.Error(err.Error())
}

// Allocate memory that's in bound to the WASM module. This memory will be
// deallocated at the end of the run.
func (r *moduleRunner) allocateBytesInbound(ctx context.Context, data []byte) (contentPtr uint64, err error) {
	contentLen := uint64(len(data))

	var results []uint64
	if r.goMalloc != nil {
		results, err = r.goMalloc.Call(ctx, contentLen)
	}
	if r.rustAlloc != nil {
		results, err = r.rustAlloc.Call(ctx, contentLen)
	}
	if err != nil {
		return
	}

	contentPtr = results[0]

	// Run de-allocation only once the process call is finished.
	r.afterProcessing = append(r.afterProcessing, func() {
		var err error
		if r.goFree != nil {
			_, err = r.goFree.Call(ctx, contentPtr)
		}
		if err != nil {
			r.funcErr(fmt.Errorf("failed to free in-bound memory: %v", err))
			return
		}
	})

	// The pointer is a linear memory offset, which is where we write the name.
	if !r.mod.Memory().Write(uint32(contentPtr), data) {
		err = errors.New("failed to write in-bound memory")
		return
	}
	return
}

// Deallocate memory that's out bound from the WASM module.
func (r *moduleRunner) readBytesOutbound(ctx context.Context, contentPtr, contentSize uint32) ([]byte, error) {
	bytes, ok := r.mod.Memory().Read(contentPtr, contentSize)
	if !ok {
		return nil, errors.New("prevented read")
	}

	dataCopy := make([]byte, len(bytes))
	copy(dataCopy, bytes)

	if r.rustDealloc != nil {
		_, _ = r.rustDealloc.Call(ctx, uint64(contentPtr), uint64(contentSize))
	}
	return dataCopy, nil
}

func (r *moduleRunner) Run(ctx context.Context, batch service.MessageBatch) (service.MessageBatch, error) {
	defer r.reset()

	var newBatch service.MessageBatch
	for i := range batch {
		r.reset()
		r.runBatch = batch
		r.targetIndex = i
		r.targetMessage = batch[i]
		_, err := r.process.Call(ctx)
		for _, fn := range r.afterProcessing {
			fn()
		}
		if err != nil {
			return nil, err
		}
		newMsg := r.targetMessage
		if r.procErr != nil {
			newMsg = batch[i].Copy()
			newMsg.SetError(r.procErr)
		}
		if newMsg != nil {
			newBatch = append(newBatch, newMsg)
		}
	}
	return newBatch, nil
}

func (r *moduleRunner) Close(ctx context.Context) error {
	_ = r.mod.Close(ctx)
	return r.runtime.Close(ctx)
}
