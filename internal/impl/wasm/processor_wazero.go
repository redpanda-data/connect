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
	err := service.RegisterProcessor(
		"wasm", wazeroAllocProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
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

	pathStr, err := conf.FieldString("path")
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

	if _, err = r.NewHostModuleBuilder("benthos_wasm").
		NewFunctionBuilder().WithFunc(mod.setBytes).Export("v0_msg_set_bytes").
		NewFunctionBuilder().WithFunc(mod.getBytes).Export("v0_msg_as_bytes").
		Instantiate(ctx, r); err != nil {
		return
	}

	if _, err = wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return
	}

	if mod.mod, err = r.InstantiateModuleFromBinary(ctx, p.wasmBinary); err != nil {
		return
	}

	mod.process = mod.mod.ExportedFunction(p.functionName)
	mod.goMalloc = mod.mod.ExportedFunction("malloc")
	mod.goFree = mod.mod.ExportedFunction("free")
	mod.rustAlloc = mod.mod.ExportedFunction("allocate")
	mod.rustDealloc = mod.mod.ExportedFunction("deallocate")

	return mod, nil
}

func (p *wazeroAllocProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
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

	res, err := modRunner.Run(ctx, msg)
	if err != nil {
		return nil, err
	}
	return service.MessageBatch{res}, nil
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

	pendingMsg      *service.Message
	afterProcessing []func()
	procErr         error

	process     api.Function
	goMalloc    api.Function
	goFree      api.Function
	rustAlloc   api.Function
	rustDealloc api.Function
}

func (r *moduleRunner) setBytes(ctx context.Context, m api.Module, contentPtr, contentSize uint32) {
	bytes, ok := r.mod.Memory().Read(ctx, contentPtr, contentSize)
	if !ok {
		r.procErr = errors.New("failed to read out-bound memory")
		r.log.Error(r.procErr.Error())
	}

	dataCopy := make([]byte, len(bytes))
	copy(dataCopy, bytes)
	r.pendingMsg.SetBytes(dataCopy)

	if r.rustDealloc != nil {
		_, _ = r.rustDealloc.Call(ctx, uint64(contentPtr), uint64(contentSize))
	}
}

func (r *moduleRunner) getBytes(ctx context.Context, m api.Module) (ptrSize uint64) {
	msgBytes, err := r.pendingMsg.AsBytes()
	if err != nil {
		r.procErr = fmt.Errorf("failed to get message as bytes: %v", err)
		r.log.Error(r.procErr.Error())
	}

	contentLen := uint64(len(msgBytes))

	var results []uint64
	if r.goMalloc != nil {
		results, err = r.goMalloc.Call(ctx, contentLen)
	}
	if r.rustAlloc != nil {
		results, err = r.rustAlloc.Call(ctx, contentLen)
	}
	if err != nil {
		r.procErr = fmt.Errorf("failed to allocate in-bound memory: %v", err)
		r.log.Error(r.procErr.Error())
	}

	contentPtr := results[0]

	// Run de-allocation only once the process call is finished.
	r.afterProcessing = append(r.afterProcessing, func() {
		var err error
		if r.goFree != nil {
			_, err = r.goFree.Call(ctx, contentPtr)
		}
		if err != nil {
			r.procErr = fmt.Errorf("failed to free in-bound memory: %v", err)
			r.log.Error(r.procErr.Error())
		}
	})

	// The pointer is a linear memory offset, which is where we write the name.
	if !r.mod.Memory().Write(ctx, uint32(contentPtr), msgBytes) {
		r.procErr = errors.New("failed to write in-bound memory")
		r.log.Error(r.procErr.Error())
	}
	return (contentPtr << uint64(32)) | contentLen
}

func (r *moduleRunner) reset() {
	r.pendingMsg = nil
	r.procErr = nil
	r.afterProcessing = nil
}

func (r *moduleRunner) Run(ctx context.Context, msg *service.Message) (*service.Message, error) {
	r.pendingMsg = msg
	_, err := r.process.Call(ctx)
	for _, fn := range r.afterProcessing {
		fn()
	}
	r.reset()
	if err != nil {
		return nil, err
	}
	if r.procErr != nil {
		return nil, r.procErr
	}
	return msg, nil
}

func (r *moduleRunner) Close(ctx context.Context) error {
	_ = r.mod.Close(ctx)
	return r.runtime.Close(ctx)
}
