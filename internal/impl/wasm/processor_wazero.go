package wasm

import (
	"context"
	"log"
	"os"

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
	log *service.Logger

	runtime wazero.Runtime
	mod     api.Module

	pendingMsg      *service.Message
	afterProcessing []func()

	process     api.Function
	goMalloc    api.Function
	goFree      api.Function
	rustAlloc   api.Function
	rustDealloc api.Function
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

func newWazeroAllocProcessor(functionName string, wasmBinary []byte, mgr *service.Resources) (proc *wazeroAllocProcessor, err error) {
	ctx := context.Background()

	r := wazero.NewRuntime(ctx)
	proc = &wazeroAllocProcessor{
		log:     mgr.Logger(),
		runtime: r,
	}
	defer func() {
		if err != nil {
			proc.runtime.Close(context.Background())
		}
	}()

	if _, err = r.NewHostModuleBuilder("benthos_wasm").
		NewFunctionBuilder().WithFunc(proc.setBytes).Export("v0_msg_set_bytes").
		NewFunctionBuilder().WithFunc(proc.getBytes).Export("v0_msg_as_bytes").
		Instantiate(ctx, r); err != nil {
		return
	}

	if _, err = wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		return
	}

	if proc.mod, err = r.InstantiateModuleFromBinary(ctx, wasmBinary); err != nil {
		return
	}

	proc.process = proc.mod.ExportedFunction(functionName)
	proc.goMalloc = proc.mod.ExportedFunction("malloc")
	proc.goFree = proc.mod.ExportedFunction("free")
	proc.rustAlloc = proc.mod.ExportedFunction("allocate")
	proc.rustDealloc = proc.mod.ExportedFunction("deallocate")
	return
}

func (p *wazeroAllocProcessor) setBytes(ctx context.Context, m api.Module, contentPtr, contentSize uint32) {
	bytes, ok := p.mod.Memory().Read(ctx, contentPtr, contentSize)
	if !ok {
		// TODO: What do we do here?
		panic("TODO read mem")
	}

	dataCopy := make([]byte, len(bytes))
	copy(dataCopy, bytes)
	p.pendingMsg.SetBytes(dataCopy)

	if p.rustDealloc != nil {
		_, _ = p.rustDealloc.Call(ctx, uint64(contentPtr), uint64(contentSize))
	}
}

func (p *wazeroAllocProcessor) getBytes(ctx context.Context, m api.Module) (ptrSize uint64) {
	msgBytes, err := p.pendingMsg.AsBytes()
	if err != nil {
		// TODO: What do we do here?
		log.Panic("TODO as bytes", err)
	}

	contentLen := uint64(len(msgBytes))

	var results []uint64
	if p.goMalloc != nil {
		results, err = p.goMalloc.Call(ctx, contentLen)
	}
	if p.rustAlloc != nil {
		results, err = p.rustAlloc.Call(ctx, contentLen)
	}
	if err != nil {
		// TODO: What do we do here?
		log.Panic("TODO bad alloc", err)
	}

	contentPtr := results[0]

	// Run de-allocation only once the process call is finished.
	p.afterProcessing = append(p.afterProcessing, func() {
		var err error
		if p.goFree != nil {
			_, err = p.goFree.Call(ctx, contentPtr)
		}
		if err != nil {
			log.Panic("TODO bad dealloc", err)
		}
	})

	// The pointer is a linear memory offset, which is where we write the name.
	if !p.mod.Memory().Write(ctx, uint32(contentPtr), msgBytes) {
		// TODO: What do we do here?
		panic("TODO mem write")
	}
	return (contentPtr << uint64(32)) | contentLen
}

func (p *wazeroAllocProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	p.pendingMsg = msg
	_, err := p.process.Call(ctx)
	p.pendingMsg = nil
	for _, fn := range p.afterProcessing {
		fn()
	}
	p.afterProcessing = nil
	if err != nil {
		return nil, err
	}
	return service.MessageBatch{msg}, nil
}

func (p *wazeroAllocProcessor) Close(ctx context.Context) error {
	_ = p.mod.Close(ctx)
	return p.runtime.Close(ctx)
}
