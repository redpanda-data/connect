package wasm

import (
	"context"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero/api"
)

func ptrLen(contentPtr, contentLen uint64) uint64 {
	return (contentPtr << uint64(32)) | contentLen
}

var moduleRunnerFunctionCtors = map[string]func(r *moduleRunner) interface{}{}

func registerModuleRunnerFunction(name string, ctor func(r *moduleRunner) interface{}) struct{} {
	moduleRunnerFunctionCtors[name] = ctor
	return struct{}{}
}

var _ = registerModuleRunnerFunction("v0_msg_set_bytes", func(r *moduleRunner) interface{} {
	return func(ctx context.Context, m api.Module, contentPtr, contentSize uint32) {
		if r.targetMessage == nil {
			r.funcErr(errors.New("attempted to set bytes of deleted message"))
			return
		}

		bytes, err := r.readBytesOutbound(ctx, contentPtr, contentSize)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to read out-bound memory: %w", err))
			return
		}
		r.targetMessage.SetBytes(bytes)
	}
})

var _ = registerModuleRunnerFunction("v0_msg_as_bytes", func(r *moduleRunner) interface{} {
	return func(ctx context.Context, m api.Module) (ptrSize uint64) {
		if r.targetMessage == nil {
			r.funcErr(errors.New("attempted to read bytes of deleted message"))
			return
		}

		msgBytes, err := r.targetMessage.AsBytes()
		if err != nil {
			r.funcErr(fmt.Errorf("failed to get message as bytes: %v", err))
			return
		}

		contentPtr, err := r.allocateBytesInbound(ctx, msgBytes)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to allocate in-bound memory: %v", err))
			return
		}
		return ptrLen(contentPtr, uint64(len(msgBytes)))
	}
})

var _ = registerModuleRunnerFunction("v0_msg_set_meta", func(r *moduleRunner) interface{} {
	return func(ctx context.Context, m api.Module, keyPtr, keySize, contentPtr, contentSize uint32) {
		if r.targetMessage == nil {
			r.funcErr(errors.New("attempted to set metadata of deleted message"))
			return
		}

		keyBytes, err := r.readBytesOutbound(ctx, keyPtr, keySize)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to read out-bound meta key memory: %w", err))
			return
		}

		contentBytes, err := r.readBytesOutbound(ctx, contentPtr, contentSize)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to read out-bound meta value memory: %w", err))
			return
		}

		r.targetMessage.MetaSetMut(string(keyBytes), string(contentBytes))
	}
})

var _ = registerModuleRunnerFunction("v0_msg_get_meta", func(r *moduleRunner) interface{} {
	return func(ctx context.Context, m api.Module, keyPtr, keySize uint32) (ptrSize uint64) {
		if r.targetMessage == nil {
			r.funcErr(errors.New("attempted to read meta of deleted message"))
			return
		}

		keyBytes, err := r.readBytesOutbound(ctx, keyPtr, keySize)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to read out-bound meta key memory: %w", err))
			return
		}

		metaValue, exists := r.targetMessage.MetaGet(string(keyBytes))
		if !exists {
			metaValue = ""
		}

		metaValueBytes := []byte(metaValue)
		contentPtr, err := r.allocateBytesInbound(ctx, metaValueBytes)
		if err != nil {
			r.funcErr(fmt.Errorf("failed to allocate in-bound memory: %v", err))
			return
		}
		return ptrLen(contentPtr, uint64(len(metaValueBytes)))
	}
})
