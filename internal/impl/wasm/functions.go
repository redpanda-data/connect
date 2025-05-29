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
	return func(ctx context.Context, _ api.Module, contentPtr, contentSize uint32) {
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
	return func(ctx context.Context, _ api.Module) (ptrSize uint64) {
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
	return func(ctx context.Context, _ api.Module, keyPtr, keySize, contentPtr, contentSize uint32) {
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
	return func(ctx context.Context, _ api.Module, keyPtr, keySize uint32) (ptrSize uint64) {
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
