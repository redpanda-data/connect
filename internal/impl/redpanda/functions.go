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

	"github.com/tetratelabs/wazero/api"
)

const (
	noActiveTransform = int32(-1)
	invalidBuffer     = int32(-2)
)

var transformHostFunctions = map[string]func(r *dataTransformEngine) any{}

func registerModuleRunnerFunction(name string, ctor func(r *dataTransformEngine) any) struct{} {
	transformHostFunctions[name] = ctor
	return struct{}{}
}

var _ = registerModuleRunnerFunction("check_abi_version_1", func(*dataTransformEngine) any {
	return func(_ context.Context, _ api.Module) {
		// Placeholder for ABI compatibility check
	}
})

var _ = registerModuleRunnerFunction("check_abi_version_2", func(*dataTransformEngine) any {
	return func(_ context.Context, _ api.Module) {
		// Placeholder for ABI compatibility check
	}
})

var _ = registerModuleRunnerFunction("read_batch_header", func(r *dataTransformEngine) any {
	return func(
		ctx context.Context,
		m api.Module,
		_,
		recordCount,
		_,
		_,
		_,
		_,
		_,
		_,
		_,
		_ uint32,
	) int32 {
		// Notify the host we're done processing a batch.
		r.hostChan <- nil
		// Wait for new batch to be submitted for processing.
		select {
		case _, ok := <-r.guestChan:
			if !ok {
				return noActiveTransform
			}
		case <-ctx.Done():
			return noActiveTransform
		}
		if !m.Memory().WriteUint32Le(recordCount, uint32(len(r.inputBatch))) {
			return invalidBuffer
		}
		longest := 0
		for _, msg := range r.inputBatch {
			longest = max(longest, msg.maxSize())
		}
		// We should write dummy values in the other fields, but they are
		// currently unused by SDKs.
		return int32(longest)
	}
})

var _ = registerModuleRunnerFunction("read_next_record", func(r *dataTransformEngine) any {
	return func(_ context.Context, m api.Module, attributes, timestamp, _, dataPtr, dataLen uint32) int32 {
		if r.targetIndex >= len(r.inputBatch) {
			return noActiveTransform
		}
		mem := m.Memory()
		msg := r.inputBatch[r.targetIndex]
		if !mem.WriteByte(attributes, 0) {
			return invalidBuffer
		}
		if !mem.WriteUint64Le(timestamp, uint64(msg.timestamp)) {
			return invalidBuffer
		}
		if !mem.WriteUint64Le(timestamp, uint64(msg.offset)) {
			return invalidBuffer
		}
		data, ok := mem.Read(dataPtr, dataLen)
		if !ok {
			return invalidBuffer
		}
		n := msg.serialize(data)
		if n < 0 {
			return invalidBuffer
		}
		r.targetIndex += 1
		return int32(n)
	}
})

var _ = registerModuleRunnerFunction("write_record", func(r *dataTransformEngine) any {
	return func(_ context.Context, m api.Module, dataPtr, dataLen uint32) int32 {
		buf, ok := m.Memory().Read(dataPtr, dataLen)
		if !ok {
			return invalidBuffer
		}
		var tmsg transformMessage
		_, err := tmsg.deserialize(buf)
		if err != nil {
			return invalidBuffer
		}
		smsg, err := r.convertTransformMessage(tmsg)
		if err != nil {
			return invalidBuffer
		}
		r.outputBatch = append(r.outputBatch, smsg)
		return int32(len(buf))
	}
})

var _ = registerModuleRunnerFunction("write_record_with_options", func(*dataTransformEngine) any {
	return func(_ context.Context, m api.Module, dataPtr, dataLen, _, _ uint32) int32 {
		dataBuf, ok := m.Memory().Read(dataPtr, dataLen)
		if !ok {
			return invalidBuffer
		}
		var tmsg transformMessage
		_, err := tmsg.deserialize(dataBuf)
		if err != nil {
			return invalidBuffer
		}
		optsBuf, ok := m.Memory().Read(dataPtr, dataLen)
		if !ok {
			return invalidBuffer
		}
		var opts transformWriteOptions
		_, err = opts.deserialize(optsBuf)
		if err != nil {
			return invalidBuffer
		}
		tmsg.outputTopic = &opts.topic
		return int32(len(dataBuf))
	}
})
