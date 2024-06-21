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

//go:generate env GOOS=wasip1 GOARCH=wasm go build -C ../../../resources/testdata/redpanda_data_transforms/uppercase -o $PWD/uppercase.wasm

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func defaultConfig() dataTransformConfig {
	var cfg dataTransformConfig
	cfg.maxMemoryPages = 1000
	cfg.timeout = time.Second
	return cfg
}

func TestDataTransformProcessorSerial(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(t, err)

	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(context.Background()))
	})

	for i := 0; i < 1000; i++ {
		inMsg := service.NewMessage([]byte(`hello world`))
		outBatches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{inMsg})
		require.NoError(t, err)

		require.Len(t, outBatches, 1)
		require.Len(t, outBatches[0], 1)
		resBytes, err := outBatches[0][0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "HELLO WORLD", string(resBytes))
	}
}

func TestDataTransformProcessorInitTimeout(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(t, err)
	cfg := defaultConfig()
	cfg.timeout = time.Nanosecond
	_, err = newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.Error(t, err)
}

func TestDataTransformProcessorOutOfMemory(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(t, err)
	cfg := defaultConfig()
	cfg.maxMemoryPages = 1
	_, err = newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.Error(t, err)
}

func TestDataTransformProcessorKeys(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(t, err)
	cfg := defaultConfig()
	cfg.inputKey, err = service.NewInterpolatedString(`${! metadata("example_input_key") }`)
	require.NoError(t, err)
	var outputKeyField = "example_output_key"
	cfg.outputKeyField = &outputKeyField
	proc, err := newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.NoError(t, err)
	inMsg := service.NewMessage([]byte(`hello world`))
	inMsg.MetaSetMut("example_input_key", "foobar")
	outBatches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{inMsg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	outKey, ok := outBatches[0][0].MetaGetMut(outputKeyField)
	assert.True(t, ok)
	assert.Equal(t, []byte("foobar"), outKey)
}

func TestDataTransformProcessorParallel(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(t, err)

	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(context.Background()))
	})

	tStarted := time.Now()
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			iters := 0
			for time.Since(tStarted) < (time.Millisecond * 500) {
				iters++
				exp := fmt.Sprintf("hello world %v:%v", id, iters)
				inMsg := service.NewMessage([]byte(exp))
				outBatches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{inMsg})
				require.NoError(t, err)

				require.Len(t, outBatches, 1)
				require.Len(t, outBatches[0], 1)
				resBytes, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, strings.ToUpper(exp), string(resBytes))
			}
		}(j)
	}
	wg.Wait()
}

func BenchmarkRedpandaDataTransforms(b *testing.B) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		b.Skip("skipping as wasm example not compiled, run go generate to remedy")
	}
	require.NoError(b, err)

	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, proc.Close(context.Background()))
	})

	b.ResetTimer()
	b.ReportAllocs()

	inMsg := service.NewMessage([]byte(`hello world`))

	for i := 0; i < b.N; i++ {
		outBatches, err := proc.ProcessBatch(context.Background(), service.MessageBatch{inMsg.Copy()})
		require.NoError(b, err)

		require.Len(b, outBatches, 1)
		require.Len(b, outBatches[0], 1)

		_, err = outBatches[0][0].AsBytes()
		require.NoError(b, err)
	}
}
