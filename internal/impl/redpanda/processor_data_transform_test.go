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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func getWASMArtifact(t testing.TB) []byte {
	t.Helper()

	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "uppercase.wasm")

	require.NoError(t, exec.Command("env", "GOOS=wasip1", "GOARCH=wasm", "GOEXPERIMENT=", "go", "build", "-C", "./testdata/uppercase", "-o", outPath).Run())

	outBytes, err := os.ReadFile(outPath)
	require.NoError(t, err)

	return outBytes
}

func TestDataTransform(t *testing.T) {
	outBytes := getWASMArtifact(t)

	t.Run("serial", func(t *testing.T) {
		testDataTransformProcessorSerial(t, outBytes)
	})

	t.Run("init_timeout", func(t *testing.T) {
		testDataTransformProcessorInitTimeout(t, outBytes)
	})

	t.Run("oom", func(t *testing.T) {
		testDataTransformProcessorOutOfMemory(t, outBytes)
	})

	t.Run("keys", func(t *testing.T) {
		testDataTransformProcessorKeys(t, outBytes)
	})

	t.Run("parallel", func(t *testing.T) {
		testDataTransformProcessorParallel(t, outBytes)
	})
}

func testDataTransformProcessorSerial(t *testing.T, wasm []byte) {
	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(t.Context()))
	})

	for range 1000 {
		inMsg := service.NewMessage([]byte(`hello world`))
		outBatches, err := proc.ProcessBatch(t.Context(), service.MessageBatch{inMsg})
		require.NoError(t, err)

		require.Len(t, outBatches, 1)
		require.Len(t, outBatches[0], 1)
		resBytes, err := outBatches[0][0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "HELLO WORLD", string(resBytes))
	}
}

func testDataTransformProcessorInitTimeout(t *testing.T, wasm []byte) {
	cfg := defaultConfig()
	cfg.timeout = time.Nanosecond
	_, err := newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.Error(t, err)
}

func testDataTransformProcessorOutOfMemory(t *testing.T, wasm []byte) {
	cfg := defaultConfig()
	cfg.maxMemoryPages = 1
	_, err := newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.Error(t, err)
}

func testDataTransformProcessorKeys(t *testing.T, wasm []byte) {
	cfg := defaultConfig()
	var err error
	cfg.inputKey, err = service.NewInterpolatedString(`${! metadata("example_input_key") }`)
	require.NoError(t, err)
	outputKeyField := "example_output_key"
	cfg.outputKeyField = &outputKeyField
	proc, err := newDataTransformProcessor(wasm, cfg, service.MockResources())
	require.NoError(t, err)
	inMsg := service.NewMessage([]byte(`hello world`))
	inMsg.MetaSetMut("example_input_key", "foobar")
	outBatches, err := proc.ProcessBatch(t.Context(), service.MessageBatch{inMsg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	outKey, ok := outBatches[0][0].MetaGetMut(outputKeyField)
	assert.True(t, ok)
	assert.Equal(t, []byte("foobar"), outKey)
}

func testDataTransformProcessorParallel(t *testing.T, wasm []byte) {
	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(t.Context()))
	})

	tStarted := time.Now()
	var wg sync.WaitGroup
	for j := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			iters := 0
			for time.Since(tStarted) < (time.Millisecond * 500) {
				iters++
				exp := fmt.Sprintf("hello world %v:%v", id, iters)
				inMsg := service.NewMessage([]byte(exp))
				outBatches, err := proc.ProcessBatch(t.Context(), service.MessageBatch{inMsg})
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
	wasm := getWASMArtifact(b)

	proc, err := newDataTransformProcessor(wasm, defaultConfig(), service.MockResources())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, proc.Close(b.Context()))
	})

	b.ReportAllocs()

	inMsg := service.NewMessage([]byte(`hello world`))

	for b.Loop() {
		outBatches, err := proc.ProcessBatch(b.Context(), service.MessageBatch{inMsg.Copy()})
		require.NoError(b, err)

		require.Len(b, outBatches, 1)
		require.Len(b, outBatches[0], 1)

		_, err = outBatches[0][0].AsBytes()
		require.NoError(b, err)
	}
}
