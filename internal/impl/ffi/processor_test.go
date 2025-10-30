// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ffi

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func SharedLibraryPath() string {
	switch runtime.GOOS {
	case "linux":
		return "./testdata/plugin.so"
	case "darwin":
		return "./testdata/plugin.dylib"
	default:
		return ""
	}
}

func CreateSharedLibrary(t *testing.T) {
	t.Helper()
	switch runtime.GOOS {
	case "linux", "darwin":
		_, err := os.Stat(SharedLibraryPath())
		if err == nil {
			return
		}
		cmd := exec.CommandContext(
			t.Context(),
			"g++",
			"-shared", "-fPIC",
			"./testdata/plugin.cc",
			"-o", SharedLibraryPath(),
		)
		if err := cmd.Run(); err != nil {
			t.Skip("unable to compile shared library:", err)
		}
	default:
		t.Skip("no shared library tests on platform", runtime.GOOS)
	}
}

func ReplaceConfig(s string, extra []string) string {
	return strings.NewReplacer(
		slices.Concat([]string{"$LIB", SharedLibraryPath()}, extra)...,
	).Replace(s)
}

func SetupFFIProcessor(t *testing.T, config string, extraReplacements ...string) (producer chan<- *service.Message, consumer <-chan *service.Message) {
	builder := service.NewStreamBuilder()
	p := make(chan *service.Message)
	producer = p
	t.Cleanup(func() { close(p) })
	builder.SetThreads(1)
	produce, err := builder.AddProducerFunc()
	require.NoError(t, err)
	go func() {
		for m := range p {
			_ = produce(t.Context(), m)
		}
	}()
	c := make(chan *service.Message)
	consumer = c
	t.Cleanup(func() { close(c) })
	err = builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		c <- m
		return nil
	})
	require.NoError(t, err)
	err = builder.AddProcessorYAML(ReplaceConfig(config, extraReplacements))
	require.NoError(t, err)
	stream, err := builder.Build()
	require.NoError(t, err)
	sig := make(chan struct{})
	go func() {
		err := stream.Run(context.Background())
		close(sig)
		require.NoError(t, err)
	}()
	t.Cleanup(func() {
		_ = stream.Stop(context.Background())
		<-sig
	})
	return producer, consumer
}

func CheckMessageJSON(t *testing.T, m *service.Message, expected string) {
	require.NoError(t, m.GetError())
	b, err := m.AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, expected, string(b))
}

func TestFFIProcessor(t *testing.T) {
	CreateSharedLibrary(t)
	t.Run("SetAndGet", func(t *testing.T) {
		producer, consumer := SetupFFIProcessor(t, `
try:
  - ffi:
      library_path: $LIB
      function_name: SetState
      args_mapping: 'root = [this.num]'
      signature:
        return:
          type: void
        parameters:
          - type: int64
  - mapping: |
      root = if this.length() != 0 {
        throw("expected no result")
      } else {
        this
      }
  - ffi:
      library_path: $LIB
      function_name: GetState
      args_mapping: 'root = []'
      signature:
        return:
          type: int64
        parameters: []
`)
		producer <- service.NewMessage([]byte(`{"num":42}`))
		CheckMessageJSON(t, <-consumer, `[42]`)
		producer <- service.NewMessage([]byte(`{"num":9}`))
		CheckMessageJSON(t, <-consumer, `[9]`)
	})
	t.Run("UpperBits", func(t *testing.T) {
		producer, consumer := SetupFFIProcessor(t, `
ffi:
  library_path: $LIB
  function_name: UpperBits
  args_mapping: 'root = [this.num]'
  signature:
    return:
      type: int32
    parameters:
      - type: int64
`)
		producer <- service.NewMessage([]byte(`{"num":4294967295}`))
		CheckMessageJSON(t, <-consumer, `[0]`)
		producer <- service.NewMessage([]byte(`{"num":-4294967296}`))
		CheckMessageJSON(t, <-consumer, `[-1]`)
		producer <- service.NewMessage([]byte(`{"num":4294967296}`))
		CheckMessageJSON(t, <-consumer, `[1]`)
		producer <- service.NewMessage([]byte(`{"num":9223372029709869056}`))
		CheckMessageJSON(t, <-consumer, `[2147483646]`)
		producer <- service.NewMessage([]byte(`{"num":1311768467451248289}`))
		CheckMessageJSON(t, <-consumer, `[305419896]`)
		producer <- service.NewMessage([]byte(`{"num":-1}`))
		CheckMessageJSON(t, <-consumer, `[-1]`)
	})
	t.Run("ReverseBytes", func(t *testing.T) {
		producer, consumer := SetupFFIProcessor(t, `
try:
  - ffi:
      library_path: $LIB
      function_name: ReverseBytes
      args_mapping: |
        # The only way I can think of right now to make a dynamically sized string
        let null_str = "%0*d".format(this.str.length(), 0).slice(0, this.str.length()).replace_all("0", "\u0000")
        root = [this.str, $null_str, this.str.length()]
      signature:
        return:
          type: int32
        parameters:
          - type: byte*
          - type: byte*
            out: true
          - type: int32
  - mapping: |
      root = if (this.array().length() != 2) {
        throw("unexpected result length: " + content().string())
      } else {
         # convert the bytes output to a string
         [this.0, this.1.string()]
      }
`)
		producer <- service.NewMessage([]byte(`{"str":"abc"}`))
		CheckMessageJSON(t, <-consumer, `[3, "cba"]`)
		producer <- service.NewMessage([]byte(`{"str":""}`))
		CheckMessageJSON(t, <-consumer, `[0, ""]`)
		producer <- service.NewMessage([]byte(`{"str":"0123456789"}`))
		CheckMessageJSON(t, <-consumer, `[10, "9876543210"]`)
	})
	// This test ensures that our fallback signature support is working.
	t.Run("Fallbacks", func(t *testing.T) {
		for _, functionName := range []string{"AssignAll", "AssignAllWithResult"} {
			retType := "void"
			if functionName == "AssignAllWithResult" {
				retType = "int64"
			}
			producer, consumer := SetupFFIProcessor(t, `
try:
  - ffi:
      library_path: $LIB
      function_name: AddInt32
      args_mapping: 'root = [68, -1]'
      signature:
        return:
          type: int32
        parameters:
          - type: int32
          - type: int32
  - ffi:
      library_path: $LIB
      function_name: AddInt64
      args_mapping: 'root = [this.0, 2]'
      signature:
        return:
          type: int64
        parameters:
          - type: int64
          - type: int64
  - ffi:
      library_path: $LIB
      function_name: $FUNC
      args_mapping: |
        root = ["000", 3, this.0]
      signature:
        return:
          type: $RET_TYPE
        parameters:
          - type: byte*
            out: true
          - type: int64
          - type: int32
  - mapping: |
      root = this.map_each(e -> e.string())
`, "$FUNC", functionName, "$RET_TYPE", retType)
			producer <- service.NewMessage([]byte(`{}`))
			if functionName == "AssignAllWithResult" {
				CheckMessageJSON(t, <-consumer, `["3", "EEE"]`)
			} else {
				CheckMessageJSON(t, <-consumer, `["EEE"]`)
			}
		}
	})
}
