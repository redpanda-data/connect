package wasm

import (
	"context"
	"os"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWazeroWASIGoProcessor(t *testing.T) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, run build.sh to remedy")
	}
	require.NoError(t, err)

	proc, err := newWazeroAllocProcessor("process", wasm, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(context.Background()))
	})

	for i := 0; i < 1000; i++ {
		inMsg := service.NewMessage([]byte(`hello world`))
		outBatch, err := proc.Process(context.Background(), inMsg)
		require.NoError(t, err)

		require.Len(t, outBatch, 1)
		resBytes, err := outBatch[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "HELLO WORLD", string(resBytes))
	}
}

func TestWazeroWASIRustProcessor(t *testing.T) {
	wasm, err := os.ReadFile("./louder.wasm")
	if os.IsNotExist(err) {
		t.Skip("skipping as wasm example not compiled, build the rust example to remedy")
	}
	require.NoError(t, err)

	proc, err := newWazeroAllocProcessor("process", wasm, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(context.Background()))
	})

	for i := 0; i < 1000; i++ {
		inMsg := service.NewMessage([]byte(`hello world`))
		outBatch, err := proc.Process(context.Background(), inMsg)
		require.NoError(t, err)

		require.Len(t, outBatch, 1)
		resBytes, err := outBatch[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, "hello world!!!!111!!11!", string(resBytes))
	}
}

func BenchmarkWazeroWASIGoCalls(b *testing.B) {
	wasm, err := os.ReadFile("./uppercase.wasm")
	if os.IsNotExist(err) {
		b.Skip("skipping as wasm example not compiled, run build.sh to remedy")
	}
	require.NoError(b, err)

	proc, err := newWazeroAllocProcessor("process", wasm, service.MockResources())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, proc.Close(context.Background()))
	})

	b.ResetTimer()
	b.ReportAllocs()

	inMsg := service.NewMessage([]byte(`hello world`))

	for i := 0; i < b.N; i++ {
		outBatch, err := proc.Process(context.Background(), inMsg.Copy())
		require.NoError(b, err)

		require.Len(b, outBatch, 1)

		_, err = outBatch[0].AsBytes()
		require.NoError(b, err)
	}
}

func BenchmarkWazeroWASIRustCalls(b *testing.B) {
	wasm, err := os.ReadFile("./louder.wasm")
	if os.IsNotExist(err) {
		b.Skip("skipping as wasm example not compiled, build the rust example to remedy")
	}
	require.NoError(b, err)

	proc, err := newWazeroAllocProcessor("process", wasm, service.MockResources())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, proc.Close(context.Background()))
	})

	b.ResetTimer()
	b.ReportAllocs()

	inMsg := service.NewMessage([]byte(`hello world`))

	for i := 0; i < b.N; i++ {
		outBatch, err := proc.Process(context.Background(), inMsg.Copy())
		require.NoError(b, err)

		require.Len(b, outBatch, 1)

		_, err = outBatch[0].AsBytes()
		require.NoError(b, err)
	}
}
