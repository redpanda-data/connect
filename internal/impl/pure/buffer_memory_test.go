package pure

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func msgEqual(t testing.TB, expected string, m *service.Message) {
	t.Helper()

	mBytes, err := m.AsBytes()
	require.NoError(t, err)

	assert.Equal(t, expected, string(mBytes))
}

func memBufFromConf(t *testing.T, conf string) *memoryBuffer {
	t.Helper()

	parsedConf, err := memoryBufferConfig().ParseYAML(conf, nil)
	require.NoError(t, err)

	buf, err := newMemoryBufferFromConfig(parsedConf, service.MockResources())
	require.NoError(t, err)

	return buf
}

func TestMemoryBasic(t *testing.T) {
	n := 100

	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
`)
	defer block.Close(ctx)

	for i := 0; i < n; i++ {
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello")),
			service.NewMessage([]byte("world")),
			service.NewMessage([]byte("12345")),
			service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < n; i++ {
		m, ackFunc, err := block.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, m, 4)
		msgEqual(t, fmt.Sprintf("test%v", i), m[3])
		require.NoError(t, ackFunc(ctx, nil))
	}
}

func TestMemoryOwnership(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
`)
	defer block.Close(ctx)

	inMsg := service.NewMessage(nil)
	inMsg.SetStructuredMut(map[string]any{
		"hello": "world",
	})

	require.NoError(t, block.WriteBatch(ctx, service.MessageBatch{inMsg}, func(ctx context.Context, _ error) error {
		inStruct, err := inMsg.AsStructuredMut()
		require.NoError(t, err)
		_, err = gabs.Wrap(inStruct).Set("quack", "moo")
		require.NoError(t, err)
		return nil
	}))

	outBatch, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, outBatch, 1)

	outStruct, err := outBatch[0].AsStructuredMut()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"hello": "world",
	}, outStruct)

	require.NoError(t, ackFunc(ctx, nil))

	_, err = gabs.Wrap(outStruct).Set("woof", "meow")
	require.NoError(t, err)

	inStruct, err := inMsg.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"hello": "world",
		"moo":   "quack",
	}, inStruct)
}

func TestMemoryNearLimit(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 2285
`)
	defer block.Close(ctx)

	n, iter := 50, 5

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte("hello")),
				service.NewMessage([]byte("world")),
				service.NewMessage([]byte("12345")),
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				t.Error(err)
			}
		}

		for i := 0; i < n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(t, err)
			require.Len(t, m, 4)
			msgEqual(t, fmt.Sprintf("test%v", i), m[3])
			require.NoError(t, ackFunc(ctx, nil))
		}
	}
}

func TestMemoryLoopingRandom(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 8000
`)
	defer block.Close(ctx)

	n, iter := 50, 5

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			b := make([]byte, rand.Int()%100)
			for k := range b {
				b[k] = '0'
			}
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage(b),
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				t.Error(err)
			}
		}

		for i := 0; i < n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(t, err)
			require.Len(t, m, 2)
			msgEqual(t, fmt.Sprintf("test%v", i), m[1])
			require.NoError(t, ackFunc(ctx, nil))
		}
	}
}

func TestMemoryLockStep(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 1000
`)
	defer block.Close(ctx)

	n := 10000

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(t, err)
			require.Len(t, m, 4)
			msgEqual(t, fmt.Sprintf("test%v", i), m[3])
			require.NoError(t, ackFunc(ctx, nil))
		}
	}()

	go func() {
		for i := 0; i < n; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte("hello")),
				service.NewMessage([]byte("world")),
				service.NewMessage([]byte("12345")),
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}

func TestMemoryAck(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 1000
`)
	defer block.Close(ctx)

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("2")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "1", m[0])

	require.NoError(t, ackFunc(ctx, errors.New("nope")))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "1", m[0])

	require.NoError(t, ackFunc(ctx, nil))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "2", m[0])

	require.NoError(t, ackFunc(ctx, nil))

	block.EndOfInput()

	_, _, err = block.ReadBatch(ctx)
	require.Error(t, err)
	assert.Equal(t, service.ErrEndOfBuffer, err)
}

func TestMemoryCloseWithPending(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 1000
`)
	defer block.Close(ctx)

	for i := 0; i < 10; i++ {
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello world")),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			t.Error(err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		block.EndOfInput()
		wg.Done()
	}()

	<-time.After(time.Millisecond * 100)
	for i := 0; i < 10; i++ {
		m, ackFunc, err := block.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, m, 1)
		msgEqual(t, "hello world", m[0])
		require.NoError(t, ackFunc(ctx, nil))
	}

	_, _, err := block.ReadBatch(ctx)
	require.Error(t, err)
	assert.Equal(t, service.ErrEndOfBuffer, err)

	wg.Wait()
}

func TestMemoryRejectLargeMessage(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 10
`)
	defer block.Close(ctx)

	err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("hello world this message is too long!")),
	}, func(ctx context.Context, err error) error { return nil })
	require.Error(t, err)
	assert.Equal(t, component.ErrMessageTooLarge, err)

	require.NoError(t, block.Close(ctx))
}

func TestMemoryBatched(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
batch_policy:
  enabled: true
  count: 3
  processors:
    - archive:
        format: lines
`)
	defer block.Close(ctx)

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("hello1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("world1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("meow1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello1\nworld1\nmeow1", m[0])
	require.NoError(t, ackFunc(ctx, nil))

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("hello2")),
		service.NewMessage([]byte("world2")),
		service.NewMessage([]byte("meow2")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello2\nworld2\nmeow2", m[0])
	require.NoError(t, ackFunc(ctx, nil))
}

func TestMemoryBatchedNack(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
batch_policy:
  enabled: true
  count: 3
  processors:
    - archive:
        format: lines
`)
	defer block.Close(ctx)

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("hello1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("world1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("meow1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello1\nworld1\nmeow1", m[0])

	require.NoError(t, ackFunc(ctx, errors.New("nope")))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello1\nworld1\nmeow1", m[0])

	require.NoError(t, ackFunc(ctx, nil))
}

func TestMemoryBatchedEarlyTerm(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
batch_policy:
  enabled: true
  count: 3
  processors:
    - archive:
        format: lines
`)
	defer block.Close(ctx)

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("hello1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	if err := block.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte("world1")),
	}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	block.EndOfInput()

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello1\nworld1", m[0])
	require.NoError(t, ackFunc(ctx, nil))

	_, _, err = block.ReadBatch(ctx)
	assert.Equal(t, service.ErrEndOfBuffer, err)
}

func TestMemoryBatchedTimed(t *testing.T) {
	ctx := context.Background()
	block := memBufFromConf(t, `
limit: 100000
batch_policy:
  enabled: true
  count: 2
  period: 50ms
`)
	defer block.Close(ctx)

	go func() {
		<-time.After(time.Millisecond * 500)
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello")),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			t.Error(err)
		}
	}()

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqual(t, "hello", m[0])
	require.NoError(t, ackFunc(ctx, nil))
}
