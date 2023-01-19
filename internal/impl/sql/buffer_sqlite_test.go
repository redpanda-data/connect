package sql_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/sql"
	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/pure/extended"
)

func msgEqualStr(t testing.TB, expected string, m *service.Message) {
	t.Helper()

	mBytes, err := m.AsBytes()
	require.NoError(t, err)

	assert.Equal(t, expected, string(mBytes))
}

func msgEqual(t testing.TB, exp, act *service.Message) {
	t.Helper()

	expBytes, err := exp.AsBytes()
	require.NoError(t, err)

	actBytes, err := act.AsBytes()
	require.NoError(t, err)

	expectedKeys := map[string]any{}
	_ = exp.MetaWalkMut(func(key string, value any) error {
		expectedKeys[key] = value
		return nil
	})
	_ = act.MetaWalkMut(func(key string, actV any) error {
		expV, exists := expectedKeys[key]
		assert.True(t, exists, "meta key %v expected", key)
		assert.Equal(t, expV, actV, "meta key %v matches", key)
		delete(expectedKeys, key)
		return nil
	})
	assert.Empty(t, expectedKeys, "metadata keys in message")

	assert.Equal(t, string(expBytes), string(actBytes), "content matches")
}

func memBufFromConf(t testing.TB, conf string) *sql.SQLiteBuffer {
	t.Helper()

	parsedConf, err := sql.SQLiteBufferConfig().ParseYAML(conf, nil)
	require.NoError(t, err)

	buf, err := sql.NewSQLiteBufferFromConfig(parsedConf, service.MockResources())
	require.NoError(t, err)

	return buf
}

func TestBufferSQLiteBasic(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	n := 100

	for i := 0; i < n; i++ {
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < n; i++ {
		m, ackFunc, err := block.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, m, 1, i)
		msgEqualStr(t, fmt.Sprintf("test%v", i), m[0])
		require.NoError(t, ackFunc(ctx, nil))
	}
}

func TestBufferSQLiteBatchPreservation(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	msgA := service.NewMessage([]byte("hello world a"))
	msgA.MetaSet("a", "first")
	msgB := service.NewMessage([]byte("hello world b"))
	msgB.MetaSet("b", "second")
	msgB.MetaSet("c", "third")
	msgC := service.NewMessage([]byte("hello world c"))

	if err := block.WriteBatch(ctx, service.MessageBatch{msgA, msgB, msgC}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 3)

	msgEqual(t, msgA, m[0])
	msgEqual(t, msgB, m[1])
	msgEqual(t, msgC, m[2])
	require.NoError(t, ackFunc(ctx, nil))
}

func TestBufferSQLiteBatchSplit(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
post_processors:
  - split: {}
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	msgA := service.NewMessage([]byte("hello world a"))
	msgA.MetaSet("a", "first")
	msgB := service.NewMessage([]byte("hello world b"))
	msgB.MetaSet("b", "second")
	msgB.MetaSet("c", "third")
	msgC := service.NewMessage([]byte("hello world c"))

	if err := block.WriteBatch(ctx, service.MessageBatch{msgA, msgB, msgC}, func(ctx context.Context, err error) error { return nil }); err != nil {
		t.Error(err)
	}

	for i, expMsg := range []*service.Message{msgA, msgB, msgC} {
		m, ackFunc, err := block.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, m, 1, i)

		msgEqual(t, expMsg, m[0])
		require.NoError(t, ackFunc(ctx, nil))
	}
}

func TestBufferSQLiteProcessors(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
pre_processors:
  - mapping: 'root = this.format_msgpack()'
post_processors:
  - mapping: 'root = content().parse_msgpack()'
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	n, m := 100, 10

	for i := 0; i < n; i++ {
		var inBatch service.MessageBatch
		for j := 0; j < m; j++ {
			inBatch = append(inBatch, service.NewMessage(fmt.Appendf(nil, `{"id":"test%v","n":%v}`, i, j)))
		}
		if err := block.WriteBatch(ctx, inBatch, func(ctx context.Context, err error) error { return nil }); err != nil {
			t.Error(err)
		}
	}

	for i := 0; i < n; i++ {
		outBatch, ackFunc, err := block.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, outBatch, m, i)
		msgEqualStr(t, fmt.Sprintf(`{"id":"test%v","n":0}`, i), outBatch[0])
		require.NoError(t, ackFunc(ctx, nil))
	}
}

func TestBufferSQLiteOwnership(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
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

func TestBufferSQLiteLoopingRandom(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	n, iter := 10, 5

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				t.Error(err)
			}
		}

		for i := 0; i < n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(t, err)
			require.Len(t, m, 1)
			msgEqualStr(t, fmt.Sprintf("test%v", i), m[0])
			require.NoError(t, ackFunc(ctx, nil))
		}
	}
}

func TestBufferSQLiteLockStep(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	n := 100

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(t, err)
			require.Len(t, m, 1)
			msgEqualStr(t, fmt.Sprintf("test%v", i), m[0])
			require.NoError(t, ackFunc(ctx, nil))
		}
	}()

	go func() {
		for i := 0; i < n; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				t.Error(err)
			}
		}
	}()

	wg.Wait()
}

func TestBufferSQLiteAck(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
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
	msgEqualStr(t, "1", m[0])

	require.NoError(t, ackFunc(ctx, errors.New("nope")))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "1", m[0])

	require.NoError(t, ackFunc(ctx, nil))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "2", m[0])

	require.NoError(t, ackFunc(ctx, nil))

	block.EndOfInput()

	_, _, err = block.ReadBatch(ctx)
	require.Error(t, err)
	assert.Equal(t, service.ErrEndOfBuffer, err)
}

func TestBufferSQLiteCloseWithPending(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	block := memBufFromConf(t, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
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
		msgEqualStr(t, "hello world", m[0])
		require.NoError(t, ackFunc(ctx, nil))
	}

	_, _, err := block.ReadBatch(ctx)
	require.Error(t, err)
	assert.Equal(t, service.ErrEndOfBuffer, err)

	wg.Wait()
}

func TestBufferSQLiteCloseAfterNack(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	conf := fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db"))

	block := memBufFromConf(t, conf)

	for _, testMsg := range []string{
		"hello world 1",
		"hello world 2",
		"hello world 3",
	} {
		require.NoError(t, block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte(testMsg)),
		}, func(ctx context.Context, err error) error { return nil }))
	}

	m, ackFuncA, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "hello world 1", m[0])

	m, ackFuncB, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "hello world 2", m[0])

	require.NoError(t, ackFuncA(ctx, errors.New("nope")))
	require.NoError(t, ackFuncB(ctx, nil))

	// Restart
	require.NoError(t, block.Close(ctx))
	block = memBufFromConf(t, conf)

	m, ackFunc, err := block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "hello world 1", m[0])
	require.NoError(t, ackFunc(ctx, nil))

	m, ackFunc, err = block.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, m, 1)
	msgEqualStr(t, "hello world 3", m[0])
	require.NoError(t, ackFunc(ctx, nil))

	require.NoError(t, block.Close(ctx))
}

func BenchmarkBufferSQLiteWrites(b *testing.B) {
	tmpDir := b.TempDir()

	ctx := context.Background()
	block := memBufFromConf(b, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkBufferSQLiteReads(b *testing.B) {
	tmpDir := b.TempDir()

	ctx := context.Background()
	block := memBufFromConf(b, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	for i := 0; i < b.N; i++ {
		if err := block.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
		}, func(ctx context.Context, err error) error { return nil }); err != nil {
			b.Error(err)
		}
	}

	block.EndOfInput()

	b.ResetTimer()
	b.ReportAllocs()

	for {
		m, ackFunc, err := block.ReadBatch(ctx)
		if errors.Is(err, service.ErrEndOfBuffer) {
			break
		}
		require.NoError(b, err)
		require.Len(b, m, 1)
		require.NoError(b, ackFunc(ctx, nil))
	}
}

func BenchmarkBufferSQLiteLockStep(b *testing.B) {
	tmpDir := b.TempDir()

	ctx := context.Background()
	block := memBufFromConf(b, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	wg := sync.WaitGroup{}
	wg.Add(1)

	b.ReportAllocs()
	b.ResetTimer()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(b, err)
			require.Len(b, m, 1)
			msgEqualStr(b, fmt.Sprintf("test%v", i), m[0])
			require.NoError(b, ackFunc(ctx, nil))
		}
	}()

	go func() {
		for i := 0; i < b.N; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(fmt.Sprintf("test%v", i))),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				b.Error(err)
			}
		}
	}()

	wg.Wait()
}

func BenchmarkBufferSQLiteLockStepLarge(b *testing.B) {
	tmpDir := b.TempDir()

	ctx := context.Background()
	block := memBufFromConf(b, fmt.Sprintf(`
path: "%v"
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	wg := sync.WaitGroup{}
	wg.Add(1)

	testMsg := []byte(strings.Repeat("heh nice one, kid ", 10000))

	b.ReportAllocs()
	b.ResetTimer()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(b, err)
			require.Len(b, m, 1)
			require.NoError(b, ackFunc(ctx, nil))
		}
	}()

	go func() {
		for i := 0; i < b.N; i++ {
			if err := block.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage(testMsg),
			}, func(ctx context.Context, err error) error { return nil }); err != nil {
				b.Error(err)
			}
		}
	}()

	wg.Wait()
}

func BenchmarkBufferSQLiteBatch1(b *testing.B) {
	benchmarkBufferSQLiteProcsBatchedN(b, 1)
}

func BenchmarkBufferSQLiteBatch10(b *testing.B) {
	benchmarkBufferSQLiteProcsBatchedN(b, 10)
}

func BenchmarkBufferSQLiteBatch100(b *testing.B) {
	benchmarkBufferSQLiteProcsBatchedN(b, 100)
}

func benchmarkBufferSQLiteProcsBatchedN(b *testing.B, n int) {
	tmpDir := b.TempDir()

	ctx := context.Background()
	block := memBufFromConf(b, fmt.Sprintf(`
path: "%v"
pre_processors:
  - mapping: 'root = this.format_msgpack()'
post_processors:
  - mapping: 'root = this.parse_msgpack()'
`, filepath.Join(tmpDir, "foo.db")))
	defer block.Close(ctx)

	wg := sync.WaitGroup{}
	wg.Add(1)

	b.ReportAllocs()
	b.ResetTimer()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N/n; i++ {
			m, ackFunc, err := block.ReadBatch(ctx)
			require.NoError(b, err)
			require.Len(b, m, n)
			require.NoError(b, ackFunc(ctx, nil))
		}
	}()

	go func() {
		for i := 0; i < b.N/n; i++ {
			batch := make(service.MessageBatch, n)
			for bi := range batch {
				batch[bi] = service.NewMessage(fmt.Appendf(nil, `{"n":"%v","b":"%v"}`, i, bi))
			}
			if err := block.WriteBatch(ctx, batch, func(ctx context.Context, err error) error { return nil }); err != nil {
				b.Error(err)
			}
		}
	}()

	wg.Wait()
}
