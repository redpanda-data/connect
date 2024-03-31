package pure

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSystemWindowBufferConfigs(t *testing.T) {
	tests := []struct {
		config           string
		lintErrContains  string
		buildErrContains string
	}{
		{
			config: `
system_window:
  size: 60m
`,
		},
		{
			config: `
system_window: {}
`,
			lintErrContains: "field size is required",
		},
		{
			config: `
system_window:
  timestamp_mapping: 'root ='
  size: 60m
`,
			lintErrContains: "expected whitespace",
		},
		{
			config: `
system_window:
  size: 60m
  slide: 5m
  offset: 1m
  allowed_lateness: 2m
`,
		},
		{
			config: `
system_window:
  size: 60m
  slide: 120m
  offset: 1m
  allowed_lateness: 2m
`,
			buildErrContains: "invalid window slide",
		},
		{
			config: `
system_window:
  size: 60m
  offset: 60m
  allowed_lateness: 2m
`,
			buildErrContains: "invalid offset",
		},
		{
			config: `
system_window:
  size: 60m
  slide: 10m
  offset: 10m
  allowed_lateness: 2m
`,
			buildErrContains: "invalid offset",
		},
		{
			config: `
system_window:
  size: 60m
  slide: 10m
  allowed_lateness: 200m
`,
			buildErrContains: "invalid allowed_lateness",
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			env := service.NewStreamBuilder()
			require.NoError(t, env.SetLoggerYAML(`level: OFF`))
			err := env.AddConsumerFunc(func(context.Context, *service.Message) error {
				return nil
			})
			require.NoError(t, err)
			_, err = env.AddProducerFunc()
			require.NoError(t, err)

			err = env.SetBufferYAML(test.config)
			if test.lintErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.lintErrContains)
				return
			}
			require.NoError(t, err)

			strm, err := env.Build()
			require.NoError(t, err)

			cancelledCtx, done := context.WithCancel(context.Background())
			done()
			err = strm.Run(cancelledCtx)
			if test.buildErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.buildErrContains)
				return
			}
			require.EqualError(t, err, "context canceled")
			require.NoError(t, strm.StopWithin(time.Second))
		})
	}
}

func TestSystemCurrentWindowCalc(t *testing.T) {
	tests := []struct {
		now                            string
		size, slide, offset            time.Duration
		prevStart, prevEnd, start, end string
	}{
		{
			now:       `2006-01-02T15:00:00Z`,
			size:      time.Hour,
			start:     `2006-01-02T14:00:00.000000001Z`,
			end:       `2006-01-02T15:00:00Z`,
			prevStart: `2006-01-02T13:00:00.000000001Z`,
			prevEnd:   `2006-01-02T14:00:00Z`,
		},
		{
			now:       `2006-01-02T15:00:00.000000001Z`,
			size:      time.Hour,
			start:     `2006-01-02T15:00:00.000000001Z`,
			end:       `2006-01-02T16:00:00Z`,
			prevStart: `2006-01-02T14:00:00.000000001Z`,
			prevEnd:   `2006-01-02T15:00:00Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			start:     `2006-01-02T15:00:00.000000001Z`,
			end:       `2006-01-02T16:00:00Z`,
			prevStart: `2006-01-02T14:00:00.000000001Z`,
			prevEnd:   `2006-01-02T15:00:00Z`,
		},
		{
			now:       `2006-01-02T15:34:05.123456789Z`,
			size:      time.Hour,
			start:     `2006-01-02T15:00:00.000000001Z`,
			end:       `2006-01-02T16:00:00Z`,
			prevStart: `2006-01-02T14:00:00.000000001Z`,
			prevEnd:   `2006-01-02T15:00:00Z`,
		},
		{
			now:       `2006-01-02T00:04:05.123456789Z`,
			size:      time.Hour,
			start:     `2006-01-02T00:00:00.000000001Z`,
			end:       `2006-01-02T01:00:00Z`,
			prevStart: `2006-01-01T23:00:00.000000001Z`,
			prevEnd:   `2006-01-02T00:00:00Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			start:     `2006-01-02T14:10:00.000000001Z`,
			end:       `2006-01-02T15:10:00Z`,
			prevStart: `2006-01-02T14:00:00.000000001Z`,
			prevEnd:   `2006-01-02T15:00:00Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			offset:    time.Minute * 30,
			start:     `2006-01-02T14:30:00.000000001Z`,
			end:       `2006-01-02T15:30:00Z`,
			prevStart: `2006-01-02T13:30:00.000000001Z`,
			prevEnd:   `2006-01-02T14:30:00Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			offset:    time.Minute * 5,
			start:     `2006-01-02T14:05:00.000000001Z`,
			end:       `2006-01-02T15:05:00Z`,
			prevStart: `2006-01-02T13:55:00.000000001Z`,
			prevEnd:   `2006-01-02T14:55:00Z`,
		},
		{
			now:       `2006-01-02T15:09:59.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			offset:    time.Minute * 5,
			start:     `2006-01-02T14:15:00.000000001Z`,
			end:       `2006-01-02T15:15:00Z`,
			prevStart: `2006-01-02T14:05:00.000000001Z`,
			prevEnd:   `2006-01-02T15:05:00Z`,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			w, err := newSystemWindowBuffer(nil, func() time.Time {
				ts, err := time.Parse(time.RFC3339Nano, test.now)
				require.NoError(t, err)
				return ts.UTC()
			}, test.size, test.slide, test.offset, 0, nil)
			require.NoError(t, err)

			prevStart, prevEnd, start, end := w.nextSystemWindow()

			assert.Equal(t, test.start, start.Format(time.RFC3339Nano), "start")
			assert.Equal(t, test.end, end.Format(time.RFC3339Nano), "end")
			assert.Equal(t, test.prevStart, prevStart.Format(time.RFC3339Nano), "prevStart")
			assert.Equal(t, test.prevEnd, prevEnd.Format(time.RFC3339Nano), "prevEnd")
		})
	}
}

func noopAck(context.Context, error) error {
	return nil
}

func TestSystemWindowWritePurge(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 1).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":7.999999999}`)),
		service.NewMessage([]byte(`{"id":"2","ts":8.5}`)),
		service.NewMessage([]byte(`{"id":"3","ts":9.5}`)),
		service.NewMessage([]byte(`{"id":"4","ts":10.5}`)),
	}, noopAck)
	require.NoError(t, err)
	assert.Len(t, w.pending, 4)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"5","ts":10.6}`)),
		service.NewMessage([]byte(`{"id":"6","ts":10.7}`)),
		service.NewMessage([]byte(`{"id":"7","ts":10.8}`)),
		service.NewMessage([]byte(`{"id":"8","ts":10.9}`)),
	}, noopAck)
	require.NoError(t, err)
	assert.Len(t, w.pending, 6)
}

func TestSystemWindowCreation(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 1).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":7.999999999}`)),
		service.NewMessage([]byte(`{"id":"2","ts":8.5}`)),
		service.NewMessage([]byte(`{"id":"3","ts":9.5}`)),
		service.NewMessage([]byte(`{"id":"4","ts":10.5}`)),
	}, noopAck)
	require.NoError(t, err)
	assert.Len(t, w.pending, 4)

	resBatch, _, err := w.ReadBatch(context.Background())
	require.NoError(t, err)

	require.Len(t, resBatch, 1)
	msgBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"3","ts":9.5}`, string(msgBytes))

	assert.Len(t, w.pending, 1)
	assert.Equal(t, "1970-01-01T00:00:10Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	currentTS = time.Unix(10, 999999100).UTC()

	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	require.Len(t, resBatch, 1)
	msgBytes, err = resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"4","ts":10.5}`, string(msgBytes))

	assert.Empty(t, w.pending)
	assert.Equal(t, "1970-01-01T00:00:11Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	currentTS = time.Unix(11, 999999100).UTC()

	smallWaitCtx, done := context.WithTimeout(context.Background(), time.Millisecond*50)
	resBatch, _, err = w.ReadBatch(smallWaitCtx)
	done()
	require.Error(t, err)
	assert.Empty(t, resBatch)
	assert.Equal(t, "1970-01-01T00:00:12Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"5","ts":8.1}`)),
		service.NewMessage([]byte(`{"id":"6","ts":9.999999999}`)),
		service.NewMessage([]byte(`{"id":"7","ts":10}`)),
		service.NewMessage([]byte(`{"id":"8","ts":11.999999999}`)),
		service.NewMessage([]byte(`{"id":"9","ts":12.1}`)),
		service.NewMessage([]byte(`{"id":"10","ts":13}`)),
	}, noopAck)
	require.NoError(t, err)
	require.Len(t, w.pending, 2)

	msgBytes, err = w.pending[0].m.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"9","ts":12.1}`, string(msgBytes))

	msgBytes, err = w.pending[1].m.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"10","ts":13}`, string(msgBytes))
}

func TestSystemWindowCreationSliding(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 0).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, time.Millisecond*500, 0, 0, nil)
	require.NoError(t, err)
	w.latestFlushedWindowEnd = time.Unix(9, 500_000_000)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":9.85}`)),
		service.NewMessage([]byte(`{"id":"2","ts":9.9}`)),
		service.NewMessage([]byte(`{"id":"3","ts":10.15}`)),
		service.NewMessage([]byte(`{"id":"4","ts":10.3}`)),
		service.NewMessage([]byte(`{"id":"5","ts":10.5}`)),
		service.NewMessage([]byte(`{"id":"6","ts":10.7}`)),
		service.NewMessage([]byte(`{"id":"7","ts":10.9}`)),
		service.NewMessage([]byte(`{"id":"8","ts":11.1}`)),
		service.NewMessage([]byte(`{"id":"9","ts":11.35}`)),
		service.NewMessage([]byte(`{"id":"10","ts":11.52}`)),
		service.NewMessage([]byte(`{"id":"11","ts":11.8}`)),
	}, noopAck)
	require.NoError(t, err)
	assert.Len(t, w.pending, 11)

	assertBatchIndex := func(i int, batch service.MessageBatch, exp string) {
		t.Helper()
		require.Greater(t, len(batch), i)
		msgBytes, err := batch[i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, exp, string(msgBytes))
	}

	resBatch, _, err := w.ReadBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, resBatch, 2)
	assertBatchIndex(0, resBatch, `{"id":"1","ts":9.85}`)
	assertBatchIndex(1, resBatch, `{"id":"2","ts":9.9}`)

	currentTS = time.Unix(10, 500000000).UTC()
	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, resBatch, 5)
	assertBatchIndex(0, resBatch, `{"id":"1","ts":9.85}`)
	assertBatchIndex(1, resBatch, `{"id":"2","ts":9.9}`)
	assertBatchIndex(2, resBatch, `{"id":"3","ts":10.15}`)
	assertBatchIndex(3, resBatch, `{"id":"4","ts":10.3}`)
	assertBatchIndex(4, resBatch, `{"id":"5","ts":10.5}`)

	currentTS = time.Unix(11, 0).UTC()
	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, resBatch, 5)
	assertBatchIndex(0, resBatch, `{"id":"3","ts":10.15}`)
	assertBatchIndex(1, resBatch, `{"id":"4","ts":10.3}`)
	assertBatchIndex(2, resBatch, `{"id":"5","ts":10.5}`)
	assertBatchIndex(3, resBatch, `{"id":"6","ts":10.7}`)
	assertBatchIndex(4, resBatch, `{"id":"7","ts":10.9}`)

	currentTS = time.Unix(11, 500_000_000).UTC()
	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, resBatch, 4)
	assertBatchIndex(0, resBatch, `{"id":"6","ts":10.7}`)
	assertBatchIndex(1, resBatch, `{"id":"7","ts":10.9}`)
	assertBatchIndex(2, resBatch, `{"id":"8","ts":11.1}`)
	assertBatchIndex(3, resBatch, `{"id":"9","ts":11.35}`)

	currentTS = time.Unix(12, 0).UTC()
	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	assert.Len(t, resBatch, 4)
	assertBatchIndex(0, resBatch, `{"id":"8","ts":11.1}`)
	assertBatchIndex(1, resBatch, `{"id":"9","ts":11.35}`)
	assertBatchIndex(2, resBatch, `{"id":"10","ts":11.52}`)
	assertBatchIndex(3, resBatch, `{"id":"11","ts":11.8}`)
}

func TestSystemWindowAckOneToMany(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 1).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	var ackCalled int
	var ackErr error

	require.NoError(t, w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":9.5}`)),
		service.NewMessage([]byte(`{"id":"2","ts":10.5}`)),
		service.NewMessage([]byte(`{"id":"3","ts":11.5}`)),
	}, func(ctx context.Context, err error) error {
		ackCalled++
		if err != nil {
			ackErr = err
		}
		return nil
	}))

	ackFuncs := []service.AckFunc{}

	resBatch, aFn, err := w.ReadBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, resBatch, 1)
	msgBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"1","ts":9.5}`, string(msgBytes))
	ackFuncs = append(ackFuncs, aFn)

	currentTS = time.Unix(11, 0).UTC()

	resBatch, aFn, err = w.ReadBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, resBatch, 1)
	msgBytes, err = resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"2","ts":10.5}`, string(msgBytes))
	ackFuncs = append(ackFuncs, aFn)

	currentTS = time.Unix(12, 0).UTC()

	resBatch, aFn, err = w.ReadBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, resBatch, 1)
	msgBytes, err = resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"3","ts":11.5}`, string(msgBytes))
	ackFuncs = append(ackFuncs, aFn)

	require.Len(t, ackFuncs, 3)
	assert.Equal(t, 0, ackCalled)
	assert.NoError(t, ackErr)

	require.NoError(t, ackFuncs[0](context.Background(), nil))
	assert.Equal(t, 0, ackCalled)
	assert.NoError(t, ackErr)

	require.NoError(t, ackFuncs[1](context.Background(), errors.New("custom error")))
	assert.Equal(t, 0, ackCalled)
	assert.NoError(t, ackErr)

	require.NoError(t, ackFuncs[2](context.Background(), nil))
	assert.Equal(t, 1, ackCalled)
	assert.EqualError(t, ackErr, "custom error")
}

func TestSystemWindowAckManyToOne(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 1).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	ackCalls := map[int]error{}

	require.NoError(t, w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":9.5}`)),
	}, func(ctx context.Context, err error) error {
		ackCalls[0] = err
		return nil
	}))

	require.NoError(t, w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"2","ts":9.6}`)),
	}, func(ctx context.Context, err error) error {
		ackCalls[1] = err
		return nil
	}))

	require.NoError(t, w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"3","ts":9.7}`)),
	}, func(ctx context.Context, err error) error {
		ackCalls[2] = err
		return nil
	}))

	resBatch, aFn, err := w.ReadBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, resBatch, 3)

	msgBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"1","ts":9.5}`, string(msgBytes))

	msgBytes, err = resBatch[1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"2","ts":9.6}`, string(msgBytes))

	msgBytes, err = resBatch[2].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"3","ts":9.7}`, string(msgBytes))

	assert.Empty(t, ackCalls)
	require.NoError(t, aFn(context.Background(), errors.New("custom error")))

	assert.Equal(t, map[int]error{
		0: errors.New("custom error"),
		1: errors.New("custom error"),
		2: errors.New("custom error"),
	}, ackCalls)
}

func TestSystemWindowParallelReadAndWrites(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 500000000).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	startChan := make(chan struct{})
	go func() {
		defer wg.Done()
		<-startChan
		for i := 0; i < 1000; i++ {
			msg := fmt.Sprintf(`{"id":"%v","ts":10.5}`, i)
			writeErr := w.WriteBatch(context.Background(), service.MessageBatch{
				service.NewMessage([]byte(msg)),
			}, func(ctx context.Context, err error) error {
				return nil
			})
			require.NoError(t, writeErr)
		}
	}()
	go func() {
		defer wg.Done()
		<-startChan
		_, _, readErr := w.ReadBatch(context.Background())
		require.NoError(t, readErr)
	}()

	close(startChan)
	wg.Wait()
}

func TestSystemWindowOwnership(t *testing.T) {
	ctx := context.Background()

	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 1).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	inMsg := service.NewMessage(nil)
	inMsg.SetStructuredMut(map[string]any{
		"hello": "world",
		"ts":    10,
	})

	err = w.WriteBatch(ctx, service.MessageBatch{inMsg}, func(ctx context.Context, _ error) error {
		inStruct, err := inMsg.AsStructuredMut()
		require.NoError(t, err)
		_, err = gabs.Wrap(inStruct).Set("quack", "moo")
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, w.pending, 1)

	outBatch, ackFunc, err := w.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, outBatch, 1)

	outStruct, err := outBatch[0].AsStructuredMut()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"hello": "world",
		"ts":    10,
	}, outStruct)

	require.NoError(t, ackFunc(ctx, nil))

	_, err = gabs.Wrap(outStruct).Set("woof", "meow")
	require.NoError(t, err)

	inStruct, err := inMsg.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"hello": "world",
		"moo":   "quack",
		"ts":    10,
	}, inStruct)
}
