package generic

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/public/bloblang"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
  slide: 10m
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
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			prevStart: `2006-01-02T14:00:00Z`,
			prevEnd:   `2006-01-02T14:59:59.999999999Z`,
			start:     `2006-01-02T15:00:00Z`,
			end:       `2006-01-02T15:59:59.999999999Z`,
		},
		{
			now:       `2006-01-02T15:34:05.123456789Z`,
			size:      time.Hour,
			prevStart: `2006-01-02T14:00:00Z`,
			prevEnd:   `2006-01-02T14:59:59.999999999Z`,
			start:     `2006-01-02T15:00:00Z`,
			end:       `2006-01-02T15:59:59.999999999Z`,
		},
		{
			now:       `2006-01-02T00:04:05.123456789Z`,
			size:      time.Hour,
			prevStart: `2006-01-01T23:00:00Z`,
			prevEnd:   `2006-01-01T23:59:59.999999999Z`,
			start:     `2006-01-02T00:00:00Z`,
			end:       `2006-01-02T00:59:59.999999999Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			prevStart: `2006-01-02T14:50:00Z`,
			prevEnd:   `2006-01-02T15:49:59.999999999Z`,
			start:     `2006-01-02T15:00:00Z`,
			end:       `2006-01-02T15:59:59.999999999Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			offset:    time.Minute * 30,
			prevStart: `2006-01-02T13:30:00Z`,
			prevEnd:   `2006-01-02T14:29:59.999999999Z`,
			start:     `2006-01-02T14:30:00Z`,
			end:       `2006-01-02T15:29:59.999999999Z`,
		},
		{
			now:       `2006-01-02T15:04:05.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			offset:    time.Minute * 30,
			prevStart: `2006-01-02T14:20:00Z`,
			prevEnd:   `2006-01-02T15:19:59.999999999Z`,
			start:     `2006-01-02T14:30:00Z`,
			end:       `2006-01-02T15:29:59.999999999Z`,
		},
		{
			now:       `2006-01-02T15:09:59.123456789Z`,
			size:      time.Hour,
			slide:     time.Minute * 10,
			offset:    time.Minute * 30,
			prevStart: `2006-01-02T14:20:00Z`,
			prevEnd:   `2006-01-02T15:19:59.999999999Z`,
			start:     `2006-01-02T14:30:00Z`,
			end:       `2006-01-02T15:29:59.999999999Z`,
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

			prevStart, prevEnd, start, end := w.currentSystemWindow()

			assert.Equal(t, test.prevStart, prevStart.Format(time.RFC3339Nano), "prevStart")
			assert.Equal(t, test.prevEnd, prevEnd.Format(time.RFC3339Nano), "prevEnd")
			assert.Equal(t, test.start, start.Format(time.RFC3339Nano), "start")
			assert.Equal(t, test.end, end.Format(time.RFC3339Nano), "end")
		})
	}
}

func TestSystemWindowWritePurge(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 0).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":7.999999999}`)),
		service.NewMessage([]byte(`{"id":"2","ts":8.5}`)),
		service.NewMessage([]byte(`{"id":"3","ts":9.5}`)),
		service.NewMessage([]byte(`{"id":"4","ts":10.5}`)),
	})
	require.NoError(t, err)
	assert.Len(t, w.pending, 4)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"5","ts":10.6}`)),
		service.NewMessage([]byte(`{"id":"6","ts":10.7}`)),
		service.NewMessage([]byte(`{"id":"7","ts":10.8}`)),
		service.NewMessage([]byte(`{"id":"8","ts":10.9}`)),
	})
	require.NoError(t, err)
	assert.Len(t, w.pending, 6)
}

func TestSystemWindowCreation(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.ts`)
	require.NoError(t, err)

	currentTS := time.Unix(10, 0).UTC()
	w, err := newSystemWindowBuffer(mapping, func() time.Time {
		return currentTS
	}, time.Second, 0, 0, 0, nil)
	require.NoError(t, err)

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"1","ts":7.999999999}`)),
		service.NewMessage([]byte(`{"id":"2","ts":8.5}`)),
		service.NewMessage([]byte(`{"id":"3","ts":9.5}`)),
		service.NewMessage([]byte(`{"id":"4","ts":10.5}`)),
	})
	require.NoError(t, err)
	assert.Len(t, w.pending, 4)

	resBatch, _, err := w.ReadBatch(context.Background())
	require.NoError(t, err)

	require.Len(t, resBatch, 1)
	msgBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"3","ts":9.5}`, string(msgBytes))

	assert.Len(t, w.pending, 1)
	assert.Equal(t, "1970-01-01T00:00:09.999999999Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	currentTS = time.Unix(10, 999999100).UTC()

	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)

	require.Len(t, resBatch, 1)
	msgBytes, err = resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"4","ts":10.5}`, string(msgBytes))

	assert.Len(t, w.pending, 0)
	assert.Equal(t, "1970-01-01T00:00:10.999999999Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	currentTS = time.Unix(11, 999999100).UTC()

	resBatch, _, err = w.ReadBatch(context.Background())
	require.NoError(t, err)
	assert.Len(t, resBatch, 0)
	assert.Equal(t, "1970-01-01T00:00:11.999999999Z", w.latestFlushedWindowEnd.Format(time.RFC3339Nano))

	err = w.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"5","ts":8}`)),
		service.NewMessage([]byte(`{"id":"6","ts":9.999999999}`)),
		service.NewMessage([]byte(`{"id":"7","ts":10}`)),
		service.NewMessage([]byte(`{"id":"8","ts":11.999999999}`)),
		service.NewMessage([]byte(`{"id":"9","ts":12}`)),
		service.NewMessage([]byte(`{"id":"10","ts":13}`)),
	})
	require.NoError(t, err)
	require.Len(t, w.pending, 2)

	msgBytes, err = w.pending[0].m.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"9","ts":12}`, string(msgBytes))

	msgBytes, err = w.pending[1].m.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"id":"10","ts":13}`, string(msgBytes))
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
