// Copyright 2026 Redpanda Data, Inc.
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

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// nackTestInput is a batch input handing out the batches it is given and
// recording the acknowledgement errors it receives.
type nackTestInput struct {
	batches []service.MessageBatch

	mu   sync.Mutex
	acks []error
}

func (*nackTestInput) Connect(context.Context) error { return nil }

func (n *nackTestInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	if len(n.batches) == 0 {
		return nil, nil, service.ErrEndOfInput
	}
	b := n.batches[0]
	n.batches = n.batches[1:]
	return b, func(_ context.Context, err error) error {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.acks = append(n.acks, err)
		return nil
	}, nil
}

func (*nackTestInput) Close(context.Context) error { return nil }

func kafkaBatch(topic string, partition, offset, size int) service.MessageBatch {
	batch := make(service.MessageBatch, size)
	for i := range batch {
		msg := service.NewMessage(fmt.Appendf(nil, "msg-%d", offset+i))
		msg.MetaSetMut("kafka_topic", topic)
		msg.MetaSetMut("kafka_partition", partition)
		msg.MetaSetMut("kafka_offset", offset+i)
		batch[i] = msg
	}
	return batch
}

// logCapture is a slog handler keeping the level and message of every record.
type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (*logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r)
	return nil
}

func (l *logCapture) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logCapture) WithGroup(string) slog.Handler { return l }

func (l *logCapture) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if r.Level == slog.LevelWarn {
			out = append(out, r.Message)
		}
	}
	return out
}

// countingMetricsExporter records counter totals by name and label values so
// the test can observe the wrapper's counter.
type countingMetricsExporter struct {
	mu       sync.Mutex
	counters map[string]int64
}

type countingMetricsCounter struct {
	e   *countingMetricsExporter
	key string
}

func (c countingMetricsCounter) Incr(count int64) { c.e.add(c.key, count) }

func (c countingMetricsCounter) IncrFloat64(count float64) { c.Incr(int64(count)) }

type noopMetricsTimer struct{}

func (noopMetricsTimer) Timing(int64) {}

type noopMetricsGauge struct{}

func (noopMetricsGauge) Set(int64)           {}
func (noopMetricsGauge) SetFloat64(float64)  {}
func (noopMetricsGauge) IncrFloat64(float64) {}
func (noopMetricsGauge) DecrFloat64(float64) {}

func (e *countingMetricsExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(labelValues ...string) service.MetricsExporterCounter {
		return countingMetricsCounter{e: e, key: name + "{" + strings.Join(labelValues, ",") + "}"}
	}
}

func (*countingMetricsExporter) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopMetricsTimer{} }
}

func (*countingMetricsExporter) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopMetricsGauge{} }
}

func (*countingMetricsExporter) Close(context.Context) error { return nil }

func (e *countingMetricsExporter) add(key string, count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.counters[key] += count
}

func (e *countingMetricsExporter) counter(key string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counters[key]
}

// newNackTestResources returns resources whose logger and metrics are
// observable by the test.
func newNackTestResources(t *testing.T) (*service.Resources, *logCapture, *countingMetricsExporter) {
	t.Helper()
	logs := &logCapture{}
	exporter := &countingMetricsExporter{counters: map[string]int64{}}

	env := service.NewEmptyEnvironment()
	require.NoError(t, env.RegisterMetricsExporter("counting", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return exporter, nil }))
	// The resource builder defaults to the "none" tracer, which lives in a
	// plugin package this test binary does not import.
	require.NoError(t, env.RegisterOtelTracerProvider("none", service.NewConfigSpec(),
		func(*service.ParsedConfig) (trace.TracerProvider, error) { return noop.NewTracerProvider(), nil }))
	builder := env.NewResourceBuilder()
	require.NoError(t, builder.SetMetricsYAML("counting: {}"))
	builder.SetLogger(slog.New(logs))
	res, closeFn, err := builder.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn(context.Background()) })
	return res, logs, exporter
}

const testNackWarnInterval = 10 * time.Second

func TestNackWarningInputReplay(t *testing.T) {
	res, logs, metrics := newNackTestResources(t)

	child := &nackTestInput{batches: []service.MessageBatch{
		kafkaBatch("foo", 0, 10, 2),
		kafkaBatch("foo", 0, 10, 2),
		kafkaBatch("foo", 0, 10, 2),
		kafkaBatch("foo", 0, 10, 2),
		kafkaBatch("foo", 1, 50, 1),
		kafkaBatch("foo", 0, 10, 2),
	}}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := newNackWarningInput(res, NackMetricNameRedpanda, true, testNackWarnInterval, child)
	in.now = func() time.Time { return now }

	rejected := errors.New("boom")
	nack := func() {
		t.Helper()
		batch, aFn, err := in.ReadBatch(t.Context())
		require.NoError(t, err)
		require.Len(t, batch, 2)
		require.NoError(t, aFn(t.Context(), rejected))
	}

	// The first rejection warns right away.
	nack()
	warnings := logs.warnings()
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "Output rejected 2 message(s) of topic foo partition 0 starting at offset 10")
	assert.Contains(t, warnings[0], "1 consecutive rejection(s) on this partition")
	assert.Contains(t, warnings[0], "2 message(s) rejected since the last warning")
	assert.Contains(t, warnings[0], "replayed until they are delivered")
	assert.Contains(t, warnings[0], "Error: boom")

	// Further rejections inside the interval are counted but not logged.
	now = now.Add(testNackWarnInterval / 2)
	nack()
	nack()
	assert.Len(t, logs.warnings(), 1)

	// Once the interval has passed the next rejection warns again, summing
	// up what happened since.
	now = now.Add(testNackWarnInterval)
	nack()
	warnings = logs.warnings()
	require.Len(t, warnings, 2)
	assert.Contains(t, warnings[1], "4 consecutive rejection(s) on this partition")
	assert.Contains(t, warnings[1], "6 message(s) rejected since the last warning")

	// A successful delivery on the partition resets its consecutive count,
	// while another partition's count is independent.
	batch, aFn, err := in.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	now = now.Add(testNackWarnInterval)
	require.NoError(t, aFn(t.Context(), rejected))
	warnings = logs.warnings()
	require.Len(t, warnings, 3)
	assert.Contains(t, warnings[2], "topic foo partition 1 starting at offset 50")
	assert.Contains(t, warnings[2], "1 consecutive rejection(s) on this partition")

	batch, aFn, err = in.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.NoError(t, aFn(t.Context(), nil))
	assert.Empty(t, in.consecutive[topicPartition{"foo", "0"}])
	assert.Equal(t, 1, in.consecutive[topicPartition{"foo", "1"}])

	// Every acknowledgement reached the child unchanged.
	child.mu.Lock()
	defer child.mu.Unlock()
	assert.Equal(t, []error{rejected, rejected, rejected, rejected, rejected, nil}, child.acks)

	assert.Equal(t, int64(8), metrics.counter("redpanda_nacks{foo,0}"))
	assert.Equal(t, int64(1), metrics.counter("redpanda_nacks{foo,1}"))
}

func TestNackWarningInputDrop(t *testing.T) {
	res, logs, metrics := newNackTestResources(t)

	child := &nackTestInput{batches: []service.MessageBatch{kafkaBatch("bar", 3, 7, 3)}}
	in := newNackWarningInput(res, NackMetricNameKafka, false, testNackWarnInterval, child)

	batch, aFn, err := in.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 3)
	require.NoError(t, aFn(t.Context(), errors.New("nope")))

	warnings := logs.warnings()
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "Output rejected 3 message(s) of topic bar partition 3 starting at offset 7")
	assert.Contains(t, warnings[0], "auto_replay_nacks is false, so rejected messages are dropped")
	assert.NotContains(t, warnings[0], "consecutive")
	assert.Equal(t, int64(3), metrics.counter("kafka_nacks{bar,3}"))
}

func TestNackWarningInputPassesErrors(t *testing.T) {
	res, logs, _ := newNackTestResources(t)

	in := newNackWarningInput(res, NackMetricNameKafka, true, testNackWarnInterval, &nackTestInput{})
	_, _, err := in.ReadBatch(t.Context())
	require.ErrorIs(t, err, service.ErrEndOfInput)
	assert.Empty(t, logs.warnings())
}

func TestNackWarningInputPartialBatchError(t *testing.T) {
	res, logs, metrics := newNackTestResources(t)

	child := &nackTestInput{batches: []service.MessageBatch{
		kafkaBatch("foo", 2, 100, 5),
		kafkaBatch("foo", 2, 100, 5),
	}}
	in := newNackWarningInput(res, NackMetricNameRedpanda, true, 0, child)

	// The output fails two messages of a reordered copy of the batch, as a
	// broker or processor might hand it over, so the batch error indexes do
	// not match the source batch.
	batch, aFn, err := in.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 5)
	sent := service.MessageBatch{batch[4], batch[3], batch[2], batch[1], batch[0]}
	bErr := service.NewBatchError(sent, errors.New("partial")).
		Failed(1, errors.New("bad 103")).
		Failed(2, errors.New("bad 102"))
	require.NoError(t, aFn(t.Context(), bErr))

	warnings := logs.warnings()
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "Output rejected 2 message(s) of topic foo partition 2 starting at offset 102")
	assert.Contains(t, warnings[0], "2 message(s) rejected since the last warning")
	assert.Equal(t, int64(2), metrics.counter("redpanda_nacks{foo,2}"))

	// A batch error without per message errors rejects the whole batch.
	batch, aFn, err = in.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, aFn(t.Context(), service.NewBatchError(batch, errors.New("all"))))

	warnings = logs.warnings()
	require.Len(t, warnings, 2)
	assert.Contains(t, warnings[1], "Output rejected 5 message(s) of topic foo partition 2 starting at offset 100")
	assert.Equal(t, int64(7), metrics.counter("redpanda_nacks{foo,2}"))

	// The child receives the batch error unchanged.
	child.mu.Lock()
	defer child.mu.Unlock()
	require.Len(t, child.acks, 2)
	assert.Same(t, bErr, child.acks[0])
}

func TestWrapBatchInputNackWarningsInterval(t *testing.T) {
	spec := service.NewConfigSpec().Fields(
		service.NewAutoRetryNacksToggleField(),
		NewNackWarningIntervalField(),
	)

	tests := []struct {
		name string
		yaml string
		want time.Duration
	}{
		{name: "default", yaml: "", want: 10 * time.Second},
		{name: "custom", yaml: "nack_warning_interval: 1m", want: time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(tc.yaml, nil)
			require.NoError(t, err)

			res, _, _ := newNackTestResources(t)
			rdr, err := WrapBatchInputNackWarnings(conf, res, NackMetricNameRedpanda, &nackTestInput{})
			require.NoError(t, err)
			in, ok := rdr.(*nackWarningInput)
			require.True(t, ok)
			assert.Equal(t, tc.want, in.warnInterval)
			assert.True(t, in.replay)
		})
	}
}
