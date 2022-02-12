package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

type mockMetric struct {
	values map[string]int64
}

func (d *mockMetric) Incr(path string, count int64) error {
	prev := d.values[path]
	prev += count
	d.values[path] = prev
	return nil
}

func (d *mockMetric) Decr(path string, count int64) error {
	prev := d.values[path]
	prev -= count
	d.values[path] = prev
	return nil
}

func (d *mockMetric) Timing(path string, delta int64) error {
	d.values[path] = delta
	return nil
}

func (d *mockMetric) Gauge(path string, value int64) error {
	d.values[path] = value
	return nil
}

func (d *mockMetric) Close() error { return nil }

//------------------------------------------------------------------------------

func TestMetricBad(t *testing.T) {
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "bad type"
	conf.Metric.Name = "some.path"
	_, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.Error(t, err)

	conf = NewConfig()
	conf.Type = "metric"
	_, err = New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.Error(t, err)
}

func TestMetricCounter(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.WrapFlat(mockStats))
	require.NoError(t, err)

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{}`),
			[]byte(`{}`),
		},
		{
			[]byte(`not even json`),
		},
		{},
		{
			[]byte(`{"foo":{"bar":"hello world"}}`),
		},
		{
			[]byte(`{"foo":{"bar":{"baz":"hello world"}}}`),
		},
	}

	expMetrics := map[string]int64{
		"foo.bar": 5,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockStats.values)
}

func TestMetricCounterBy(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.WrapFlat(mockStats))
	require.NoError(t, err)

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{}`),
			[]byte(`{"foo":{"bar":3}}`),
		},
		{
			[]byte(`not even json`),
		},
		{
			[]byte(`{"foo":{"bar":-2}}`),
		},
		{
			[]byte(`{"foo":{"bar":3}}`),
		},
		{
			[]byte(`{"foo":{"bar":{"baz":"hello world"}}}`),
		},
	}

	expMetrics := map[string]int64{
		"foo.bar": 8,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockStats.values)
}

func TestMetricGauge(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.WrapFlat(mockStats))
	require.NoError(t, err)

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{"foo":{"bar":5}}`),
			[]byte(`{}`),
		},
		{
			[]byte(`not even json`),
		},
		{
			[]byte(`{"foo":{"bar":-5}}`),
			[]byte(`{"foo":{"bar":7}}`),
		},
		{
			[]byte(`{"foo":{"bar":"hello world"}}`),
		},
		{
			[]byte(`{"foo":{"bar":{"baz":"hello world"}}}`),
		},
	}

	expMetrics := map[string]int64{
		"foo.bar": 7,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockStats.values)
}

func TestMetricTiming(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, mock.NewManager(), log.Noop(), metrics.WrapFlat(mockStats))
	require.NoError(t, err)

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{"foo":{"bar":5}}`),
			[]byte(`{}`),
		},
		{
			[]byte(`not even json`),
		},
		{
			[]byte(`{"foo":{"bar":-5}}`),
			[]byte(`{"foo":{"bar":7}}`),
		},
		{
			[]byte(`{"foo":{"bar":"hello world"}}`),
		},
		{
			[]byte(`{"foo":{"bar":{"baz":"hello world"}}}`),
		},
	}

	expMetrics := map[string]int64{
		"foo.bar": 7,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockStats.values)
}
