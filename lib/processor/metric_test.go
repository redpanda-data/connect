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
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	proc, err := New(conf, mock.NewManager(), log.Noop(), mockMetrics)
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

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricCounterBy(t *testing.T) {
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	proc, err := New(conf, mock.NewManager(), log.Noop(), mockMetrics)
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

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricGauge(t *testing.T) {
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	proc, err := New(conf, mock.NewManager(), log.Noop(), mockMetrics)
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

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricTiming(t *testing.T) {
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	proc, err := New(conf, mock.NewManager(), log.Noop(), mockMetrics)
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

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	expTimingAvgs := map[string]float64{
		"foo.bar": 6,
	}
	actTimingAvgs := map[string]float64{}
	for k, v := range mockMetrics.FlushTimings() {
		actTimingAvgs[k] = v.Mean()
	}

	assert.Equal(t, expTimingAvgs, actTimingAvgs)
}
