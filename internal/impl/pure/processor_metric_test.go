package pure_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestMetricBad(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "bad type"
	conf.Metric.Name = "some.path"
	_, err := mock.NewManager().NewProcessor(conf)
	require.Error(t, err)

	conf = processor.NewConfig()
	conf.Type = "metric"
	_, err = mock.NewManager().NewProcessor(conf)
	require.Error(t, err)
}

func TestMetricCounter(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	mgr := mock.NewManager()
	mgr.M = mockMetrics

	proc, err := mgr.NewProcessor(conf)
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
		"foo.bar": 4,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessBatch(context.Background(), message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricCounterBy(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	mgr := mock.NewManager()
	mgr.M = mockMetrics

	proc, err := mgr.NewProcessor(conf)
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
		msg, res := proc.ProcessBatch(context.Background(), message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricGauge(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	mgr := mock.NewManager()
	mgr.M = mockMetrics

	proc, err := mgr.NewProcessor(conf)
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
		msg, res := proc.ProcessBatch(context.Background(), message.QuickBatch(i))
		assert.Len(t, msg, 1)
		assert.Nil(t, res)
	}

	assert.Equal(t, expMetrics, mockMetrics.FlushCounters())
}

func TestMetricTiming(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Name = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	mockMetrics := metrics.NewLocal()

	mgr := mock.NewManager()
	mgr.M = mockMetrics

	proc, err := mgr.NewProcessor(conf)
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
		msg, res := proc.ProcessBatch(context.Background(), message.QuickBatch(i))
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
