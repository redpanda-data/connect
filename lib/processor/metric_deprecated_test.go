package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestMetricDeprecatedBad(t *testing.T) {
	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "bad type"
	conf.Metric.Path = "some.path"
	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad type")
	}

	conf = NewConfig()
	conf.Type = "metric"
	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from empty path")
	}
}

func TestMetricDeprecatedCounter(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		"foo.bar": 7,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedCounterParts(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_parts"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedCounterBy(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{}`),
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
		"foo.bar": 5,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedGauge(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		},
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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedTiming(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		},
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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedCounterLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size()}",
	}

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		"foo.bar": 7,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedCounterPartsLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_parts"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size()}",
	}

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedCounterByLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size()}",
	}

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

	inputs := [][][]byte{
		{},
		{},
		{
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{}`),
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
		"foo.bar": 5,
	}

	for _, i := range inputs {
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedGaugeLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size()}",
	}

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		},
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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

func TestMetricDeprecatedTimingLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json(\"foo.bar\")}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size()}",
	}

	proc, err := New(conf, nil, log.Noop(), metrics.WrapFlat(mockStats))
	if err != nil {
		t.Fatal(err)
	}

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
		},
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
		msg, res := proc.ProcessMessage(message.New(i))
		if exp, act := 1, len(msg); exp != act {
			t.Errorf("Wrong count of resulting messages: %v != %v", act, exp)
		}
		if res != nil {
			t.Error(res.Error())
		}
	}

	if !reflect.DeepEqual(expMetrics, mockStats.values) {
		t.Errorf("Wrong result: %v != %v", mockStats.values, expMetrics)
	}
}

//------------------------------------------------------------------------------
