// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
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

func TestMetricCounter(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"

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

func TestMetricCounterParts(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_parts"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"

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

func TestMetricCounterBy(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"

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

func TestMetricGauge(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"

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

func TestMetricTiming(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"

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

func TestMetricCounterLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size}",
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

func TestMetricCounterPartsLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_parts"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size}",
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

func TestMetricCounterByLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "counter_by"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size}",
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

func TestMetricGaugeLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "gauge"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size}",
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

func TestMetricTimingLabels(t *testing.T) {
	mockStats := &mockMetric{
		values: map[string]int64{},
	}

	conf := NewConfig()
	conf.Type = "metric"
	conf.Metric.Type = "timing"
	conf.Metric.Path = "foo.bar"
	conf.Metric.Value = "${!json_field:foo.bar}"
	conf.Metric.Labels = map[string]string{
		"batch_size": "${!batch_size}",
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
