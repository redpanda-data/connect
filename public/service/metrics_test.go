package service

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestMetricsNil(t *testing.T) {
	var m *Metrics

	m.NewCounter("foo").Incr(1)
	m.NewGauge("bar").Set(10)
	m.NewTimer("baz").Timing(10)
}

func TestMetricsNoLabels(t *testing.T) {
	conf := metrics.NewConfig()
	conf.Type = "prometheus"
	conf.Prometheus.UseHistogramTiming = true

	stats, err := bundle.AllMetrics.Init(conf, mock.NewManager())
	require.NoError(t, err)

	nm := newReverseAirGapMetrics(stats)

	ctr := nm.NewCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.NewGauge("gaugeone")
	gge.Set(12)

	tmr := nm.NewTimer("timerone")
	tmr.Timing(13)

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	w := httptest.NewRecorder()
	stats.HandlerFunc()(w, req)

	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), "counterone 21")
	assert.Contains(t, string(body), "gaugeone 12")
	assert.Contains(t, string(body), "timerone_sum 1.3e-08")
}

func TestMetricsWithLabels(t *testing.T) {
	conf := metrics.NewConfig()
	conf.Type = "prometheus"
	conf.Prometheus.UseHistogramTiming = true

	stats, err := bundle.AllMetrics.Init(conf, mock.NewManager())
	require.NoError(t, err)

	nm := newReverseAirGapMetrics(stats)

	ctr := nm.NewCounter("countertwo", "label1")
	ctr.Incr(10, "value1")
	ctr.Incr(11, "value2")

	gge := nm.NewGauge("gaugetwo", "label2")
	gge.Set(12, "value3")

	tmr := nm.NewTimer("timertwo", "label3", "label4")
	tmr.Timing(13, "value4", "value5")

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	w := httptest.NewRecorder()
	stats.HandlerFunc()(w, req)

	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)

	assert.Contains(t, string(body), "countertwo{label1=\"value1\"} 10")
	assert.Contains(t, string(body), "countertwo{label1=\"value2\"} 11")
	assert.Contains(t, string(body), "gaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, string(body), "timertwo_sum{label3=\"value4\",label4=\"value5\"} 1.3e-08")
}

//------------------------------------------------------------------------------

type mockMetricsExporter struct {
	testField string
	values    map[string]int64
	lock      *sync.Mutex
}

type mockMetricsExporterType struct {
	name   string
	values map[string]int64
	lock   *sync.Mutex
}

func (m *mockMetricsExporterType) Incr(count int64) {
	m.lock.Lock()
	m.values[m.name] += count
	m.lock.Unlock()
}

func (m *mockMetricsExporterType) Timing(delta int64) {
	m.lock.Lock()
	m.values[m.name] = delta
	m.lock.Unlock()
}

func (m *mockMetricsExporterType) Set(value int64) {
	m.lock.Lock()
	m.values[m.name] = value
	m.lock.Unlock()
}

func (m *mockMetricsExporter) NewCounterCtor(name string, labelKeys ...string) MetricsExporterCounterCtor {
	return func(labelValues ...string) MetricsExporterCounter {
		return &mockMetricsExporterType{
			name:   fmt.Sprintf("counter:%v:%v:%v", name, labelKeys, labelValues),
			values: m.values,
			lock:   m.lock,
		}
	}
}

func (m *mockMetricsExporter) NewTimerCtor(name string, labelKeys ...string) MetricsExporterTimerCtor {
	return func(labelValues ...string) MetricsExporterTimer {
		return &mockMetricsExporterType{
			name:   fmt.Sprintf("timer:%v:%v:%v", name, labelKeys, labelValues),
			values: m.values,
			lock:   m.lock,
		}
	}
}

func (m *mockMetricsExporter) NewGaugeCtor(name string, labelKeys ...string) MetricsExporterGaugeCtor {
	return func(labelValues ...string) MetricsExporterGauge {
		return &mockMetricsExporterType{
			name:   fmt.Sprintf("gauge:%v:%v:%v", name, labelKeys, labelValues),
			values: m.values,
			lock:   m.lock,
		}
	}
}

func (m *mockMetricsExporter) Close(ctx context.Context) error {
	return nil
}

func TestMetricsPlugin(t *testing.T) {
	testMetrics := &mockMetricsExporter{
		values: map[string]int64{},
		lock:   &sync.Mutex{},
	}

	env := NewEnvironment()
	confSpec := NewConfigSpec().Field(NewStringField("foo"))

	require.NoError(t, env.RegisterMetricsExporter(
		"meow", confSpec,
		func(conf *ParsedConfig, log *Logger) (MetricsExporter, error) {
			testStr, err := conf.FieldString("foo")
			if err != nil {
				return nil, err
			}
			testMetrics.testField = testStr
			return testMetrics, nil
		}))

	builder := env.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(`
input:
  label: fooinput
  generate:
    count: 2
    interval: 1ns
    mapping: 'root.id = uuid_v4()'

pipeline:
  processors:
    - metric:
        name: customthing
        type: gauge
        labels:
          topic: testtopic
        value: 1234

output:
  label: foooutput
  drop: {}

metrics:
  meow:
    foo: foo value from config
`))

	strm, err := builder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	require.NoError(t, strm.Run(ctx))

	testMetrics.lock.Lock()
	assert.Equal(t, "foo value from config", testMetrics.testField)

	assert.Greater(t, testMetrics.values["timer:input_latency_ns:[label path]:[fooinput root.input]"], int64(1))
	delete(testMetrics.values, "timer:input_latency_ns:[label path]:[fooinput root.input]")

	assert.Greater(t, testMetrics.values["timer:output_latency_ns:[label path]:[foooutput root.output]"], int64(1))
	delete(testMetrics.values, "timer:output_latency_ns:[label path]:[foooutput root.output]")

	assert.Equal(t, map[string]int64{
		"counter:input_connection_up:[label path]:[fooinput root.input]":               1,
		"counter:input_received:[label path]:[fooinput root.input]":                    2,
		"counter:output_batch_sent:[label path]:[foooutput root.output]":               2,
		"counter:output_connection_up:[label path]:[foooutput root.output]":            1,
		"counter:output_sent:[label path]:[foooutput root.output]":                     2,
		"gauge:customthing:[label path topic]:[ root.pipeline.processors.0 testtopic]": 1234,
	}, testMetrics.values)
	testMetrics.lock.Unlock()
}
