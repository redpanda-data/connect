package service

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsNil(t *testing.T) {
	var m *Metrics

	m.NewCounter("foo").Incr(1)
	m.NewGauge("bar").Set(10)
	m.NewTimer("baz").Timing(10)
}

func TestMetricsNoLabels(t *testing.T) {
	conf := metrics.NewConfig()
	conf.Type = metrics.TypePrometheus

	stats, err := bundle.AllMetrics.Init(conf, log.Noop())
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
	assert.Contains(t, string(body), "timerone_sum 13")
}

func TestMetricsWithLabels(t *testing.T) {
	conf := metrics.NewConfig()
	conf.Type = metrics.TypePrometheus

	stats, err := bundle.AllMetrics.Init(conf, log.Noop())
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
	assert.Contains(t, string(body), "timertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
}
