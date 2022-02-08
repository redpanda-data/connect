package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestProm(t *testing.T) (metrics.Type, http.HandlerFunc) {
	t.Helper()

	conf := metrics.NewConfig()
	conf.Prometheus.Prefix = ""
	conf.Type = metrics.TypePrometheus

	prom, err := metrics.New(conf)
	require.NoError(t, err)

	wHandler, ok := prom.(metrics.WithHandlerFunc)
	require.True(t, ok)

	return prom, wHandler.HandlerFunc()
}

func getPage(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestNamespacedNothing(t *testing.T) {
	prom, handler := getTestProm(t)

	nm := NewNamespaced(prom)

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ntimerone_sum 13")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
}

func TestNamespacedPrefix(t *testing.T) {
	prom, handler := getTestProm(t)

	nm := NewNamespaced(prom).WithPrefix("foo")

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	nm2 := nm.WithPrefix("bar")

	ctrThree := nm2.GetCounter("counterthree")
	ctrThree.Incr(22)

	body := getPage(t, handler)

	assert.Contains(t, body, "\nfoo_counterone 21")
	assert.Contains(t, body, "\nfoo_gaugeone 12")
	assert.Contains(t, body, "\nfoo_timerone_sum 13")
	assert.Contains(t, body, "\nfoo_countertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\nfoo_countertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\nfoo_gaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\nfoo_timertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
	assert.Contains(t, body, "\nbar_counterthree 22")
}

func TestNamespacedPrefixStaticLabels(t *testing.T) {
	prom, handler := getTestProm(t)

	nm := NewNamespaced(prom).WithPrefix("foo").WithLabels("static1", "svalue1")

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	nm2 := nm.WithPrefix("bar").WithLabels("static2", "svalue2")

	ctrThree := nm2.GetCounter("counterthree")
	ctrThree.Incr(22)

	body := getPage(t, handler)

	assert.Contains(t, body, "\nfoo_counterone{static1=\"svalue1\"} 21")
	assert.Contains(t, body, "\nfoo_gaugeone{static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\nfoo_timerone_sum{static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\nfoo_countertwo{label1=\"value1\",static1=\"svalue1\"} 10")
	assert.Contains(t, body, "\nfoo_countertwo{label1=\"value2\",static1=\"svalue1\"} 11")
	assert.Contains(t, body, "\nfoo_gaugetwo{label2=\"value3\",static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\nfoo_timertwo_sum{label3=\"value4\",label4=\"value5\",static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\nbar_counterthree{static1=\"svalue1\",static2=\"svalue2\"} 22")
}

func TestNamespacedPrefixStaticLabelsWithMappings(t *testing.T) {
	prom, handler := getTestProm(t)

	mappingFooToBar, err := NewMapping(mock.NewManager(), `root = this.replace("foo","bar")`, log.Noop())
	require.NoError(t, err)

	mappingBarToBaz, err := NewMapping(mock.NewManager(), `root = this.replace("bar","baz")`, log.Noop())
	require.NoError(t, err)

	nm := NewNamespaced(prom).WithPrefix("foo").WithLabels("static1", "svalue1")
	nm = nm.WithMapping(mappingBarToBaz)
	nm = nm.WithMapping(mappingFooToBar)

	ctr := nm.GetCounter("counter")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gauge")
	gge.Set(12)

	tmr := nm.GetTimer("timer")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\nbaz_counter{static1=\"svalue1\"} 21")
	assert.Contains(t, body, "\nbaz_gauge{static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\nbaz_timer_sum{static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\nbaz_countertwo{label1=\"value1\",static1=\"svalue1\"} 10")
	assert.Contains(t, body, "\nbaz_countertwo{label1=\"value2\",static1=\"svalue1\"} 11")
	assert.Contains(t, body, "\nbaz_gaugetwo{label2=\"value3\",static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\nbaz_timertwo_sum{label3=\"value4\",label4=\"value5\",static1=\"svalue1\"} 13")
}

func TestNamespacedPrefixStaticLabelsWithMappingLabels(t *testing.T) {
	prom, handler := getTestProm(t)

	mappingFooToBar, err := NewMapping(mock.NewManager(), `meta = meta().map_each(kv -> kv.value.replace("value","bar"))
meta extra1 = "extravalue1"
root = this.replace("foo","bar")`, log.Noop())
	require.NoError(t, err)

	mappingBarToBaz, err := NewMapping(mock.NewManager(), `meta = meta().map_each(kv -> kv.value.replace("bar","baz"))
meta extra2 = "extravalue2"
root = this.replace("bar","baz")`, log.Noop())
	require.NoError(t, err)

	nm := NewNamespaced(prom).WithPrefix("foo").WithLabels("static1", "svalue1")
	nm = nm.WithMapping(mappingBarToBaz)
	nm = nm.WithMapping(mappingFooToBar)

	ctr := nm.GetCounter("counter")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gauge")
	gge.Set(12)

	tmr := nm.GetTimer("timer")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\nbaz_counter{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 21")
	assert.Contains(t, body, "\nbaz_gauge{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 12")
	assert.Contains(t, body, "\nbaz_timer_sum{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 13")
	assert.Contains(t, body, "\nbaz_countertwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label1=\"value1\",static1=\"sbaz1\"} 10")
	assert.Contains(t, body, "\nbaz_countertwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label1=\"value2\",static1=\"sbaz1\"} 11")
	assert.Contains(t, body, "\nbaz_gaugetwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label2=\"value3\",static1=\"sbaz1\"} 12")
	assert.Contains(t, body, "\nbaz_timertwo_sum{extra1=\"extravalue1\",extra2=\"extravalue2\",label3=\"value4\",label4=\"value5\",static1=\"sbaz1\"} 13")
}
