package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	imetrics "github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func getTestProm(t *testing.T) (metrics.Type, http.HandlerFunc) {
	t.Helper()

	conf := metrics.NewConfig()
	conf.Type = metrics.TypePrometheus

	ns, err := bundle.AllMetrics.Init(conf, log.Noop())
	require.NoError(t, err)

	prom := ns.Child()
	return prom, prom.HandlerFunc()
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

	nm := imetrics.NewNamespaced(prom)

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
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

	nm := imetrics.NewNamespaced(prom)

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	ctrThree := nm.GetCounter("counterthree")
	ctrThree.Incr(22)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ntimerone_sum 13")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
	assert.Contains(t, body, "\ncounterthree 22")
}

func TestNamespacedPrefixStaticLabels(t *testing.T) {
	prom, handler := getTestProm(t)

	nm := imetrics.NewNamespaced(prom).WithLabels("static1", "svalue1")

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	nm2 := nm.WithLabels("static2", "svalue2")

	ctrThree := nm2.GetCounter("counterthree")
	ctrThree.Incr(22)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone{static1=\"svalue1\"} 21")
	assert.Contains(t, body, "\ngaugeone{static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\ntimerone_sum{static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\",static1=\"svalue1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\",static1=\"svalue1\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\",static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\",static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\ncounterthree{static1=\"svalue1\",static2=\"svalue2\"} 22")
}

func TestNamespacedPrefixStaticLabelsWithMappings(t *testing.T) {
	prom, handler := getTestProm(t)

	mappingFooToBar, err := imetrics.NewMapping(`root = this.replace("foo","bar")`, log.Noop())
	require.NoError(t, err)

	mappingBarToBaz, err := imetrics.NewMapping(`root = this.replace("bar","baz")`, log.Noop())
	require.NoError(t, err)

	nm := imetrics.NewNamespaced(prom).WithLabels("static1", "svalue1")
	nm = nm.WithMapping(mappingBarToBaz)
	nm = nm.WithMapping(mappingFooToBar)

	ctr := nm.GetCounter("counter")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gauge")
	gge.Set(12)

	tmr := nm.GetTimer("timer")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounter{static1=\"svalue1\"} 21")
	assert.Contains(t, body, "\ngauge{static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\ntimer_sum{static1=\"svalue1\"} 13")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\",static1=\"svalue1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\",static1=\"svalue1\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\",static1=\"svalue1\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\",static1=\"svalue1\"} 13")
}

func TestNamespacedPrefixStaticLabelsWithMappingLabels(t *testing.T) {
	prom, handler := getTestProm(t)

	mappingFooToBar, err := imetrics.NewMapping(`meta = meta().map_each(kv -> kv.value.replace("value","bar"))
meta extra1 = "extravalue1"
root = this.replace("foo","bar")`, log.Noop())
	require.NoError(t, err)

	mappingBarToBaz, err := imetrics.NewMapping(`meta = meta().map_each(kv -> kv.value.replace("bar","baz"))
meta extra2 = "extravalue2"
root = this.replace("bar","baz")`, log.Noop())
	require.NoError(t, err)

	nm := imetrics.NewNamespaced(prom).WithLabels("static1", "svalue1")
	nm = nm.WithMapping(mappingBarToBaz)
	nm = nm.WithMapping(mappingFooToBar)

	ctr := nm.GetCounter("counter")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gauge")
	gge.Set(12)

	tmr := nm.GetTimer("timer")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounter{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 21")
	assert.Contains(t, body, "\ngauge{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 12")
	assert.Contains(t, body, "\ntimer_sum{extra1=\"extravalue1\",extra2=\"extravalue2\",static1=\"sbaz1\"} 13")
	assert.Contains(t, body, "\ncountertwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label1=\"value1\",static1=\"sbaz1\"} 10")
	assert.Contains(t, body, "\ncountertwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label1=\"value2\",static1=\"sbaz1\"} 11")
	assert.Contains(t, body, "\ngaugetwo{extra1=\"extravalue1\",extra2=\"extravalue2\",label2=\"value3\",static1=\"sbaz1\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{extra1=\"extravalue1\",extra2=\"extravalue2\",label3=\"value4\",label4=\"value5\",static1=\"sbaz1\"} 13")
}
