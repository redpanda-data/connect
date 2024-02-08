package otlp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func otlpMetricsFromYAML(t testing.TB, conf string, args ...any) *OtlpMetricsExporter {
	t.Helper()
	otelConf, err := ConfigSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	c, err := NewOtlpMetricsConfig(otelConf)
	require.NoError(t, err)

	om, err := NewOtlpMetricsExporter(c, nil)
	require.NoError(t, err)

	return om
}

func getTestOtlpMetrics(t *testing.T) *OtlpMetricsExporter {
	t.Helper()

	otlpExporter := otlpMetricsFromYAML(t, `
http:
  - url: localhost:4318
    secure: false
grpc:
  - url: localhost:4317
    secure: false 
histogram_buckets: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]`,
	)
	return otlpExporter
}

type intCtorExpanded interface {
	Incr(i int64)
}

type intGagExpanded interface {
	Set(i int64)
}

func TestOtelMetrics(t *testing.T) {
	nm := getTestOtlpMetrics(t)

	ctr := nm.NewCounterCtor("counterone")()
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.NewGaugeCtor("gaugeone")()
	gge.Set(12)

	tmr := nm.NewTimerCtor("timerone")()
	tmr.Timing(13)

	ctrTwo := nm.NewCounterCtor("countertwo", "label1")
	ctrTwo("value1").Incr(10)
	ctrTwo("value2").Incr(11)
	ctrTwo("value3").(intCtorExpanded).Incr(10)

	ggeTwo := nm.NewGaugeCtor("gaugetwo", "label2")
	ggeTwo("value3").Set(12)

	ggeThree := nm.NewGaugeCtor("gaugethree")()
	ggeThree.(intGagExpanded).Set(11)

	tmrTwo := nm.NewTimerCtor("timertwo", "label3", "label4")
	tmrTwo("value4", "value5").Timing(13)
}

func TestOtelHistMetrics(t *testing.T) {
	nm := getTestOtlpMetrics(t)

	applyTestMetrics(nm)

	tmr := nm.NewTimerCtor("timerone")()
	tmr.Timing(13)
	tmrTwo := nm.NewTimerCtor("timertwo", "label3", "label4")
	tmrTwo("value4", "value5").Timing(14)
}

func applyTestMetrics(nm *OtlpMetricsExporter) {
	ctr := nm.NewCounterCtor("counterone")()
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.NewGaugeCtor("gaugeone")()
	gge.Set(12)

	ctrTwo := nm.NewCounterCtor("countertwo", "label1")
	ctrTwo("value1").Incr(10)
	ctrTwo("value2").Incr(11)

	ggeTwo := nm.NewGaugeCtor("gaugetwo", "label2")
	ggeTwo("value3").Set(12)
}
