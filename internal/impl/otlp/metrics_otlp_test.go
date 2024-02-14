package otlp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type otelCollectorTest struct {
	url    string
	secure bool
}

type otelMetricsTestResult struct {
	httpCollectors   []otelCollectorTest
	grpcCollectors   []otelCollectorTest
	histogramBuckets []float64
	labels           map[string]string
}

type otelMetricsTest struct {
	config string
	result otelMetricsTestResult
}

var (
	testConfigs = []otelMetricsTest{
		{
			config: `
http:
  - url: localhost:4318
    secure: false
grpc:
  - url: localhost:4317
    secure: false 
histogram_buckets: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]`,
			result: otelMetricsTestResult{
				httpCollectors:   []otelCollectorTest{{url: "localhost:4318", secure: false}},
				grpcCollectors:   []otelCollectorTest{{url: "localhost:4317", secure: false}},
				histogramBuckets: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
				labels:           map[string]string{},
			},
		},
		{
			config: `
http:
  - url: localhost:4318
    secure: false
histogram_buckets: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]`,
			result: otelMetricsTestResult{
				httpCollectors:   []otelCollectorTest{{url: "localhost:4318", secure: false}},
				grpcCollectors:   nil,
				histogramBuckets: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
				labels:           map[string]string{},
			},
		},
		{
			config: `
grpc:
  - url: localhost:4317
    secure: false
histogram_buckets: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]`,
			result: otelMetricsTestResult{
				httpCollectors:   nil,
				grpcCollectors:   []otelCollectorTest{{url: "localhost:4317", secure: false}},
				histogramBuckets: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
				labels:           map[string]string{},
			},
		},
		{
			config: `
http: 
histogram_buckets: [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ]`,
			result: otelMetricsTestResult{
				httpCollectors:   nil,
				grpcCollectors:   nil,
				histogramBuckets: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
				labels:           map[string]string{},
			},
		},
	}
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

func getTestOtlpMetrics(t *testing.T, config string) *OtlpMetricsExporter {
	t.Helper()

	otlpExporter := otlpMetricsFromYAML(t, config)
	return otlpExporter
}

type intCtorExpanded interface {
	Incr(i int64)
}

type intGagExpanded interface {
	Set(i int64)
}

func TestOtelHistMetricsConfigs(t *testing.T) {
	for _, config := range testConfigs {
		result := otelMetricsTestResult{}

		t.Helper()
		otelConf, err := ConfigSpec().ParseYAML(config.config, nil)
		require.NoError(t, err)

		list, err := otelConf.FieldObjectList("http")
		if err == nil {
			for _, pc := range list {
				u, err := pc.FieldString("url")
				require.NoError(t, err)

				secure, err := pc.FieldBool("secure")
				require.NoError(t, err)

				result.httpCollectors = append(result.httpCollectors, otelCollectorTest{u, secure})
			}
		}

		list, err = otelConf.FieldObjectList("grpc")
		if err == nil {
			for _, pc := range list {
				u, err := pc.FieldString("url")
				require.NoError(t, err)

				secure, err := pc.FieldBool("secure")
				require.NoError(t, err)

				result.grpcCollectors = append(result.grpcCollectors, otelCollectorTest{u, secure})
			}
		}

		labels, err := otelConf.FieldStringMap("labels")
		require.NoError(t, err)
		result.labels = labels

		var buckets []float64
		buckets, err = otelConf.FieldFloatList("histogram_buckets")
		require.NoError(t, err)
		result.histogramBuckets = buckets
		assert.Equal(t, config.result, result)
	}
}

func TestOtelMetrics(t *testing.T) {
	for _, config := range testConfigs {
		nm := getTestOtlpMetrics(t, config.config)

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
}

func TestOtelHistMetrics(t *testing.T) {
	for _, config := range testConfigs {
		nm := getTestOtlpMetrics(t, config.config)

		applyTestMetrics(nm)

		tmr := nm.NewTimerCtor("timerone")()
		tmr.Timing(13)
		tmrTwo := nm.NewTimerCtor("timertwo", "label3", "label4")
		tmrTwo("value4", "value5").Timing(14)
	}
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
