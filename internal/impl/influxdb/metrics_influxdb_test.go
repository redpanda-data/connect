package influxdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func fromYAML(t testing.TB, conf string, args ...any) *influxDBMetrics {
	t.Helper()

	pConf, err := ConfigSpec().ParseYAML(fmt.Sprintf(conf, args...), nil)
	require.NoError(t, err)

	i, err := fromParsed(pConf, nil)
	require.NoError(t, err)
	return i
}

func TestInfluxTimers(t *testing.T) {
	i := fromYAML(t, `
url: http://localhost:8086
db: db0
`)

	expectedMetrics := 3
	i.NewTimerCtor("ti mer")().Timing(100)
	i.NewTimerCtor("ti mer")().Timing(200)
	i.NewTimerCtor("timer with labels", "label")("value").Timing(200)
	i.NewTimerCtor("timer with labels", "label")("value2").Timing(400)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`ti\ mer`,
		`timer\ with\ labels,label=value`,
		`timer\ with\ labels,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else if len(values) != 14 {
			t.Errorf("number of values was not expected %d", len(values))
		}
	}
}

func TestInfluxCounters(t *testing.T) {
	i := fromYAML(t, `
url: http://localhost:8086
db: db0
`)

	expectedMetrics := 3
	i.NewCounterCtor("cou nter")().Incr(1)
	i.NewCounterCtor("cou nter")().Incr(1)
	i.NewCounterCtor("counter with labels", "label")("value").Incr(2)
	i.NewCounterCtor("counter with labels", "label")("value").Incr(2)
	i.NewCounterCtor("counter with labels", "label")("value2").Incr(2)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`cou\ nter`,
		`counter\ with\ labels,label=value`,
		`counter\ with\ labels,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else if len(values) != 1 {
			t.Errorf("number of values was not expected %d", len(values))
		}
	}
}

func TestInfluxGauge(t *testing.T) {
	i := fromYAML(t, `
url: http://localhost:8086
db: db0
`)

	expectedMetrics := 3
	i.NewGaugeCtor("ga uge")().Set(10)
	i.NewGaugeCtor("ga uge")().Set(20)
	i.NewGaugeCtor("ga uge")().Set(30)
	i.NewGaugeCtor("gauge with labels", "label")("value").Set(100)
	i.NewGaugeCtor("gauge with labels", "label")("value").Set(200)
	i.NewGaugeCtor("gauge with labels", "label")("value2").Set(100)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`ga\ uge`,
		`gauge\ with\ labels,label=value`,
		`gauge\ with\ labels,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else if len(values) != 1 {
			t.Errorf("number of values was not expected %d", len(values))
		}
	}
}
