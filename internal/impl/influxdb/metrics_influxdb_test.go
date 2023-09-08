package influxdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestInfluxTimers(t *testing.T) {
	config := metrics.NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"

	influx, err := newInfluxDB(config, mock.NewManager())
	require.NoError(t, err)

	i := influx.(*influxDBMetrics)

	expectedMetrics := 3
	i.GetTimer("ti mer").Timing(100)
	i.GetTimer("ti mer").Timing(200)
	i.GetTimerVec("timer with labels", "label").With("value").Timing(200)
	i.GetTimerVec("timer with labels", "label").With("value2").Timing(400)

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
	config := metrics.NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"

	influx, err := newInfluxDB(config, mock.NewManager())
	require.NoError(t, err)

	i := influx.(*influxDBMetrics)

	expectedMetrics := 3
	i.GetCounter("cou nter").Incr(1)
	i.GetCounter("cou nter").Incr(1)
	i.GetCounterVec("counter with labels", "label").With("value").Incr(2)
	i.GetCounterVec("counter with labels", "label").With("value").Incr(2)
	i.GetCounterVec("counter with labels", "label").With("value2").Incr(2)

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
	config := metrics.NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"

	influx, err := newInfluxDB(config, mock.NewManager())
	require.NoError(t, err)

	i := influx.(*influxDBMetrics)

	expectedMetrics := 3
	i.GetGauge("ga uge").Set(10)
	i.GetGauge("ga uge").Set(20)
	i.GetGauge("ga uge").Set(30)
	i.GetGaugeVec("gauge with labels", "label").With("value").Set(100)
	i.GetGaugeVec("gauge with labels", "label").With("value").Set(200)
	i.GetGaugeVec("gauge with labels", "label").With("value2").Set(100)

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

func TestInflux_makeClientDefault(t *testing.T) {
	config := metrics.NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"

	flux, err := newInfluxDB(config, mock.NewManager())
	require.NoError(t, err)

	i := flux.(*influxDBMetrics)
	if i.client == nil {
		t.Errorf("expected a client")
	}
}

func TestInflux_makeClientHTTPS(t *testing.T) {
	config := metrics.NewConfig()
	config.InfluxDB.URL = "https://localhost:8086"
	config.InfluxDB.DB = "db0"

	flux, err := newInfluxDB(config, mock.NewManager())
	require.NoError(t, err)

	i := flux.(*influxDBMetrics)
	if i.client == nil {
		t.Errorf("expected a client")
	}
}

func TestInflux_makeClientUDP(t *testing.T) {
	config := metrics.NewConfig()
	config.InfluxDB.URL = "udp://localhost:8065"
	config.InfluxDB.DB = "db0"
	flux, err := newInfluxDB(config, mock.NewManager())
	if err != nil {
		t.Errorf("unexpected error %s", err)
	}
	i := flux.(*influxDBMetrics)
	if i.client == nil {
		t.Errorf("expected a client")
	}
}

func TestInflux_makeClientInvalid(t *testing.T) {
	config := metrics.NewConfig()
	influxConfig := metrics.NewInfluxDBConfig()
	influxConfig.URL = "scheme://localhost:8065"
	influxConfig.DB = "db0"
	config.InfluxDB = influxConfig
	flux, err := newInfluxDB(config, mock.NewManager())
	if err == nil {
		t.Errorf("expected error but did not receive one")
	}
	if flux != nil {
		t.Errorf("did not expect client created")
	}
}
