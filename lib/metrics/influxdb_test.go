package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfluxInterface(t *testing.T) {
	o := &InfluxDB{}
	if Type(o) == nil {
		t.Errorf("Type does not satisfy Type interface.")
	}
}

func TestInfluxTimers(t *testing.T) {
	config := NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"
	config.InfluxDB.PathMapping = `meta buz = "first"`

	influx, err := NewInfluxDB(config)
	require.NoError(t, err)

	i := influx.(*InfluxDB)

	expectedMetrics := 3
	i.GetTimer("").Timing(100)
	i.GetTimer("ti mer").Timing(100)
	i.GetTimer("ti mer").Timing(200)
	i.GetTimerVec("", []string{"label"}).With("value").Timing(200)
	i.GetTimerVec("timer with labels", []string{"label"}).With("value").Timing(200)
	i.GetTimerVec("timer with labels", []string{"label"}).With("value2").Timing(400)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`ti\ mer,buz=first`,
		`timer\ with\ labels,buz=first,label=value`,
		`timer\ with\ labels,buz=first,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else {
			if len(values) != 14 {
				t.Errorf("number of values was not expected %d", len(values))
			}
		}
	}
}

func TestInfluxCounters(t *testing.T) {
	config := NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"
	config.InfluxDB.PathMapping = `meta buz = "first"`

	influx, err := NewInfluxDB(config)
	require.NoError(t, err)

	i := influx.(*InfluxDB)

	expectedMetrics := 3
	i.GetCounter("").Incr(1)
	i.GetCounter("cou nter").Incr(1)
	i.GetCounter("cou nter").Incr(1)
	i.GetCounterVec("", []string{"label"}).With("value").Incr(2)
	i.GetCounterVec("counter with labels", []string{"label"}).With("value").Incr(2)
	i.GetCounterVec("counter with labels", []string{"label"}).With("value").Incr(2)
	i.GetCounterVec("counter with labels", []string{"label"}).With("value2").Incr(2)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`cou\ nter,buz=first`,
		`counter\ with\ labels,buz=first,label=value`,
		`counter\ with\ labels,buz=first,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else {
			if len(values) != 1 {
				t.Errorf("number of values was not expected %d", len(values))
			}
		}
	}
}

func TestInfluxGauge(t *testing.T) {
	config := NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"
	config.InfluxDB.PathMapping = `meta buz = "first"`

	influx, err := NewInfluxDB(config)
	require.NoError(t, err)

	i := influx.(*InfluxDB)

	expectedMetrics := 3
	i.GetGauge("").Set(10)
	i.GetGauge("ga uge").Set(10)
	i.GetGauge("ga uge").Set(20)
	i.GetGauge("ga uge").Set(30)
	i.GetGaugeVec("", []string{"label"}).With("value").Set(100)
	i.GetGaugeVec("gauge with labels", []string{"label"}).With("value").Set(100)
	i.GetGaugeVec("gauge with labels", []string{"label"}).With("value").Set(200)
	i.GetGaugeVec("gauge with labels", []string{"label"}).With("value2").Set(100)

	m := i.getAllMetrics()
	if len(m) != expectedMetrics {
		t.Errorf("expected %d metrics, received %d", expectedMetrics, len(m))
	}

	measurements := []string{
		`ga\ uge,buz=first`,
		`gauge\ with\ labels,buz=first,label=value`,
		`gauge\ with\ labels,buz=first,label=value2`,
	}

	for _, measurementName := range measurements {
		if values, ok := m[measurementName]; !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			t.Errorf("expected to find %s in %v", measurementName, keys)
		} else {
			if len(values) != 1 {
				t.Errorf("number of values was not expected %d", len(values))
			}
		}
	}
}

func TestInflux_makeClientDefault(t *testing.T) {
	config := NewConfig()
	config.InfluxDB.URL = "http://localhost:8086"
	config.InfluxDB.DB = "db0"

	flux, err := NewInfluxDB(config)
	require.NoError(t, err)

	i := flux.(*InfluxDB)
	if i.client == nil {
		t.Errorf("expected a client")
	}
}

func TestInflux_makeClientUDP(t *testing.T) {
	config := NewConfig()
	config.InfluxDB.URL = "udp://localhost:8065"
	config.InfluxDB.DB = "db0"
	flux, err := NewInfluxDB(config)
	if err != nil {
		t.Errorf("unexpected error %s", err)
	}
	i := flux.(*InfluxDB)
	if i.client == nil {
		t.Errorf("expected a client")
	}
}

func TestInflux_makeClientInvalid(t *testing.T) {
	config := NewConfig()
	influxConfig := NewInfluxDBConfig()
	influxConfig.URL = "scheme://localhost:8065"
	influxConfig.DB = "db0"
	config.InfluxDB = influxConfig
	flux, err := NewInfluxDB(config)
	if err == nil {
		t.Errorf("expected error but did not receive one")
	}
	if flux != nil {
		t.Errorf("did not expect client created")
	}
}
