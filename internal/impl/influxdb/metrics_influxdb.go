package influxdb

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/rcrowley/go-metrics"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	imetrics "github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	_ = bundle.AllMetrics.Add(newInfluxDB, docs.ComponentSpec{
		Name:        "influxdb",
		Type:        docs.TypeMetrics,
		Status:      docs.StatusBeta,
		Version:     "3.36.0",
		Summary:     `Send metrics to InfluxDB 1.x using the ` + "`/write`" + ` endpoint.`,
		Description: `See https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint for further details on the write API.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "A URL of the format `[https|http|udp]://host:port` to the InfluxDB host.").HasDefault(""),
			docs.FieldString("db", "The name of the database to use.").HasDefault(""),
			btls.FieldSpec(),
			docs.FieldString("username", "A username (when applicable).").Advanced().HasDefault(""),
			docs.FieldString("password", "A password (when applicable).").Advanced().HasDefault("").Secret(),
			docs.FieldObject("include", "Optional additional metrics to collect, enabling these metrics may have some performance implications as it acquires a global semaphore and does `stoptheworld()`.").WithChildren(
				docs.FieldString("runtime", "A duration string indicating how often to poll and collect runtime metrics. Leave empty to disable this metric", "1m").HasDefault(""),
				docs.FieldString("debug_gc", "A duration string indicating how often to poll and collect GC metrics. Leave empty to disable this metric.", "1m").HasDefault(""),
			).Advanced(),
			docs.FieldString("interval", "A duration string indicating how often metrics should be flushed.").Advanced().HasDefault("1m"),
			docs.FieldString("ping_interval", "A duration string indicating how often to ping the host.").Advanced().HasDefault("20s"),
			docs.FieldString("precision", "[ns|us|ms|s] timestamp precision passed to write api.").Advanced().HasDefault("s"),
			docs.FieldString("timeout", "How long to wait for response for both ping and writing metrics.").Advanced().HasDefault("5s"),
			docs.FieldString("tags", "Global tags added to each metric.",
				map[string]string{
					"hostname": "localhost",
					"zone":     "danger",
				},
			).Map().Advanced().HasDefault(map[string]any{}),
			docs.FieldString("retention_policy", "Sets the retention policy for each write.").Advanced().HasDefault(""),
			docs.FieldString("write_consistency", "[any|one|quorum|all] sets write consistency when available.").Advanced().HasDefault(""),
		),
	})
}

type influxDBMetrics struct {
	client      client.Client
	batchConfig client.BatchPointsConfig

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	registry        metrics.Registry
	runtimeRegistry metrics.Registry
	config          imetrics.InfluxDBConfig
	mgr             bundle.NewManagement
	log             log.Modular
}

func newInfluxDB(config imetrics.Config, nm bundle.NewManagement) (imetrics.Type, error) {
	i := &influxDBMetrics{
		config:          config.InfluxDB,
		registry:        metrics.NewRegistry(),
		runtimeRegistry: metrics.NewRegistry(),
		mgr:             nm,
		log:             nm.Logger(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	if config.InfluxDB.Include.Runtime != "" {
		metrics.RegisterRuntimeMemStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.Runtime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureRuntimeMemStats(i.runtimeRegistry, interval)
	}

	if config.InfluxDB.Include.DebugGC != "" {
		metrics.RegisterDebugGCStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.DebugGC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureDebugGCStats(i.runtimeRegistry, interval)
	}

	var err error
	if i.interval, err = time.ParseDuration(config.InfluxDB.Interval); err != nil {
		return nil, fmt.Errorf("failed to parse interval: %s", err)
	}

	if i.pingInterval, err = time.ParseDuration(config.InfluxDB.PingInterval); err != nil {
		return nil, fmt.Errorf("failed to parse ping interval: %s", err)
	}

	if i.timeout, err = time.ParseDuration(config.InfluxDB.Timeout); err != nil {
		return nil, fmt.Errorf("failed to parse timeout interval: %s", err)
	}

	if err := i.makeClient(); err != nil {
		return nil, err
	}

	i.batchConfig = client.BatchPointsConfig{
		Precision:        config.InfluxDB.Precision,
		Database:         config.InfluxDB.DB,
		RetentionPolicy:  config.InfluxDB.RetentionPolicy,
		WriteConsistency: config.InfluxDB.WriteConsistency,
	}

	go i.loop()

	return i, nil
}

func (i *influxDBMetrics) makeClient() error {
	var c client.Client
	u, err := url.Parse(i.config.URL)
	if err != nil {
		return fmt.Errorf("problem parsing url: %s", err)
	}

	if u.Scheme == "https" {
		tlsConfig := &tls.Config{}
		if i.config.TLS.Enabled {
			tlsConfig, err = i.config.TLS.Get(i.mgr.FS())
			if err != nil {
				return err
			}
		}
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:      u.String(),
			TLSConfig: tlsConfig,
			Username:  i.config.Username,
			Password:  i.config.Password,
		})
	} else if u.Scheme == "http" {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     u.String(),
			Username: i.config.Username,
			Password: i.config.Password,
		})
	} else if u.Scheme == "udp" {
		c, err = client.NewUDPClient(client.UDPConfig{
			Addr: u.Host,
		})
	} else {
		return fmt.Errorf("protocol needs to be http, https or udp and is %s", u.Scheme)
	}

	if err == nil {
		i.client = c
	}
	return err
}

func (i *influxDBMetrics) loop() {
	ticker := time.NewTicker(i.interval)
	pingTicker := time.NewTicker(i.pingInterval)
	defer ticker.Stop()
	defer pingTicker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			if err := i.publishRegistry(); err != nil {
				i.log.Errorf("failed to send metrics data: %s", err)
			}
		case <-pingTicker.C:
			_, _, err := i.client.Ping(i.timeout)
			if err != nil {
				i.log.Warnf("unable to ping influx endpoint: %s", err)
				if err = i.makeClient(); err != nil {
					i.log.Errorf("unable to recreate client: %s", err)
				}
			}
		}
	}
}

func (i *influxDBMetrics) publishRegistry() error {
	points, err := client.NewBatchPoints(i.batchConfig)
	if err != nil {
		return fmt.Errorf("problem creating batch points for influx: %s", err)
	}
	now := time.Now()
	all := i.getAllMetrics()
	for k, v := range all {
		name, normalTags := decodeInfluxDBName(k)
		tags := make(map[string]string, len(i.config.Tags)+len(normalTags))
		// apply normal tags
		for k, v := range normalTags {
			tags[k] = v
		}
		// override with any global
		for k, v := range i.config.Tags {
			tags[k] = v
		}
		p, err := client.NewPoint(name, tags, v, now)
		if err != nil {
			i.log.Debugf("problem formatting metrics on %s: %s", name, err)
		} else {
			points.AddPoint(p)
		}
	}

	return i.client.Write(points)
}

func getMetricValues(i any) map[string]any {
	var values map[string]any
	switch metric := i.(type) {
	case metrics.Counter:
		values = make(map[string]any, 1)
		values["count"] = metric.Count()
	case metrics.Gauge:
		values = make(map[string]any, 1)
		values["value"] = metric.Value()
	case metrics.GaugeFloat64:
		values = make(map[string]any, 1)
		values["value"] = metric.Value()
	case metrics.Timer:
		values = make(map[string]any, 14)
		t := metric.Snapshot()
		ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
		values["count"] = t.Count()
		values["min"] = t.Min()
		values["max"] = t.Max()
		values["mean"] = t.Mean()
		values["stddev"] = t.StdDev()
		values["p50"] = ps[0]
		values["p75"] = ps[1]
		values["p95"] = ps[2]
		values["p99"] = ps[3]
		values["p999"] = ps[4]
		values["1m.rate"] = t.Rate1()
		values["5m.rate"] = t.Rate5()
		values["15m.rate"] = t.Rate15()
		values["mean.rate"] = t.RateMean()
	case metrics.Histogram:
		values = make(map[string]any, 10)
		t := metric.Snapshot()
		ps := t.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
		values["count"] = t.Count()
		values["min"] = t.Min()
		values["max"] = t.Max()
		values["mean"] = t.Mean()
		values["stddev"] = t.StdDev()
		values["p50"] = ps[0]
		values["p75"] = ps[1]
		values["p95"] = ps[2]
		values["p99"] = ps[3]
		values["p999"] = ps[4]
	}
	return values
}

func (i *influxDBMetrics) getAllMetrics() map[string]map[string]any {
	data := make(map[string]map[string]any)
	i.registry.Each(func(name string, metric any) {
		values := getMetricValues(metric)
		data[name] = values
	})
	i.runtimeRegistry.Each(func(name string, metric any) {
		values := getMetricValues(metric)
		data[name] = values
	})
	return data
}

func (i *influxDBMetrics) GetCounter(path string) imetrics.StatCounter {
	encodedName := encodeInfluxDBName(path, nil, nil)
	return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
		return influxDBCounter{
			metrics.NewCounter(),
		}
	}).(influxDBCounter)
}

func (i *influxDBMetrics) GetCounterVec(path string, n ...string) imetrics.StatCounterVec {
	return imetrics.FakeCounterVec(func(l ...string) imetrics.StatCounter {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
			return influxDBCounter{
				metrics.NewCounter(),
			}
		}).(influxDBCounter)
	})
}

func (i *influxDBMetrics) GetTimer(path string) imetrics.StatTimer {
	encodedName := encodeInfluxDBName(path, nil, nil)
	return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
		return influxDBTimer{
			metrics.NewTimer(),
		}
	}).(influxDBTimer)
}

func (i *influxDBMetrics) GetTimerVec(path string, n ...string) imetrics.StatTimerVec {
	return imetrics.FakeTimerVec(func(l ...string) imetrics.StatTimer {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
			return influxDBTimer{
				metrics.NewTimer(),
			}
		}).(influxDBTimer)
	})
}

func (i *influxDBMetrics) GetGauge(path string) imetrics.StatGauge {
	encodedName := encodeInfluxDBName(path, nil, nil)
	result := i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
		return influxDBGauge{
			metrics.NewGauge(),
		}
	}).(influxDBGauge)
	return result
}

func (i *influxDBMetrics) GetGaugeVec(path string, n ...string) imetrics.StatGaugeVec {
	return imetrics.FakeGaugeVec(func(l ...string) imetrics.StatGauge {
		encodedName := encodeInfluxDBName(path, n, l)
		return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
			return influxDBGauge{
				metrics.NewGauge(),
			}
		}).(influxDBGauge)
	})
}

func (i *influxDBMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (i *influxDBMetrics) Close() error {
	if err := i.publishRegistry(); err != nil {
		i.log.Errorf("failed to send metrics data: %s", err)
	}
	i.client.Close()
	return nil
}
