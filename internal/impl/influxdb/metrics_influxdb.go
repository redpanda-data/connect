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

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	imFieldURL              = "url"
	imFieldDB               = "db"
	imFieldTLS              = "tls"
	imFieldInterval         = "interval"
	imFieldPassword         = "password"
	imFieldPingInterval     = "ping_interval"
	imFieldPrecision        = "precision"
	imFieldTimeout          = "timeout"
	imFieldUsername         = "username"
	imFieldRetentionPolicy  = "retention_policy"
	imFieldWriteConsistency = "write_consistency"
	imFieldInclude          = "include"
	imFieldIncludeRuntime   = "runtime"
	imFieldIncludeDebugGC   = "debug_gc"
	imFieldTags             = "tags"
)

func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.36.0").
		Summary(`Send metrics to InfluxDB 1.x using the `+"`/write`"+` endpoint.`).
		Description(`See https://docs.influxdata.com/influxdb/v1.8/tools/api/#write-http-endpoint for further details on the write API.`).
		Fields(
			service.NewURLField(imFieldURL).
				Description("A URL of the format `[https|http|udp]://host:port` to the InfluxDB host."),
			service.NewStringField(imFieldDB).
				Description("The name of the database to use."),
			service.NewTLSToggledField(imFieldTLS), // TODO: V5 use non-toggled here
			service.NewStringField(imFieldUsername).
				Description("A username (when applicable).").
				Advanced().
				Default(""),
			service.NewStringField(imFieldPassword).
				Description("A password (when applicable).").
				Advanced().
				Secret().
				Default(""),
			service.NewObjectField(imFieldInclude,
				service.NewDurationField(imFieldIncludeRuntime).
					Description("A duration string indicating how often to poll and collect runtime metrics. Leave empty to disable this metric").
					Example("1m").
					Default(""),
				service.NewDurationField(imFieldIncludeDebugGC).
					Description("A duration string indicating how often to poll and collect GC metrics. Leave empty to disable this metric.").
					Example("1m").
					Default(""),
			).
				Description("Optional additional metrics to collect, enabling these metrics may have some performance implications as it acquires a global semaphore and does `stoptheworld()`.").
				Advanced(),
			service.NewDurationField(imFieldInterval).
				Description("A duration string indicating how often metrics should be flushed.").
				Advanced().
				Default("1m"),
			service.NewDurationField(imFieldPingInterval).
				Description("A duration string indicating how often to ping the host.").
				Advanced().
				Default("20s"),
			service.NewStringField(imFieldPrecision).
				Description("[ns|us|ms|s] timestamp precision passed to write api.").
				Advanced().
				Default("s"),
			service.NewDurationField(imFieldTimeout).
				Description("How long to wait for response for both ping and writing metrics.").
				Advanced().
				Default("5s"),
			service.NewStringMapField(imFieldTags).
				Description("Global tags added to each metric.").
				Advanced().
				Example(map[string]string{
					"hostname": "localhost",
					"zone":     "danger",
				}).
				Default(map[string]any{}),
			service.NewStringField(imFieldRetentionPolicy).
				Description("Sets the retention policy for each write.").
				Advanced().
				Optional(),
			service.NewStringField(imFieldWriteConsistency).
				Description("[any|one|quorum|all] sets write consistency when available.").
				Advanced().
				Optional(),
		)
}

func init() {
	err := service.RegisterMetricsExporter(
		"influxdb", ConfigSpec(),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return fromParsed(conf, log)
		})
	if err != nil {
		panic(err)
	}
}

type influxDBMetrics struct {
	client      client.Client
	clientConf  clientConf
	batchConfig client.BatchPointsConfig

	tags         map[string]string
	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	registry        metrics.Registry
	runtimeRegistry metrics.Registry
	log             *service.Logger
}

func fromParsed(conf *service.ParsedConfig, logger *service.Logger) (i *influxDBMetrics, err error) {
	i = &influxDBMetrics{
		registry:        metrics.NewRegistry(),
		runtimeRegistry: metrics.NewRegistry(),
		log:             logger,
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	if runTime, _ := conf.FieldString(imFieldInclude, imFieldIncludeRuntime); runTime != "" {
		metrics.RegisterRuntimeMemStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(runTime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureRuntimeMemStats(i.runtimeRegistry, interval)
	}

	if debugGC, _ := conf.FieldString(imFieldInclude, imFieldIncludeDebugGC); debugGC != "" {
		metrics.RegisterDebugGCStats(i.runtimeRegistry)
		interval, err := time.ParseDuration(debugGC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureDebugGCStats(i.runtimeRegistry, interval)
	}

	if i.interval, err = conf.FieldDuration(imFieldInterval); err != nil {
		return
	}
	if i.pingInterval, err = conf.FieldDuration(imFieldPingInterval); err != nil {
		return
	}
	if i.timeout, err = conf.FieldDuration(imFieldTimeout); err != nil {
		return
	}

	if i.clientConf, err = clientConfFromParsed(conf); err != nil {
		return nil, err
	}
	if i.client, err = i.clientConf.build(); err != nil {
		return nil, err
	}

	if i.tags, err = conf.FieldStringMap(imFieldTags); err != nil {
		return
	}

	i.batchConfig = client.BatchPointsConfig{}
	if i.batchConfig.Precision, err = conf.FieldString(imFieldPrecision); err != nil {
		return
	}
	if i.batchConfig.Database, err = conf.FieldString(imFieldDB); err != nil {
		return
	}
	i.batchConfig.RetentionPolicy, _ = conf.FieldString(imFieldRetentionPolicy)
	i.batchConfig.WriteConsistency, _ = conf.FieldString(imFieldWriteConsistency)

	go i.loop()

	return i, nil
}

type clientConf struct {
	u        *url.URL
	tlsConf  *tls.Config
	username string
	password string
}

func clientConfFromParsed(conf *service.ParsedConfig) (c clientConf, err error) {
	if c.u, err = conf.FieldURL(imFieldURL); err != nil {
		return
	}
	c.username, _ = conf.FieldString(imFieldUsername)
	c.password, _ = conf.FieldString(imFieldPassword)
	if c.tlsConf, err = conf.FieldTLS(imFieldTLS); err != nil {
		return
	}
	return
}

func (conf clientConf) build() (c client.Client, err error) {
	if conf.u.Scheme == "https" {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:      conf.u.String(),
			TLSConfig: conf.tlsConf,
			Username:  conf.username,
			Password:  conf.password,
		})
	} else if conf.u.Scheme == "http" {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     conf.u.String(),
			Username: conf.username,
			Password: conf.password,
		})
	} else if conf.u.Scheme == "udp" {
		c, err = client.NewUDPClient(client.UDPConfig{
			Addr: conf.u.Host,
		})
	} else {
		return nil, fmt.Errorf("protocol needs to be http, https or udp and is %s", conf.u.Scheme)
	}
	return c, err
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
				if tmpClient, err := i.clientConf.build(); err != nil {
					i.log.Errorf("unable to recreate client: %s", err)
				} else {
					i.client = tmpClient
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
		tags := make(map[string]string, len(i.tags)+len(normalTags))
		// apply normal tags
		for k, v := range normalTags {
			tags[k] = v
		}
		// override with any global
		for k, v := range i.tags {
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

func (i *influxDBMetrics) NewCounterCtor(path string, n ...string) service.MetricsExporterCounterCtor {
	return func(labelValues ...string) service.MetricsExporterCounter {
		encodedName := encodeInfluxDBName(path, n, labelValues)
		return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
			return influxDBCounter{
				metrics.NewCounter(),
			}
		}).(influxDBCounter)
	}
}

func (i *influxDBMetrics) NewTimerCtor(path string, n ...string) service.MetricsExporterTimerCtor {
	return func(labelValues ...string) service.MetricsExporterTimer {
		encodedName := encodeInfluxDBName(path, n, labelValues)
		return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
			return influxDBTimer{
				metrics.NewTimer(),
			}
		}).(influxDBTimer)
	}
}

func (i *influxDBMetrics) NewGaugeCtor(path string, n ...string) service.MetricsExporterGaugeCtor {
	return func(labelValues ...string) service.MetricsExporterGauge {
		encodedName := encodeInfluxDBName(path, n, labelValues)
		return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
			return influxDBGauge{
				metrics.NewGauge(),
			}
		}).(influxDBGauge)
	}
}

func (i *influxDBMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (i *influxDBMetrics) Close(context.Context) error {
	if err := i.publishRegistry(); err != nil {
		i.log.Errorf("failed to send metrics data: %s", err)
	}
	i.client.Close()
	return nil
}
