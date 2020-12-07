package metrics

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/rcrowley/go-metrics"
)

func init() {
	Constructors[TypeInfluxDB] = TypeSpec{
		constructor: NewInfluxDB,
		Summary: `
Send metrics to InfluxDB 1.x using the /write endpoint.`,
		Description: `description goes here`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "[http|udp]://host:port combination required to specify influx host."),
			docs.FieldCommon("db", "name of the influx database to use."),
			docs.FieldAdvanced("username", "influx username."),
			docs.FieldAdvanced("password", "influx password."),
			docs.FieldAdvanced("include", `collecting these metrics may have some performance implications as it acquires a global semaphore and does stoptheworld().`).WithChildren(
				docs.FieldCommon("runtime", "how often to poll and collect runtime metrics at the duration set.", "1m").HasDefault(""),
				docs.FieldCommon("debug_gc", "how often to poll and collect gc metrics at the duration set.", "1m").HasDefault(""),
			),
			docs.FieldAdvanced("interval", "how often to send metrics to influx."),
			docs.FieldAdvanced("ping_interval", "how often to poll health of influx."),
			docs.FieldAdvanced("precision", "[ns|us|ms|s] timestamp precision passed to influx."),
			docs.FieldAdvanced("timeout", "how long to wait for response from influx for both ping and writing metrics."),
			docs.FieldAdvanced("tags", "tags to add to each metric sent to influx."),
			docs.FieldAdvanced("retention_policy", "sets the retention policy for each write."),
			docs.FieldAdvanced("write_consistency", "[any|one|quorum|all] sets write consistency when available."),
			pathMappingDocs(true),
		},
	}
}

// InfluxDBConfig is config for the influx metrics type.
type InfluxDBConfig struct {
	URL string `json:"url" yaml:"url"`
	DB  string `json:"db" yaml:"db"`

	Interval         string          `json:"interval" yaml:"interval"`
	Password         string          `json:"password" yaml:"password"`
	PingInterval     string          `json:"ping_interval" yaml:"ping_interval"`
	Precision        string          `json:"precision" yaml:"precision"`
	Timeout          string          `json:"timeout" yaml:"timeout"`
	Username         string          `json:"username" yaml:"username"`
	RetentionPolicy  string          `json:"retention_policy" yaml:"retention_policy"`
	WriteConsistency string          `json:"write_consistency" yaml:"write_consistency"`
	Include          IncludeInfluxDB `json:"include" yaml:"include"`

	PathMapping string            `json:"path_mapping" yaml:"path_mapping"`
	Tags        map[string]string `json:"tags" yaml:"tags"`
}

type IncludeInfluxDB struct {
	Runtime string `json:"runtime" yaml:"runtime"`
	DebugGC string `json:"debug_gc" yaml:"debug_gc"`
}

// NewInfluxDBConfig creates an InfluxDBConfig struct with default values.
func NewInfluxDBConfig() InfluxDBConfig {
	return InfluxDBConfig{
		URL: "http://localhost:8086",
		DB:  "db0",

		Precision:    "s",
		Interval:     "1m",
		PingInterval: "20s",
		Timeout:      "5s",
	}
}

// InfluxDB is the stats and client holder
type InfluxDB struct {
	client      client.Client
	batchConfig client.BatchPointsConfig

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	pathMapping *pathMapping
	registry    metrics.Registry
	config      InfluxDBConfig
	log         log.Modular
}

// NewInfluxDB creates and returns a new InfluxDB object.
func NewInfluxDB(config Config, opts ...func(Type)) (Type, error) {
	i := &InfluxDB{
		config:   config.InfluxDB,
		registry: metrics.NewRegistry(),
		log:      log.Noop(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(i)
	}

	if config.InfluxDB.Include.Runtime != "" {
		metrics.RegisterRuntimeMemStats(i.registry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.Runtime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureRuntimeMemStats(i.registry, interval)
	}

	if config.InfluxDB.Include.DebugGC != "" {
		metrics.RegisterDebugGCStats(i.registry)
		interval, err := time.ParseDuration(config.InfluxDB.Include.DebugGC)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureDebugGCStats(i.registry, interval)
	}

	var err error
	if i.pathMapping, err = newPathMapping(config.InfluxDB.PathMapping, i.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

	if i.interval, err = time.ParseDuration(config.InfluxDB.Interval); err != nil {
		return nil, fmt.Errorf("failed to parse interval: %s", err)
	}

	if i.pingInterval, err = time.ParseDuration(config.InfluxDB.PingInterval); err != nil {
		return nil, fmt.Errorf("failed to parse ping interval: %s", err)
	}

	if i.timeout, err = time.ParseDuration(config.InfluxDB.Timeout); err != nil {
		return nil, fmt.Errorf("failed to parse timeout interval: %s", err)
	}

	if err = i.makeClient(); err != nil {
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

func (i *InfluxDB) toCMName(dotSepName string) (string, []string, []string) {
	return i.pathMapping.mapPathWithTags(dotSepName)
}

func (i *InfluxDB) makeClient() error {
	var c client.Client
	u, err := url.Parse(i.config.URL)
	if err != nil {
		return fmt.Errorf("problem parsing url: %s", err)
	}
	if u.Scheme == "http" {
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
		return fmt.Errorf("protocol needs to be http or udp and is %s", u.Scheme)
	}

	if err == nil {
		i.client = c
	}
	return err
}

func (i *InfluxDB) loop() {
	ticker := time.NewTicker(i.interval)
	pingTicker := time.NewTicker(i.pingInterval)
	defer ticker.Stop()
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

func (i *InfluxDB) publishRegistry() error {
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

func (i *InfluxDB) getAllMetrics() map[string]map[string]interface{} {
	data := make(map[string]map[string]interface{})
	i.registry.Each(func(name string, i interface{}) {
		values := make(map[string]interface{})
		switch metric := i.(type) {
		case metrics.Counter:
			values["count"] = metric.Count()
		case metrics.Gauge:
			values["value"] = metric.Value()
		case metrics.GaugeFloat64:
			values["value"] = metric.Value()
		case metrics.Timer:
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
			values["p999"] = ps[3]
			values["1m.rate"] = t.Rate1()
			values["5m.rate"] = t.Rate5()
			values["15m.rate"] = t.Rate15()
			values["mean.rate"] = t.RateMean()
		}
		data[name] = values
	})
	return data
}

// GetCounter returns a stat counter object for a path.
func (i *InfluxDB) GetCounter(path string) StatCounter {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxDBName(name, labels, values)
	return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
		return influxDBCounter{
			metrics.NewCounter(),
		}
	}).(influxDBCounter)
}

// GetCounterVec returns a stat counter object for a path with the labels
func (i *InfluxDB) GetCounterVec(path string, n []string) StatCounterVec {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	labels = append(labels, n...)
	return &fCounterVec{
		f: func(l []string) StatCounter {
			values = append(values, l...)
			encodedName := encodeInfluxDBName(path, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
				return influxDBCounter{
					metrics.NewCounter(),
				}
			}).(influxDBCounter)
		},
	}
}

// GetTimer returns a stat timer object for a path.
func (i *InfluxDB) GetTimer(path string) StatTimer {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxDBName(name, labels, values)
	return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
		return influxDBTimer{
			metrics.NewTimer(),
		}
	}).(influxDBTimer)
}

// GetTimerVec returns a stat timer object for a path with the labels
func (i *InfluxDB) GetTimerVec(path string, n []string) StatTimerVec {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	labels = append(labels, n...)
	return &fTimerVec{
		f: func(l []string) StatTimer {
			values = append(values, l...)
			encodedName := encodeInfluxDBName(name, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
				return influxDBTimer{
					metrics.NewTimer(),
				}
			}).(influxDBTimer)
		},
	}
}

// GetGauge returns a stat gauge object for a path.
func (i *InfluxDB) GetGauge(path string) StatGauge {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxDBName(name, labels, values)
	var result = i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
		return influxDBGauge{
			metrics.NewGauge(),
		}
	}).(influxDBGauge)
	return result
}

// GetGaugeVec returns a stat timer object for a path with the labels
func (i *InfluxDB) GetGaugeVec(path string, n []string) StatGaugeVec {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	labels = append(labels, n...)
	return &fGaugeVec{
		f: func(l []string) StatGauge {
			values = append(values, l...)
			encodedName := encodeInfluxDBName(name, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
				return influxDBGauge{
					metrics.NewGauge(),
				}
			}).(influxDBGauge)
		},
	}
}

// SetLogger sets the logger used to print connection errors.
func (i *InfluxDB) SetLogger(log log.Modular) {
	i.log = log
}

// Close reports metrics one last time and stops the InfluxDB object and closes the underlying client connection
func (i *InfluxDB) Close() error {
	if err := i.publishRegistry(); err != nil {
		i.log.Errorf("failed to send metrics data: %s", err)
	}
	i.client.Close()
	return nil
}
