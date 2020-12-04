package metrics

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/rcrowley/go-metrics"
)

func init() {
	Constructors[TypeInfluxV1] = TypeSpec{
		constructor: NewInfluxV1,
		Summary: `
Send metrics to InfluxDB 1.x using the /write endpoint.`,
		Description: `description goes here`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "[http|udp]://host:port combination required to specify influx host."),
			docs.FieldCommon("db", "name of the influx database to use."),
			docs.FieldAdvanced("username", "influx username."),
			docs.FieldAdvanced("password", "influx password."),
			docs.FieldAdvanced("prefix", "prefix all measurement names."),
			docs.FieldAdvanced("include_runtime", "go runtime stats will be collected and reported."),
			docs.FieldAdvanced("include_runtime_interval", "how often runtime stats are collected"),
			docs.FieldAdvanced("include_debug_gc", "go gc stats will be collected and reported."),
			docs.FieldAdvanced("include_debug_gc_interval", "how often gc stats are collected"),
			docs.FieldAdvanced("interval", "how often to send metrics to influx."),
			docs.FieldAdvanced("ping_interval", "how often to poll health of influx."),
			docs.FieldAdvanced("timeout", "how long to wait for response from influx for both ping and writing metrics."),
			docs.FieldAdvanced("tags", "tags to add to each metric sent to influx."),
			docs.FieldAdvanced("retention_policy", "sets the retention policy for each write."),
			docs.FieldAdvanced("write_consistency", "[any|one|quorum|all] sets write consistency when available."),
			pathMappingDocs(true),
		},
	}
}

// InfluxV1Config is config for the influx metrics type.
type InfluxV1Config struct {
	URL string `json:"url" yaml:"url"`
	DB  string `json:"db" yaml:"db"`

	Interval               string `json:"interval" yaml:"interval"`
	IncludeRuntime         bool   `json:"include_runtime" yaml:"include_runtime"`
	IncludeRuntimeInterval string `json:"include_runtime_interval" yaml:"include_runtime_interval"`
	IncludeDebugGC         bool   `json:"include_debug_gc" yaml:"include_debug_gc"`
	IncludeDebugGCInterval string `json:"include_debug_gc_interval" yaml:"include_debug_gc_interval"`
	Password               string `json:"password" yaml:"password"`
	PingInterval           string `json:"ping_interval" yaml:"ping_interval"`
	Precision              string `json:"precision" yaml:"precision"`
	Timeout                string `json:"timeout" yaml:"timeout"`
	Username               string `json:"username" yaml:"username"`
	RetentionPolicy        string `json:"retention_policy" yaml:"retention_policy"`
	WriteConsistency       string `json:"write_consistency" yaml:"write_consistency"`

	PathMapping string            `json:"path_mapping" yaml:"path_mapping"`
	Prefix      string            `json:"prefix" yaml:"prefix"`
	Tags        map[string]string `json:"tags" yaml:"tags"`
}

// NewInfluxV1Config creates an InfluxV1Config struct with default values.
func NewInfluxV1Config() InfluxV1Config {
	return InfluxV1Config{
		URL: "http://localhost:8086",
		DB:  "db0",

		Prefix:                 "benthos.",
		Precision:              "s",
		Interval:               "1m",
		PingInterval:           "20s",
		Timeout:                "5s",
		IncludeRuntimeInterval: "1m",
		IncludeDebugGCInterval: "1m",
	}
}

// InfluxV1 is the stats and client holder
type InfluxV1 struct {
	client      client.Client
	batchConfig client.BatchPointsConfig

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	pathMapping *pathMapping
	registry    metrics.Registry
	config      InfluxV1Config
	log         log.Modular
}

// NewInfluxV1 creates and returns a new InfluxV1 object.
func NewInfluxV1(config Config, opts ...func(Type)) (Type, error) {
	i := &InfluxV1{
		config:   config.InfluxV1,
		registry: metrics.NewRegistry(),
		log:      log.Noop(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(i)
	}

	if config.InfluxV1.IncludeRuntime {
		metrics.RegisterRuntimeMemStats(i.registry)
		interval, err := time.ParseDuration(config.InfluxV1.IncludeRuntimeInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureRuntimeMemStats(i.registry, interval)
	}

	if config.InfluxV1.IncludeDebugGC {
		metrics.RegisterDebugGCStats(i.registry)
		interval, err := time.ParseDuration(config.InfluxV1.IncludeDebugGCInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse interval: %s", err)
		}
		go metrics.CaptureDebugGCStats(i.registry, interval)
	}

	var err error
	if i.pathMapping, err = newPathMapping(config.InfluxV1.PathMapping, i.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

	if i.interval, err = time.ParseDuration(config.InfluxV1.Interval); err != nil {
		return nil, fmt.Errorf("failed to parse interval: %s", err)
	}

	if i.pingInterval, err = time.ParseDuration(config.InfluxV1.PingInterval); err != nil {
		return nil, fmt.Errorf("failed to parse ping interval: %s", err)
	}

	if i.timeout, err = time.ParseDuration(config.InfluxV1.Timeout); err != nil {
		return nil, fmt.Errorf("failed to parse timeout interval: %s", err)
	}

	if err = i.makeClient(); err != nil {
		return nil, err
	}

	i.batchConfig = client.BatchPointsConfig{
		Precision:        config.InfluxV1.Precision,
		Database:         config.InfluxV1.DB,
		RetentionPolicy:  config.InfluxV1.RetentionPolicy,
		WriteConsistency: config.InfluxV1.WriteConsistency,
	}

	go i.loop()

	return i, nil
}

func (i *InfluxV1) toCMName(dotSepName string) (string, []string, []string) {
	return i.pathMapping.mapPathWithTags(dotSepName)
}

func (i *InfluxV1) makeClient() error {
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

func (i *InfluxV1) loop() {
	ticker := time.NewTicker(i.interval)
	pingTicker := time.NewTicker(i.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			i.log.Infoln("i.publishRegistry() for last time")
			if err := i.publishRegistry(); err != nil {
				i.log.Errorf("failed to send metrics data: %s", err)
			}
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

func (i *InfluxV1) publishRegistry() error {
	points, err := client.NewBatchPoints(i.batchConfig)
	if err != nil {
		return fmt.Errorf("problem creating batch points for influx: %s", err)
	}
	now := time.Now()
	all := i.getAllMetrics()
	for k, v := range all {
		name, normalTags := decodeInfluxName(k)
		tags := make(map[string]string, len(i.config.Tags)+len(normalTags))
		// apply normal tags
		for k, v := range normalTags {
			tags[k] = v
		}
		// override with any global
		for k, v := range i.config.Tags {
			tags[k] = v
		}
		p, err := client.NewPoint(i.config.Prefix+name, tags, v, now)
		if err != nil {
			i.log.Debugf("problem formatting metrics on %s: %s", name, err)
		} else {
			points.AddPoint(p)
		}
	}

	sortedPoints := make([]string, len(points.Points()))
	for k, v := range points.Points() {
		sortedPoints[k] = v.String()
	}
	sort.Strings(sortedPoints)
	for _, v := range sortedPoints {
		fmt.Printf("%s\n", v)
	}
	return i.client.Write(points)
}

func (i *InfluxV1) getAllMetrics() map[string]map[string]interface{} {
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
func (i *InfluxV1) GetCounter(path string) StatCounter {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxName(name, labels, values)
	return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
		return InfluxCounter{
			metrics.NewCounter(),
		}
	}).(InfluxCounter)
}

// GetCounterVec returns a stat counter object for a path with the labels
func (i *InfluxV1) GetCounterVec(path string, n []string) StatCounterVec {
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
			encodedName := encodeInfluxName(path, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
				return InfluxCounter{
					metrics.NewCounter(),
				}
			}).(InfluxCounter)
		},
	}
}

// GetTimer returns a stat timer object for a path.
func (i *InfluxV1) GetTimer(path string) StatTimer {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxName(name, labels, values)
	return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
		return InfluxTimer{
			metrics.NewTimer(),
		}
	}).(InfluxTimer)
}

// GetTimerVec returns a stat timer object for a path with the labels
func (i *InfluxV1) GetTimerVec(path string, n []string) StatTimerVec {
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
			encodedName := encodeInfluxName(name, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
				return InfluxTimer{
					metrics.NewTimer(),
				}
			}).(InfluxTimer)
		},
	}
}

// GetGauge returns a stat gauge object for a path.
func (i *InfluxV1) GetGauge(path string) StatGauge {
	name, labels, values := i.toCMName(path)
	if len(name) == 0 {
		return DudStat{}
	}
	encodedName := encodeInfluxName(name, labels, values)
	var result = i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
		return InfluxGauge{
			metrics.NewGauge(),
		}
	}).(InfluxGauge)
	return result
}

// GetGaugeVec returns a stat timer object for a path with the labels
func (i *InfluxV1) GetGaugeVec(path string, n []string) StatGaugeVec {
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
			encodedName := encodeInfluxName(name, labels, values)
			return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
				return InfluxGauge{
					metrics.NewGauge(),
				}
			}).(InfluxGauge)
		},
	}
}

func (i *InfluxV1) SetLogger(log log.Modular) {
	i.log = log
}

func (i *InfluxV1) Close() error {
	i.client.Close()
	return nil
}
