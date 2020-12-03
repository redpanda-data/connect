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
	Constructors[TypeInflux] = TypeSpec{
		constructor: NewInflux,
		Summary: `
Send metrics to InfluxDB 1.x using the /write endpoint.`,
		Description: `description goes here`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("url", "(http|udp)://host:port combination required to specify influx host."),
			docs.FieldCommon("db", "name of the influx database to use."),
			docs.FieldAdvanced("username", "influx username."),
			docs.FieldAdvanced("password", "influx password."),
			docs.FieldAdvanced("prefix", "prefix all measurement names."),
			docs.FieldAdvanced("interval", "how often to send metrics to influx."),
			docs.FieldAdvanced("ping_interval", "how often to poll health of influx."),
			docs.FieldAdvanced("timeout", "how long to wait for response from influx for both ping and writing metrics."),
			docs.FieldAdvanced("tags", "tags to add to each metric sent to influx."),
			pathMappingDocs(true),
		},
	}
}

// InfluxConfig is config for the influx v2 metrics type.
type InfluxConfig struct {
	URL string `json:"url" yaml:"url"`
	DB  string `json:"db" yaml:"db"`

	Interval     string `json:"interval" yaml:"interval"`
	Password     string `json:"password" yaml:"password"`
	PingInterval string `json:"ping_interval" yaml:"ping_interval"`
	Precision    string `json:"precision" yaml:"precision"`
	Timeout      string `json:"timeout" yaml:"timeout"`
	Username     string `json:"username" yaml:"username"`

	Prefix string            `json:"prefix" yaml:"prefix"`
	Tags   map[string]string `json:"tags" yaml:"tags"`
}

// NewInfluxConfig creates an InfluxConfig struct with default values.
func NewInfluxConfig() InfluxConfig {
	return InfluxConfig{
		URL: "http://localhost:8086",
		DB:  "db",

		Prefix:       "benthos.",
		Precision:    "s",
		Interval:     "1m",
		PingInterval: "20s",
		Timeout:      "5s",
	}
}

// Influx is the stats and client holder
type Influx struct {
	client      client.Client
	batchConfig client.BatchPointsConfig

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	registry metrics.Registry
	config   InfluxConfig
	log      log.Modular
}

// NewInflux creates and returns a new Influx object.
func NewInflux(config Config, opts ...func(Type)) (Type, error) {
	i := &Influx{
		config:   config.Influx,
		registry: metrics.NewRegistry(),
		log:      log.Noop(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(i)
	}

	var err error
	if i.interval, err = time.ParseDuration(config.Influx.Interval); err != nil {
		return nil, fmt.Errorf("failed to parse interval: %v", err)
	}

	if i.pingInterval, err = time.ParseDuration(config.Influx.PingInterval); err != nil {
		return nil, fmt.Errorf("failed to parse ping interval: %v", err)
	}

	if i.timeout, err = time.ParseDuration(config.Influx.Timeout); err != nil {
		return nil, fmt.Errorf("failed to parse timeout interval: %v", err)
	}

	if err = i.makeClient(); err != nil {
		return nil, err
	}

	i.batchConfig = client.BatchPointsConfig{
		Precision: config.Influx.Precision,
		Database:  config.Influx.DB,
	}

	go i.loop()

	return i, nil
}

func (i *Influx) GetCounter(path string) StatCounter {
	if len(path) == 0 {
		return DudStat{}
	}
	return i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Counter {
		return NewCounter()
	}).(InfluxCounter)
}

func (i *Influx) GetCounterVec(path string, n []string) StatCounterVec {
	if len(path) == 0 {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	return &fCounterVec{
		f: func(l []string) StatCounter {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			//{"a":"something"}f("encodedName: %s\n", encodedName)
			return i.registry.GetOrRegister(encodedName, func() metrics.Counter {
				return NewCounter()
			}).(InfluxCounter)
		},
	}
}

func (i *Influx) GetTimer(path string) StatTimer {
	if len(path) == 0 {
		return DudStat{}
	}
	return i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Timer {
		return NewTimer()
	}).(InfluxTimer)
}

func (i *Influx) GetTimerVec(path string, n []string) StatTimerVec {
	if len(path) == 0 {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	return &fTimerVec{
		f: func(l []string) StatTimer {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			return i.registry.GetOrRegister(encodedName, func() metrics.Timer {
				return NewTimer()
			}).(InfluxTimer)
		},
	}
}

func (i *Influx) GetGauge(path string) StatGauge {
	var result = i.registry.GetOrRegister(i.config.Prefix+path, func() metrics.Gauge {
		return NewGauge()
	}).(InfluxGauge)
	return result
}

func (i *Influx) GetGaugeVec(path string, n []string) StatGaugeVec {
	if len(path) == 0 {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	return &fGaugeVec{
		f: func(l []string) StatGauge {
			encodedName := encodeInfluxName(i.config.Prefix+path, n, l)
			//fmt.Printf("encodedName: %s\n", encodedName)
			return i.registry.GetOrRegister(encodedName, func() metrics.Gauge {
				return NewGauge()
			}).(InfluxGauge)
		},
	}
}

func (i *Influx) makeClient() error {
	var c client.Client
	var err error

	u, err := url.Parse(i.config.URL)
	if err != nil {
		return fmt.Errorf("problem parsing url: %s", err)
	}
	if u.Scheme == "http" {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     u.Host,
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

func (i *Influx) loop() {
	ticker := time.NewTicker(i.interval)
	pingTicker := time.NewTicker(i.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-i.ctx.Done():
			return
		case <-ticker.C:
			if err := i.publishRegistry(); err != nil {
				i.log.Errorf("failed to send metrics data: %v", err)
			}
		case <-pingTicker.C:
			_, _, err := i.client.Ping(i.timeout)
			if err != nil {
				i.log.Warnf("unable to ping influx endpoint: %v", err)
				if err = i.makeClient(); err != nil {
					i.log.Errorf("unable to recreate client: %v", err)
				}
			}
		}
	}
}

func (i *Influx) publishRegistry() error {
	points, err := client.NewBatchPoints(i.batchConfig)
	if err != nil {
		return fmt.Errorf("problem creating batch points for influx: %s", err)
	}
	now := time.Now()
	all := i.getAllMetrics()
	for k, v := range all {
		name, normalTags := decodeInfluxName(k)
		//fmt.Printf("k: %s, name: %s, tags: %v\n", k, name, normalTags)
		tags := make(map[string]string, len(i.config.Tags)+len(normalTags))
		// add global tags
		for k, v := range i.config.Tags {
			tags[k] = v
		}
		// override any actual tags
		for k, v := range normalTags {
			tags[k] = v
		}
		p, err := client.NewPoint(name, tags, v, now)
		if err != nil {
			i.log.Errorf("problem formatting metrics: %s", err)
		}
		points.AddPoint(p)
	}
	sortedPoints := make([]string, len(points.Points()))
	for k, v := range points.Points() {
		sortedPoints[k] = v.String()
	}
	sort.Strings(sortedPoints)
	for _, v := range sortedPoints {
		fmt.Printf("%s\n", v)
	}
	return nil
}

func (i *Influx) getAllMetrics() map[string]map[string]interface{} {
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
			ps := t.Percentiles([]float64{0.5, 0.95, 0.99, 0.999})
			values["count"] = t.Count()
			values["min"] = t.Min()
			values["max"] = t.Max()
			values["mean"] = t.Mean()
			values["stddev"] = t.StdDev()
			values["p50"] = ps[0]
			values["p95"] = ps[1]
			values["p99"] = ps[2]
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

func (i *Influx) SetLogger(log log.Modular) {
	i.log = log
}

func (i *Influx) Close() error {
	i.client.Close()
	return nil
}
