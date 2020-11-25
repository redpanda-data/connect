package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	client "github.com/influxdata/influxdb1-client/v2"
)

func init() {
	Constructors[TypeInflux] = TypeSpec{
		constructor: NewInflux,
		Summary: `
Send metrics to InfluxDB 1.x using the /write endpoint.`,
		Description: `description goes here`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("address", "host:port combination required to specify influx host."),
			docs.FieldCommon("db", "name of the influx database to use."),
			docs.FieldAdvanced("username", "influx username."),
			docs.FieldAdvanced("password", "influx password."),
			docs.FieldAdvanced("prefix", "prefix all measurement names."),
			docs.FieldAdvanced("interval", "how often to send metrics to influx."),
			docs.FieldAdvanced("ping_interval", "how often to poll health of influx."),
			docs.FieldAdvanced("timeout", "how long to wait for response from influx for both ping and writing metrics."),
			docs.FieldAdvanced("protocol", "http or udp influx client. defaults to http."),
			docs.FieldAdvanced("tags", "tags to add to each metric sent to influx."),
			pathMappingDocs(true),
		},
	}
}

// InfluxConfig is config for the influx v2 metrics type.
type InfluxConfig struct {
	Address string `json:"address" yaml:"address"`
	DB      string `json:"db" yaml:"db"`

	Interval     string `json:"interval" yaml:"interval"`
	Password     string `json:"password" yaml:"password"`
	PingInterval string `json:"ping_interval" yaml:"ping_interval"`
	Precision    string `json:"precision" yaml:"precision"`
	Protocol     string `json:"protocol" yaml:"protocol"`
	Timeout      string `json:"timeout" yaml:"timeout"`
	Username     string `json:"username" yaml:"username"`

	Prefix string            `json:"prefix" yaml:"prefix"`
	Tags   map[string]string `json:"tags" yaml:"tags"`
}

// NewInfluxConfig creates an InfluxConfig struct with default values.
func NewInfluxConfig() InfluxConfig {
	return InfluxConfig{
		Address: "http://localhost:8086",
		DB:      "db",

		Precision:    "s",
		Interval:     "1m",
		PingInterval: "20s",
		Timeout:      "5s",
		Protocol:     "http",
	}
}

// Influx is the stats and client holder
type Influx struct {
	client      client.Client
	batchConfig client.BatchPointsConfig

	tagKeys   []string
	tagValues []string

	interval     time.Duration
	pingInterval time.Duration
	timeout      time.Duration

	ctx    context.Context
	cancel func()

	local  *Local
	config InfluxConfig
	log    log.Modular
}

// NewInflux creates and returns a new Influx object.
func NewInflux(config Config, opts ...func(Type)) (Type, error) {
	i := &Influx{
		config: config.Influx,
		local:  NewLocal(),
		log:    log.Noop(),
	}

	i.ctx, i.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(i)
	}

	i.tagKeys = make([]string, 0, len(config.Influx.Tags))
	i.tagValues = make([]string, 0, len(config.Influx.Tags))
	for k, v := range config.Influx.Tags {
		i.tagKeys = append(i.tagKeys, k)
		i.tagValues = append(i.tagValues, v)
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
	return i.local.GetCounter(i.config.Prefix + path)
}

func (i *Influx) GetCounterVec(path string, labelNames []string) StatCounterVec {
	return i.local.GetCounterVec(i.config.Prefix+path, labelNames)
}

func (i *Influx) GetTimer(path string) StatTimer {
	return i.local.GetTimer(i.config.Prefix + path)
}

func (i *Influx) GetTimerVec(path string, labelNames []string) StatTimerVec {
	return i.local.GetTimerVec(i.config.Prefix+path, labelNames)
}

func (i *Influx) GetGauge(path string) StatGauge {
	return i.local.GetGauge(i.config.Prefix + path)
}

func (i *Influx) GetGaugeVec(path string, labelNames []string) StatGaugeVec {
	return i.local.GetGaugeVec(i.config.Prefix+path, labelNames)
}

func (i *Influx) makeClient() error {
	var c client.Client
	var err error
	if i.config.Protocol == "http" {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     i.config.Address,
			Username: i.config.Username,
			Password: i.config.Password,
		})
	} else if i.config.Protocol == "udp" {
		c, err = client.NewUDPClient(client.UDPConfig{
			Addr: i.config.Address,
		})
	} else {
		return fmt.Errorf("protocol needs to be http or udp and is %s", i.config.Protocol)
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
			if err := i.publish(); err != nil {
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

func (i *Influx) publish() error {

	points, err := client.NewBatchPoints(i.batchConfig)
	if err != nil {
		return fmt.Errorf("problem creating batch points for influx: %s", err)
	}

	counters := i.local.GetCountersWithLabels()
	timers := i.local.GetTimingsWithLabels()
	now := time.Now()

	for k, v := range counters {
		tags := make(map[string]string, v.LabelLength()+len(i.config.Tags))
		v.labelsAndValues.Range(func(key, value interface{}) bool {
			tags[fmt.Sprintf("%s", key)] = fmt.Sprintf("%s", value)
			return true
		})

		for k, v := range i.config.Tags {
			tags[k] = v
		}

		fields := map[string]interface{}{
			"count": *v.Value,
		}

		p, err := client.NewPoint(k, tags, fields, now)
		if err != nil {
			i.log.Errorf("problem with point %v", err)
		}
		points.AddPoint(p)
	}

	for k, v := range timers {
		tags := make(map[string]string, v.LabelLength()+len(i.config.Tags))
		v.labelsAndValues.Range(func(key, value interface{}) bool {
			tags[fmt.Sprintf("%s", key)] = fmt.Sprintf("%s", value)
			return true
		})

		for k, v := range i.config.Tags {
			tags[k] = v
		}

		fields := map[string]interface{}{
			"value": *v.Value,
		}
		p, err := client.NewPoint(k, tags, fields, now)
		if err != nil {
			i.log.Errorf("problem with point %v", err)
		}
		points.AddPoint(p)
	}

	return i.client.Write(points)
}

func (i *Influx) SetLogger(log log.Modular) {
	i.log = log
}

func (i *Influx) Close() error {
	i.client.Close()
	return nil
}
