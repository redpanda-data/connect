package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	statsd "github.com/smira/go-statsd"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeStatsd] = TypeSpec{
		constructor: newStatsd,
		Summary: `
Pushes metrics using the [StatsD protocol](https://github.com/statsd/statsd).
Supported tagging formats are 'none', 'datadog' and 'influxdb'.`,
		Description: `
The underlying client library has recently been updated in order to support
tagging.

The 'network' field is deprecated and scheduled for removal. If you currently
rely on sending Statsd metrics over TCP and want it to be supported long term
please [raise an issue](https://github.com/Jeffail/benthos/issues).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("address", "The address to send metrics to."),
			docs.FieldCommon("flush_period", "The time interval between metrics flushes."),
			docs.FieldCommon("tag_format", "Metrics tagging is supported in a variety of formats.").HasOptions(
				"none", "datadog", "influxdb",
			),
		},
	}
}

//------------------------------------------------------------------------------

type wrappedDatadogLogger struct {
	log log.Modular
}

func (s wrappedDatadogLogger) Printf(msg string, args ...interface{}) {
	s.log.Warnf(fmt.Sprintf(msg, args...))
}

//------------------------------------------------------------------------------

// StatsdConfig is config for the Statsd metrics type.
type StatsdConfig struct {
	Address     string `json:"address" yaml:"address"`
	FlushPeriod string `json:"flush_period" yaml:"flush_period"`
	TagFormat   string `json:"tag_format" yaml:"tag_format"`
}

// NewStatsdConfig creates an StatsdConfig struct with default values.
func NewStatsdConfig() StatsdConfig {
	return StatsdConfig{
		Address:     "localhost:4040",
		FlushPeriod: "100ms",
		TagFormat:   TagFormatNone,
	}
}

// Tag formats supported by the statsd metric type.
const (
	TagFormatNone     = "none"
	TagFormatDatadog  = "datadog"
	TagFormatInfluxDB = "influxdb"
)

//------------------------------------------------------------------------------

type statsdStat struct {
	path string
	s    *statsd.Client
	tags []statsd.Tag
}

func (s *statsdStat) Incr(count int64) {
	s.s.Incr(s.path, count, s.tags...)
}

func (s *statsdStat) Decr(count int64) {
	s.s.Decr(s.path, count, s.tags...)
}

func (s *statsdStat) Timing(delta int64) {
	s.s.Timing(s.path, delta, s.tags...)
}

func (s *statsdStat) Set(value int64) {
	s.s.Gauge(s.path, value, s.tags...)
}

//------------------------------------------------------------------------------

type statsdMetrics struct {
	config Config
	s      *statsd.Client
	log    log.Modular
}

func newStatsd(config Config, log log.Modular) (Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %s", err)
	}

	s := &statsdMetrics{
		config: config,
		log:    log,
	}

	statsdOpts := []statsd.Option{
		statsd.FlushInterval(flushPeriod),
		statsd.Logger(wrappedDatadogLogger{log: s.log}),
	}

	switch config.Statsd.TagFormat {
	case TagFormatInfluxDB:
		statsdOpts = append(statsdOpts, statsd.TagStyle(statsd.TagFormatInfluxDB))
	case TagFormatDatadog:
		statsdOpts = append(statsdOpts, statsd.TagStyle(statsd.TagFormatDatadog))
	case TagFormatNone:
	default:
		return nil, fmt.Errorf("tag format '%s' was not recognised", config.Statsd.TagFormat)
	}

	client := statsd.NewClient(config.Statsd.Address, statsdOpts...)

	s.s = client
	return s, nil
}

//------------------------------------------------------------------------------

func (h *statsdMetrics) GetCounter(path string) StatCounter {
	return h.GetCounterVec(path).With()
}

func (h *statsdMetrics) GetCounterVec(path string, n ...string) StatCounterVec {
	return &fCounterVec{
		f: func(l ...string) StatCounter {
			return &statsdStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

func (h *statsdMetrics) GetTimer(path string) StatTimer {
	return h.GetTimerVec(path).With()
}

func (h *statsdMetrics) GetTimerVec(path string, n ...string) StatTimerVec {
	return &fTimerVec{
		f: func(l ...string) StatTimer {
			return &statsdStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

func (h *statsdMetrics) GetGauge(path string) StatGauge {
	return h.GetGaugeVec(path).With()
}

func (h *statsdMetrics) GetGaugeVec(path string, n ...string) StatGaugeVec {
	return &fGaugeVec{
		f: func(l ...string) StatGauge {
			return &statsdStat{
				path: path,
				s:    h.s,
				tags: tags(n, l),
			}
		},
	}
}

func (h *statsdMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (h *statsdMetrics) Close() error {
	h.s.Close()
	return nil
}

func tags(labels, values []string) []statsd.Tag {
	if len(labels) != len(values) {
		return nil
	}
	tags := make([]statsd.Tag, len(labels))
	for i := range labels {
		tags[i] = statsd.StringTag(labels[i], values[i])
	}
	return tags
}
