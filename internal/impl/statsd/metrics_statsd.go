package statsd

import (
	"fmt"
	"net/http"
	"time"

	statsd "github.com/smira/go-statsd"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func init() {
	_ = bundle.AllMetrics.Add(newStatsd, docs.ComponentSpec{
		Name:   "statsd",
		Type:   docs.TypeMetrics,
		Status: docs.StatusStable,
		Summary: `
Pushes metrics using the [StatsD protocol](https://github.com/statsd/statsd).
Supported tagging formats are 'none', 'datadog' and 'influxdb'.`,
		Description: `
The underlying client library has recently been updated in order to support
tagging.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("address", "The address to send metrics to.").HasDefault(""),
			docs.FieldString("flush_period", "The time interval between metrics flushes.").HasDefault("100ms"),
			docs.FieldString("tag_format", "Metrics tagging is supported in a variety of formats.").HasOptions(
				"none", "datadog", "influxdb",
			).HasDefault("none"),
		),
	})
}

//------------------------------------------------------------------------------

type wrappedDatadogLogger struct {
	log log.Modular
}

func (s wrappedDatadogLogger) Printf(msg string, args ...any) {
	s.log.Warnf(fmt.Sprintf(msg, args...))
}

//------------------------------------------------------------------------------

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
	config metrics.Config
	s      *statsd.Client
	log    log.Modular
}

func newStatsd(config metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %s", err)
	}

	s := &statsdMetrics{
		config: config,
		log:    nm.Logger(),
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

func (h *statsdMetrics) GetCounter(path string) metrics.StatCounter {
	return h.GetCounterVec(path).With()
}

func (h *statsdMetrics) GetCounterVec(path string, n ...string) metrics.StatCounterVec {
	return metrics.FakeCounterVec(func(l ...string) metrics.StatCounter {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, l),
		}
	})
}

func (h *statsdMetrics) GetTimer(path string) metrics.StatTimer {
	return h.GetTimerVec(path).With()
}

func (h *statsdMetrics) GetTimerVec(path string, n ...string) metrics.StatTimerVec {
	return metrics.FakeTimerVec(func(l ...string) metrics.StatTimer {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, l),
		}
	})
}

func (h *statsdMetrics) GetGauge(path string) metrics.StatGauge {
	return h.GetGaugeVec(path).With()
}

func (h *statsdMetrics) GetGaugeVec(path string, n ...string) metrics.StatGaugeVec {
	return metrics.FakeGaugeVec(func(l ...string) metrics.StatGauge {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, l),
		}
	})
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
