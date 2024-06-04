package statsd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	statsd "github.com/smira/go-statsd"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	smFieldAddress     = "address"
	smFieldFlushPeriod = "flush_period"
	smFieldTagFormat   = "tag_format"
)

func statsdSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Pushes metrics using the https://github.com/statsd/statsd[StatsD protocol^]. Supported tagging formats are 'none', 'datadog' and 'influxdb'.").
		Fields(
			service.NewStringField(smFieldAddress).
				Description("The address to send metrics to."),
			service.NewDurationField(smFieldFlushPeriod).
				Description("The time interval between metrics flushes.").
				Default("100ms"),
			service.NewStringEnumField(smFieldTagFormat, "none", "datadog", "influxdb").
				Description("Metrics tagging is supported in a variety of formats.").
				Default("none"),
		)
}

func init() {
	err := service.RegisterMetricsExporter("statsd", statsdSpec(), func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
		return newStatsdFromParsed(conf, log)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type wrappedDatadogLogger struct {
	log *service.Logger
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

func (s *statsdStat) IncrFloat64(count float64) {
	s.Incr(int64(count))
}

func (s *statsdStat) Decr(count int64) {
	s.s.Decr(s.path, count, s.tags...)
}

func (s *statsdStat) DecrFloat64(count float64) {
	s.Decr(int64(count))
}

func (s *statsdStat) Timing(delta int64) {
	s.s.Timing(s.path, delta, s.tags...)
}

func (s *statsdStat) Set(value int64) {
	s.s.Gauge(s.path, value, s.tags...)
}

func (s *statsdStat) SetFloat64(value float64) {
	s.Set(int64(value))
}

//------------------------------------------------------------------------------

type statsdMetrics struct {
	s   *statsd.Client
	log *service.Logger
}

func newStatsdFromParsed(conf *service.ParsedConfig, log *service.Logger) (s *statsdMetrics, err error) {
	s = &statsdMetrics{
		log: log,
	}

	var flushPeriod time.Duration
	if flushPeriod, err = conf.FieldDuration(smFieldFlushPeriod); err != nil {
		return
	}

	statsdOpts := []statsd.Option{
		statsd.FlushInterval(flushPeriod),
		statsd.Logger(wrappedDatadogLogger{log: s.log}),
	}

	var tagFormatStr string
	if tagFormatStr, err = conf.FieldString(smFieldTagFormat); err != nil {
		return
	}

	switch tagFormatStr {
	case TagFormatInfluxDB:
		statsdOpts = append(statsdOpts, statsd.TagStyle(statsd.TagFormatInfluxDB))
	case TagFormatDatadog:
		statsdOpts = append(statsdOpts, statsd.TagStyle(statsd.TagFormatDatadog))
	case TagFormatNone:
	default:
		return nil, fmt.Errorf("tag format '%s' was not recognised", tagFormatStr)
	}

	var address string
	if address, err = conf.FieldString(smFieldAddress); err != nil {
		return
	}

	client := statsd.NewClient(address, statsdOpts...)

	s.s = client
	return s, nil
}

//------------------------------------------------------------------------------

func (h *statsdMetrics) NewCounterCtor(path string, n ...string) service.MetricsExporterCounterCtor {
	return func(labelValues ...string) service.MetricsExporterCounter {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, labelValues),
		}
	}
}

func (h *statsdMetrics) NewTimerCtor(path string, n ...string) service.MetricsExporterTimerCtor {
	return func(labelValues ...string) service.MetricsExporterTimer {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, labelValues),
		}
	}
}

func (h *statsdMetrics) NewGaugeCtor(path string, n ...string) service.MetricsExporterGaugeCtor {
	return func(labelValues ...string) service.MetricsExporterGauge {
		return &statsdStat{
			path: path,
			s:    h.s,
			tags: tags(n, labelValues),
		}
	}
}

func (h *statsdMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (h *statsdMetrics) Close(context.Context) error {
	_ = h.s.Close()
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
