package pure

import (
	"context"
	"net/http"
	"time"

	gmetrics "github.com/rcrowley/go-metrics"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	lmFieldPushInterval = "push_interval"
	lmFieldFlushMetrics = "flush_metrics"
)

func loggerMetricsSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary(`Prints aggregated metrics through the logger.`).
		Description(`
Prints each metric produced by Benthos as a log event (level `+"`info`"+` by default) during shutdown, and optionally on an interval.

This metrics type is useful for debugging pipelines when you only have access to the logger output and not the service-wide server. Otherwise it's recommended that you use either the `+"`prometheus` or `json_api`"+`types.`).
		Fields(
			service.NewStringField(lmFieldPushInterval).
				Description("An optional period of time to continuously print all metrics.").
				Optional(),
			service.NewBoolField(lmFieldFlushMetrics).
				Description("Whether counters and timing metrics should be reset to 0 each time metrics are printed.").
				Default(false),
		)
}

func init() {
	err := service.RegisterMetricsExporter("logger", loggerMetricsSpec(),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return newLoggerFromParsed(conf, log)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type loggerMetrics struct {
	local   *metrics.Local
	log     *service.Logger
	flush   bool
	shutSig *shutdown.Signaller
}

func newLoggerFromParsed(conf *service.ParsedConfig, log *service.Logger) (l *loggerMetrics, err error) {
	l = &loggerMetrics{
		local:   metrics.NewLocal(),
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}
	if l.flush, err = conf.FieldBool(lmFieldFlushMetrics); err != nil {
		return
	}

	if piStr, _ := conf.FieldString(lmFieldPushInterval); piStr != "" {
		var interval time.Duration
		if interval, err = conf.FieldDuration(lmFieldPushInterval); err != nil {
			return
		}
		go func() {
			for {
				select {
				case <-l.shutSig.CloseAtLeisureChan():
					return
				case <-time.After(interval):
					l.publishMetrics()
				}
			}
		}()
	}
	return
}

//------------------------------------------------------------------------------

func (s *loggerMetrics) publishMetrics() {
	var counters map[string]int64
	var timings map[string]gmetrics.Timer
	if s.flush {
		counters = s.local.FlushCounters()
		timings = s.local.FlushTimings()
	} else {
		counters = s.local.GetCounters()
		timings = s.local.GetTimings()
	}

	for k, v := range counters {
		name, tagNames, tagValues := metrics.ReverseLabelledPath(k)
		e := s.log.With("name", name, "value", v)
		if len(tagNames) > 0 {
			tagKVs := map[string]string{}
			for i := range tagNames {
				tagKVs[tagNames[i]] = tagValues[i]
			}
			e = e.With(tagKVs)
		}
		e.Info("Counter metric")
	}

	for k, v := range timings {
		ps := v.Percentiles([]float64{0.5, 0.9, 0.99})
		name, tagNames, tagValues := metrics.ReverseLabelledPath(k)
		e := s.log.With("name", name, "p50", ps[0], "p90", ps[1], "p99", ps[2])
		if len(tagNames) > 0 {
			tagKVs := map[string]string{}
			for i := range tagNames {
				tagKVs[tagNames[i]] = tagValues[i]
			}
			e = e.With(tagKVs)
		}
		e.Info("Timing metric")
	}
}

func (s *loggerMetrics) NewCounterCtor(path string, n ...string) service.MetricsExporterCounterCtor {
	tmp := s.local.GetCounterVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterCounter {
		return tmp.With(labelValues...)
	}
}

func (s *loggerMetrics) NewTimerCtor(path string, n ...string) service.MetricsExporterTimerCtor {
	tmp := s.local.GetTimerVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterTimer {
		return tmp.With(labelValues...)
	}
}

func (s *loggerMetrics) NewGaugeCtor(path string, n ...string) service.MetricsExporterGaugeCtor {
	tmp := s.local.GetGaugeVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterGauge {
		return tmp.With(labelValues...)
	}
}

func (s *loggerMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (s *loggerMetrics) Close(context.Context) error {
	s.shutSig.CloseNow()
	s.publishMetrics()
	return nil
}
