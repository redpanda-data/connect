package pure

import (
	"fmt"
	"net/http"
	"time"

	gmetrics "github.com/rcrowley/go-metrics"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	_ = bundle.AllMetrics.Add(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
		return newLogger(conf.Logger, nm.Logger())
	}, docs.ComponentSpec{
		Name:    "logger",
		Type:    docs.TypeMetrics,
		Status:  docs.StatusBeta,
		Summary: `Prints aggregated metrics through the logger.`,
		Description: `
Prints each metric produced by Benthos as a log event (level ` + "`info`" + ` by default) during shutdown, and optionally on an interval.

This metrics type is useful for debugging pipelines when you only have access to the logger output and not the service-wide server. Otherwise it's recommended that you use either the ` + "`prometheus` or `json_api`" + `types.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("push_interval", "An optional period of time to continuously print all metrics.").HasDefault(""),
			docs.FieldBool("flush_metrics", "Whether counters and timing metrics should be reset to 0 each time metrics are printed.").HasDefault(false),
		),
	})
}

//------------------------------------------------------------------------------

type loggerMetrics struct {
	local   *metrics.Local
	log     log.Modular
	flush   bool
	shutSig *shutdown.Signaller
}

func newLogger(config metrics.LoggerConfig, log log.Modular) (metrics.Type, error) {
	t := &loggerMetrics{
		local:   metrics.NewLocal(),
		log:     log,
		flush:   config.FlushMetrics,
		shutSig: shutdown.NewSignaller(),
	}

	if len(config.PushInterval) > 0 {
		interval, err := time.ParseDuration(config.PushInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse push interval: %v", err)
		}
		go func() {
			for {
				select {
				case <-t.shutSig.CloseAtLeisureChan():
					return
				case <-time.After(interval):
					t.publishMetrics()
				}
			}
		}()
	}

	return t, nil
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
			e = e.WithFields(tagKVs)
		}
		e.Infoln("Counter metric")
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
			e = e.WithFields(tagKVs)
		}
		e.Infoln("Timing metric")
	}
}

func (s *loggerMetrics) GetCounter(path string) metrics.StatCounter {
	return s.GetCounterVec(path).With()
}

func (s *loggerMetrics) GetCounterVec(path string, n ...string) metrics.StatCounterVec {
	return s.local.GetCounterVec(path, n...)
}

func (s *loggerMetrics) GetTimer(path string) metrics.StatTimer {
	return s.GetTimerVec(path).With()
}

func (s *loggerMetrics) GetTimerVec(path string, n ...string) metrics.StatTimerVec {
	return s.local.GetTimerVec(path, n...)
}

func (s *loggerMetrics) GetGauge(path string) metrics.StatGauge {
	return s.GetGaugeVec(path).With()
}

func (s *loggerMetrics) GetGaugeVec(path string, n ...string) metrics.StatGaugeVec {
	return s.local.GetGaugeVec(path, n...)
}

func (s *loggerMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (s *loggerMetrics) Close() error {
	s.shutSig.CloseNow()
	s.publishMetrics()
	return nil
}
