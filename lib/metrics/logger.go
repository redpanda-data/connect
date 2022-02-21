package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/rcrowley/go-metrics"
)

func init() {
	Constructors["logger"] = TypeSpec{
		constructor: func(conf Config, log log.Modular) (Type, error) {
			return newLogger(conf.Logger, log)
		},
		Status:  docs.StatusBeta,
		Summary: `Prints aggregated metrics through the logger.`,
		Description: `
Prints each metric produced by Benthos as a log event (level ` + "`info`" + ` by default) during shutdown, and optionally on an interval.

This metrics type is useful for debugging pipelines when you only have access to the logger output and not the service-wide server. Otherwise it's recommended that you use either the ` + "`prometheus` or `json_api`" + `types.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("push_interval", "An optional period of time to continuously print all metrics."),
			docs.FieldCommon("flush_metrics", "Whether counters and timing metrics should be reset to 0 each time metrics are printed."),
		},
	}
}

//------------------------------------------------------------------------------

// LoggerConfig contains configuration parameters for the Stdout metrics
// aggregator.
type LoggerConfig struct {
	PushInterval string `json:"push_interval" yaml:"push_interval"`
	FlushMetrics bool   `json:"flush_metrics" yaml:"flush_metrics"`
}

// NewLoggerConfig returns a new StdoutConfig with default values.
func NewLoggerConfig() LoggerConfig {
	return LoggerConfig{
		PushInterval: "",
		FlushMetrics: false,
	}
}

//------------------------------------------------------------------------------

type loggerMetrics struct {
	local   *Local
	log     log.Modular
	flush   bool
	shutSig *shutdown.Signaller
}

func newLogger(config LoggerConfig, log log.Modular) (Type, error) {
	t := &loggerMetrics{
		local:   NewLocal(),
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
	var timings map[string]metrics.Timer
	if s.flush {
		counters = s.local.FlushCounters()
		timings = s.local.FlushTimings()
	} else {
		counters = s.local.GetCounters()
		timings = s.local.GetTimings()
	}

	for k, v := range counters {
		name, tagNames, tagValues := reverseLabelledPath(k)
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
		name, tagNames, tagValues := reverseLabelledPath(k)
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

func (s *loggerMetrics) GetCounter(path string) StatCounter {
	return s.GetCounterVec(path).With()
}

func (s *loggerMetrics) GetCounterVec(path string, n ...string) StatCounterVec {
	return s.local.GetCounterVec(path, n...)
}

func (s *loggerMetrics) GetTimer(path string) StatTimer {
	return s.GetTimerVec(path).With()
}

func (s *loggerMetrics) GetTimerVec(path string, n ...string) StatTimerVec {
	return s.local.GetTimerVec(path, n...)
}

func (s *loggerMetrics) GetGauge(path string) StatGauge {
	return s.GetGaugeVec(path).With()
}

func (s *loggerMetrics) GetGaugeVec(path string, n ...string) StatGaugeVec {
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
