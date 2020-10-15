package metrics

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/quipo/statsd"
)

//------------------------------------------------------------------------------

type wrappedLogger struct {
	m log.Modular
}

func (w *wrappedLogger) Println(v ...interface{}) {
	w.m.Warnf(fmt.Sprintln(v...))
}

//------------------------------------------------------------------------------

// StatsdLegacyStat is a representation of a single metric stat. Interactions with
// this stat are thread safe.
type StatsdLegacyStat struct {
	path string
	s    statsd.Statsd
}

// Incr increments a metric by an amount.
func (s *StatsdLegacyStat) Incr(count int64) error {
	s.s.Incr(s.path, count)
	return nil
}

// Decr decrements a metric by an amount.
func (s *StatsdLegacyStat) Decr(count int64) error {
	s.s.Decr(s.path, count)
	return nil
}

// Timing sets a timing metric.
func (s *StatsdLegacyStat) Timing(delta int64) error {
	s.s.Timing(s.path, delta)
	return nil
}

// Set sets a gauge metric.
func (s *StatsdLegacyStat) Set(value int64) error {
	s.s.Gauge(s.path, value)
	return nil
}

//------------------------------------------------------------------------------

// StatsdLegacy is a stats object with capability to hold internal stats as a JSON
// endpoint.
type StatsdLegacy struct {
	config      Config
	s           statsd.Statsd
	log         log.Modular
	pathMapping *pathMapping
}

// NewStatsdLegacy creates and returns a new StatsdLegacy object.
func NewStatsdLegacy(config Config, opts ...func(Type)) (Type, error) {
	flushPeriod, err := time.ParseDuration(config.Statsd.FlushPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %s", err)
	}
	s := &StatsdLegacy{
		config: config,
		log:    log.Noop(),
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.pathMapping, err = newPathMapping(config.Statsd.PathMapping, s.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

	if config.Statsd.Network == "tcp" {
		s.log.Warnf(
			"Network set to 'tcp', falling back to legacy statsd client. The " +
				"network field is due to be removed in the next major release, " +
				" if you are relying on this field please raise an issue at: " +
				"https://github.com/Jeffail/benthos/issues\n",
		)
	} else {
		s.log.Warnf(
			"Falling back to legacy statsd client. To use the new client set " +
				"the 'tag_format' field to 'none', 'datadog' or 'influxdb'. The " +
				"network field is due to be removed in the next major release, " +
				"if you are relying on this field please raise an issue at: " +
				"https://github.com/Jeffail/benthos/issues\n",
		)
	}

	if config.Statsd.TagFormat != TagFormatNone && config.Statsd.TagFormat != TagFormatLegacy {
		return nil, fmt.Errorf("tag format '%v' is not supported when using 'tcp' network traffic", config.Statsd.TagFormat)
	}

	prefix := config.Statsd.Prefix
	if len(prefix) > 0 && prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}

	statsdclient := statsd.NewStatsdBuffer(
		flushPeriod,
		statsd.NewStatsdClient(config.Statsd.Address, prefix),
	)
	statsdclient.Logger = &wrappedLogger{m: s.log}
	if config.Statsd.Network == "udp" {
		if err := statsdclient.CreateSocket(); err != nil {
			return nil, err
		}
	} else {
		if err := statsdclient.CreateTCPSocket(); err != nil {
			return nil, err
		}
	}
	s.s = statsdclient
	return s, nil
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *StatsdLegacy) GetCounter(path string) StatCounter {
	if path = h.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return &StatsdLegacyStat{
		path: path,
		s:    h.s,
	}
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *StatsdLegacy) GetCounterVec(path string, n []string) StatCounterVec {
	path = h.pathMapping.mapPathNoTags(path)
	return fakeCounterVec(func([]string) StatCounter {
		if len(path) == 0 {
			return DudStat{}
		}
		return &StatsdLegacyStat{
			path: path,
			s:    h.s,
		}
	})
}

// GetTimer returns a stat timer object for a path.
func (h *StatsdLegacy) GetTimer(path string) StatTimer {
	if path = h.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return &StatsdLegacyStat{
		path: path,
		s:    h.s,
	}
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *StatsdLegacy) GetTimerVec(path string, n []string) StatTimerVec {
	path = h.pathMapping.mapPathNoTags(path)
	return fakeTimerVec(func([]string) StatTimer {
		if len(path) == 0 {
			return DudStat{}
		}
		return &StatsdLegacyStat{
			path: path,
			s:    h.s,
		}
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *StatsdLegacy) GetGauge(path string) StatGauge {
	if path = h.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return &StatsdLegacyStat{
		path: path,
		s:    h.s,
	}
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *StatsdLegacy) GetGaugeVec(path string, n []string) StatGaugeVec {
	path = h.pathMapping.mapPathNoTags(path)
	return fakeGaugeVec(func([]string) StatGauge {
		if len(path) == 0 {
			return DudStat{}
		}
		return &StatsdLegacyStat{
			path: path,
			s:    h.s,
		}
	})
}

// SetLogger sets the logger used to print connection errors.
func (h *StatsdLegacy) SetLogger(log log.Modular) {
	h.log = log
}

// Close stops the StatsdLegacy object from aggregating metrics and cleans up
// resources.
func (h *StatsdLegacy) Close() error {
	h.s.Close()
	return nil
}

//------------------------------------------------------------------------------
