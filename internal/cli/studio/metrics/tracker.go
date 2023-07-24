package metrics

import (
	"net/http"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
)

// Tracker keeps a reference to observed metrics and is capable of flushing the
// currently observed counters. This also implements the internal metrics type
// as it's used as a drop-in replacement in order to gather those observed
// counters.
type Tracker struct {
	currentEpoch *Observed
	lastFlushed  time.Time
	tNowFn       func() time.Time
	m            sync.Mutex
}

// OptSetNowFn sets the function used to obtain a new time value representing
// now. By default time.Now is used.
func OptSetNowFn(fn func() time.Time) func(*Tracker) {
	return func(t *Tracker) {
		t.tNowFn = fn
	}
}

// NewTracker returns a metrics implementation that records studio specific
// metrics information.
func NewTracker(opts ...func(t *Tracker)) *Tracker {
	t := &Tracker{
		currentEpoch: newObserved(),
		tNowFn:       time.Now,
	}
	for _, opt := range opts {
		opt(t)
	}
	t.lastFlushed = t.tNowFn()
	return t
}

// Flush the latest observed metrics and reset all counters for the next epoch.
func (t *Tracker) Flush() *Observed {
	t.m.Lock()
	current := t.currentEpoch
	t.currentEpoch = newObserved()
	t.lastFlushed = t.tNowFn()
	t.m.Unlock()
	return current
}

// LastFlushed returns the time at which the metrics were last flushed.
func (t *Tracker) LastFlushed() time.Time {
	t.m.Lock()
	v := t.lastFlushed
	t.m.Unlock()
	return v
}

func (t *Tracker) withCurrentEpoch(fn func(*Observed)) {
	t.m.Lock()
	fn(t.currentEpoch)
	t.m.Unlock()
}

type closureStat func(v int64)

func (c closureStat) Incr(v int64) {
	c(v)
}

func (c closureStat) Decr(v int64) {
	c(v)
}

func (c closureStat) SetInt64(v int64) {
	c(v)
}

func (c closureStat) Timing(v int64) {
	c(v)
}

func (t *Tracker) counterForNameAndLabel(path, label string) metrics.StatCounter {
	switch path {
	case "input_received":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				i := m.Input[label]
				i.Received += v
				m.Input[label] = i
			})
		})
	case "processor_received":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				p := m.Processor[label]
				p.Received += v
				m.Processor[label] = p
			})
		})
	case "processor_sent":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				p := m.Processor[label]
				p.Sent += v
				m.Processor[label] = p
			})
		})
	case "processor_error":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				p := m.Processor[label]
				p.Error += v
				m.Processor[label] = p
			})
		})
	case "output_sent":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				o := m.Output[label]
				o.Sent += v
				m.Output[label] = o
			})
		})
	case "output_error":
		return closureStat(func(v int64) {
			t.withCurrentEpoch(func(m *Observed) {
				o := m.Output[label]
				o.Error += v
				m.Output[label] = o
			})
		})
	}
	return metrics.DudStat{}
}

// GetCounter returns an editable counter stat for a given path.
func (t *Tracker) GetCounter(name string) metrics.StatCounter {
	return t.counterForNameAndLabel(name, "")
}

// GetCounterVec returns an editable counter stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the
// same path.
func (t *Tracker) GetCounterVec(name string, labelNames ...string) metrics.StatCounterVec {
	labelNamesToIndex := map[string]int{}
	for i, n := range labelNames {
		labelNamesToIndex[n] = i
	}
	return metrics.FakeCounterVec(func(s ...string) metrics.StatCounter {
		label := ""
		if index, exists := labelNamesToIndex["label"]; exists && len(s) > index {
			label = s[index]
		}
		return t.counterForNameAndLabel(name, label)
	})
}

// GetTimer returns an editable timer stat for a given path.
func (t *Tracker) GetTimer(name string) metrics.StatTimer {
	return metrics.DudStat{} // Not using any of these metrics (yet)
}

// GetTimerVec returns an editable timer stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the
// same path.
func (t *Tracker) GetTimerVec(name string, labelNames ...string) metrics.StatTimerVec {
	return metrics.FakeTimerVec(func(s ...string) metrics.StatTimer {
		return metrics.DudStat{} // Not using any of these metrics (yet)
	})
}

// GetGauge returns an editable gauge stat for a given path.
func (t *Tracker) GetGauge(name string) metrics.StatGauge {
	return metrics.DudStat{} // Not using any of these metrics (yet)
}

// GetGaugeVec returns an editable gauge stat for a given path with labels,
// these labels must be consistent with any other metrics registered on the
// same path.
func (t *Tracker) GetGaugeVec(name string, labelNames ...string) metrics.StatGaugeVec {
	return metrics.FakeGaugeVec(func(s ...string) metrics.StatGauge {
		return metrics.DudStat{} // Not using any of these metrics (yet)
	})
}

// HandlerFunc returns an optional HTTP request handler that exposes metrics
// from the implementation. If nil is returned then no endpoint will be
// registered.
func (t *Tracker) HandlerFunc() http.HandlerFunc {
	return nil
}

// Close stops aggregating stats and cleans up resources.
func (t *Tracker) Close() error {
	return nil
}
