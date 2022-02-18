package metrics

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// LocalStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type LocalStat struct {
	Value *int64
}

// Incr increments a metric by an amount.
func (l *LocalStat) Incr(count int64) {
	atomic.AddInt64(l.Value, count)
}

// Decr decrements a metric by an amount.
func (l *LocalStat) Decr(count int64) {
	atomic.AddInt64(l.Value, -count)
}

// Timing sets a timing metric.
func (l *LocalStat) Timing(delta int64) {
	atomic.StoreInt64(l.Value, delta)
}

// Set sets a gauge metric.
func (l *LocalStat) Set(value int64) {
	atomic.StoreInt64(l.Value, value)
}

//------------------------------------------------------------------------------

// Local is a metrics aggregator that stores metrics locally.
type Local struct {
	flatCounters map[string]*LocalStat
	flatTimings  map[string]*LocalStat

	mut sync.Mutex
}

// NewLocal creates and returns a new Local aggregator.
func NewLocal() *Local {
	return &Local{
		flatCounters: make(map[string]*LocalStat),
		flatTimings:  make(map[string]*LocalStat),
	}
}

//------------------------------------------------------------------------------

// GetCounters returns a map of metric paths to counters.
func (l *Local) GetCounters() map[string]int64 {
	return l.getCounters(false)
}

// FlushCounters returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0
func (l *Local) FlushCounters() map[string]int64 {
	return l.getCounters(true)
}

// getCounters internal method that returns a copy of the counter maps before
// optionally reseting the counter as determined by the reset value passed in
func (l *Local) getCounters(reset bool) map[string]int64 {
	l.mut.Lock()
	localFlatCounters := make(map[string]int64, len(l.flatCounters))
	for k := range l.flatCounters {
		localFlatCounters[k] = atomic.LoadInt64(l.flatCounters[k].Value)
		if reset {
			atomic.StoreInt64(l.flatCounters[k].Value, 0)
		}
	}
	l.mut.Unlock()
	return localFlatCounters
}

// GetTimings returns a map of metric paths to timers.
func (l *Local) GetTimings() map[string]int64 {
	return l.getTimings(false)
}

// FlushTimings returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0
func (l *Local) FlushTimings() map[string]int64 {
	return l.getTimings(true)
}

// FlushTimings returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0
func (l *Local) getTimings(reset bool) map[string]int64 {
	l.mut.Lock()
	localFlatTimings := make(map[string]int64, len(l.flatTimings))
	for k := range l.flatTimings {
		localFlatTimings[k] = atomic.LoadInt64(l.flatTimings[k].Value)
		if reset {
			atomic.StoreInt64(l.flatTimings[k].Value, 0)
		}
	}
	l.mut.Unlock()
	return localFlatTimings
}

//------------------------------------------------------------------------------

func createLabelledPath(name string, tagNames, tagValues []string) string {
	if len(tagNames) == 0 {
		return name
	}

	b := &strings.Builder{}
	b.WriteString(name)

	if len(tagNames) == len(tagValues) {
		tags := make(map[string]string, len(tagNames))
		for k, v := range tagNames {
			tags[v] = tagValues[k]
		}
		sort.Strings(tagNames)

		b.WriteByte('{')
		for i, v := range tagNames {
			if i > 0 {
				b.WriteString(tagEncodingSeparator)
			}
			b.WriteString(v)
			b.WriteString("=")
			b.WriteString(strconv.QuoteToASCII(tags[v]))
		}
		b.WriteByte('}')
	}
	return b.String()
}

// GetCounter returns a stat counter object for a path.
func (l *Local) GetCounter(path string) StatCounter {
	return l.GetCounterVec(path).With()
}

// GetTimer returns a stat timer object for a path.
func (l *Local) GetTimer(path string) StatTimer {
	return l.GetTimerVec(path).With()
}

// GetGauge returns a stat gauge object for a path.
func (l *Local) GetGauge(path string) StatGauge {
	return l.GetGaugeVec(path).With()
}

// GetCounterVec returns a stat counter object for a path and records the
// labels and values.
func (l *Local) GetCounterVec(path string, k ...string) StatCounterVec {
	return fakeCounterVec(func(v ...string) StatCounter {
		newPath := createLabelledPath(path, k, v)
		l.mut.Lock()
		st, exists := l.flatCounters[newPath]
		if !exists {
			var i int64
			st = &LocalStat{Value: &i}
			l.flatCounters[newPath] = st
		}
		l.mut.Unlock()
		return st
	})
}

// GetTimerVec returns a stat timer object for a path with the labels
// and values.
func (l *Local) GetTimerVec(path string, k ...string) StatTimerVec {
	return fakeTimerVec(func(v ...string) StatTimer {
		newPath := createLabelledPath(path, k, v)
		l.mut.Lock()
		st, exists := l.flatTimings[newPath]
		if !exists {
			var i int64
			st = &LocalStat{Value: &i}
			l.flatTimings[newPath] = st
		}
		l.mut.Unlock()
		return st
	})
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (l *Local) GetGaugeVec(path string, k ...string) StatGaugeVec {
	return fakeGaugeVec(func(v ...string) StatGauge {
		newPath := createLabelledPath(path, k, v)
		l.mut.Lock()
		st, exists := l.flatCounters[newPath]
		if !exists {
			var i int64
			st = &LocalStat{Value: &i}
			l.flatCounters[newPath] = st
		}
		l.mut.Unlock()
		return st
	})
}

// HandlerFunc returns nil.
func (l *Local) HandlerFunc() http.HandlerFunc {
	return nil
}

// Close stops the Local object from aggregating metrics and cleans up
// resources.
func (l *Local) Close() error {
	return nil
}
