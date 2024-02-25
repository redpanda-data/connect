package metrics

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rcrowley/go-metrics"
)

// not sure if this is necessary yet.
var tagEncodingSeparator = ","

// LocalStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type LocalStat struct {
	Value *int64
}

// Incr increments a metric by an int64 amount.
func (l *LocalStat) Incr(count int64) {
	atomic.AddInt64(l.Value, count)
}

// Decr decrements a metric by an amount.
func (l *LocalStat) Decr(count int64) {
	atomic.AddInt64(l.Value, -count)
}

// Set sets a gauge metric.
func (l *LocalStat) Set(value int64) {
	atomic.StoreInt64(l.Value, value)
}

func (l *LocalStat) IncrFloat64(count float64) {
	l.Incr(int64(count))
}

func (l *LocalStat) DecrFloat64(count float64) {
	l.Decr(int64(count))
}

func (l *LocalStat) SetFloat64(value float64) {
	l.Set(int64(value))
}

// LocalTiming is a representation of a single metric timing.
type LocalTiming struct {
	t    metrics.Timer
	lock sync.Mutex
}

// Timing sets a timing metric.
func (l *LocalTiming) Timing(delta int64) {
	l.lock.Lock()
	l.t.Update(time.Duration(delta))
	l.lock.Unlock()
}

//------------------------------------------------------------------------------

// Local is a metrics aggregator that stores metrics locally.
type Local struct {
	flatCounters map[string]*LocalStat
	flatTimings  map[string]*LocalTiming

	mut sync.Mutex
}

// NewLocal creates and returns a new Local aggregator.
func NewLocal() *Local {
	return &Local{
		flatCounters: make(map[string]*LocalStat),
		flatTimings:  make(map[string]*LocalTiming),
	}
}

//------------------------------------------------------------------------------

// GetCounters returns a map of metric paths to counters.
func (l *Local) GetCounters() map[string]int64 {
	return l.getCounters(false)
}

// FlushCounters returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0.
func (l *Local) FlushCounters() map[string]int64 {
	return l.getCounters(true)
}

// getCounters internal method that returns a copy of the counter maps before
// optionally reseting the counter as determined by the reset value passed in.
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
func (l *Local) GetTimings() map[string]metrics.Timer {
	return l.getTimings(false)
}

// FlushTimings returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0.
func (l *Local) FlushTimings() map[string]metrics.Timer {
	return l.getTimings(true)
}

// FlushTimings returns a map of the current state of the metrics paths to
// counters and then resets the counters to 0.
func (l *Local) getTimings(reset bool) map[string]metrics.Timer {
	l.mut.Lock()
	localFlatTimings := make(map[string]metrics.Timer, len(l.flatTimings))
	for k, v := range l.flatTimings {
		v.lock.Lock()
		localFlatTimings[k] = v.t.Snapshot()
		if reset {
			v.t.Stop()
			v.t = metrics.NewTimer()
		}
		v.lock.Unlock()
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

		sortedTagNames := make([]string, len(tagNames))
		copy(sortedTagNames, tagNames)
		sort.Strings(sortedTagNames)

		b.WriteByte('{')
		for i, v := range sortedTagNames {
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

// ReverseLabelledPath extracts a name, tag names and tag values from a labelled
// metric name.
func ReverseLabelledPath(path string) (name string, tagNames, tagValues []string) {
	if !strings.HasSuffix(path, "}") {
		name = path
		return
	}

	labelsStart := strings.Index(path, "{")
	if labelsStart == -1 {
		name = path
		return
	}

	name = path[:labelsStart]
	for _, tagKVStr := range strings.Split(path[labelsStart+1:len(path)-1], tagEncodingSeparator) {
		tagKV := strings.Split(tagKVStr, "=")
		if len(tagKV) != 2 {
			continue
		}
		tagNames = append(tagNames, tagKV[0])
		tagValue, _ := strconv.Unquote(tagKV[1])
		tagValues = append(tagValues, tagValue)
	}
	return
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
	return FakeCounterVec(func(v ...string) StatCounter {
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
	return FakeTimerVec(func(v ...string) StatTimer {
		newPath := createLabelledPath(path, k, v)
		l.mut.Lock()
		st, exists := l.flatTimings[newPath]
		if !exists {
			st = &LocalTiming{t: metrics.NewTimer()}
			l.flatTimings[newPath] = st
		}
		l.mut.Unlock()
		return st
	})
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (l *Local) GetGaugeVec(path string, k ...string) StatGaugeVec {
	return FakeGaugeVec(func(v ...string) StatGauge {
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
