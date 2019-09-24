// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"sync"
	"sync/atomic"

	"github.com/Jeffail/benthos/v3/lib/log"
)

//------------------------------------------------------------------------------

// LocalStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type LocalStat struct {
	Value           *int64
	labelsAndValues map[string]string
}

// Incr increments a metric by an amount.
func (l *LocalStat) Incr(count int64) error {
	atomic.AddInt64(l.Value, count)
	return nil
}

// Decr decrements a metric by an amount.
func (l *LocalStat) Decr(count int64) error {
	atomic.AddInt64(l.Value, -count)
	return nil
}

// Timing sets a timing metric.
func (l *LocalStat) Timing(delta int64) error {
	atomic.StoreInt64(l.Value, delta)
	return nil
}

// Set sets a gauge metric.
func (l *LocalStat) Set(value int64) error {
	atomic.StoreInt64(l.Value, value)
	return nil
}

func (l *LocalStat) setLabelsAndValues(ls, vs []string) *LocalStat {
	for i, k := range ls {
		l.labelsAndValues[k] = vs[i]
	}
	return l
}

// HasLabelWithValue takes a label/value pair and returns true if that
// combination has been recorded, or false otherwise.
//
// This is mostly useful in tests for custom processors.
func (l *LocalStat) HasLabelWithValue(k, v string) bool {
	label := l.labelsAndValues[k]
	return v == label
}

func newLocalStat(c int64) *LocalStat {
	return &LocalStat{
		Value:           &c,
		labelsAndValues: make(map[string]string),
	}
}

//------------------------------------------------------------------------------

// Local is a metrics aggregator that stores metrics locally.
type Local struct {
	flatCounters map[string]*LocalStat
	flatTimings  map[string]*LocalStat

	sync.Mutex
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
	l.Lock()
	localFlatCounters := make(map[string]int64, len(l.flatCounters))
	for k := range l.flatCounters {
		localFlatCounters[k] = atomic.LoadInt64(l.flatCounters[k].Value)
		if reset {
			atomic.StoreInt64(l.flatCounters[k].Value, 0)
		}
	}
	l.Unlock()
	return localFlatCounters
}

// GetCountersWithLabels returns a map of metric paths to counters including
// labels and values.
func (l *Local) GetCountersWithLabels() map[string]LocalStat {
	l.Lock()
	localFlatCounters := make(map[string]LocalStat, len(l.flatCounters))
	for k := range l.flatCounters {
		o := l.flatCounters[k]
		cv := atomic.LoadInt64(o.Value)
		ls := *newLocalStat(cv)
		for k, v := range o.labelsAndValues {
			ls.labelsAndValues[k] = v
		}
		localFlatCounters[k] = ls
	}
	l.Unlock()
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
	l.Lock()
	localFlatTimings := make(map[string]int64, len(l.flatTimings))
	for k := range l.flatTimings {
		localFlatTimings[k] = atomic.LoadInt64(l.flatTimings[k].Value)
		if reset {
			atomic.StoreInt64(l.flatTimings[k].Value, 0)
		}
	}
	l.Unlock()
	return localFlatTimings
}

// GetTimingsWithLabels returns a map of metric paths to timers, including
// labels and values.
func (l *Local) GetTimingsWithLabels() map[string]LocalStat {
	l.Lock()
	localFlatTimings := make(map[string]LocalStat, len(l.flatTimings))
	for k := range l.flatTimings {
		o := l.flatTimings[k]
		cv := atomic.LoadInt64(o.Value)
		ls := *newLocalStat(cv)
		for k, v := range o.labelsAndValues {
			ls.labelsAndValues[k] = v
		}
		localFlatTimings[k] = ls
	}
	l.Unlock()
	return localFlatTimings
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (l *Local) GetCounter(path string) StatCounter {
	l.Lock()
	st, exists := l.flatCounters[path]
	if !exists {
		st = newLocalStat(0)
		l.flatCounters[path] = st
	}
	l.Unlock()

	return st
}

// GetTimer returns a stat timer object for a path.
func (l *Local) GetTimer(path string) StatTimer {
	l.Lock()
	st, exists := l.flatTimings[path]
	if !exists {
		st = newLocalStat(0)
		l.flatTimings[path] = st
	}
	l.Unlock()

	return st
}

// GetGauge returns a stat gauge object for a path.
func (l *Local) GetGauge(path string) StatGauge {
	l.Lock()
	st, exists := l.flatCounters[path]
	if !exists {
		st = newLocalStat(0)
		l.flatCounters[path] = st
	}
	l.Unlock()

	return st
}

// GetCounterVec returns a stat counter object for a path and records the
// labels and values.
func (l *Local) GetCounterVec(path string, k []string) StatCounterVec {
	return fakeCounterVec(func(v []string) StatCounter {
		return l.GetCounter(path).(*LocalStat).setLabelsAndValues(k, v)
	})
}

// GetTimerVec returns a stat timer object for a path with the labels
// and values.
func (l *Local) GetTimerVec(path string, k []string) StatTimerVec {
	return fakeTimerVec(func(v []string) StatTimer {
		return l.GetTimer(path).(*LocalStat).setLabelsAndValues(k, v)
	})
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (l *Local) GetGaugeVec(path string, k []string) StatGaugeVec {
	return fakeGaugeVec(func(v []string) StatGauge {
		return l.GetGauge(path).(*LocalStat).setLabelsAndValues(k, v)
	})
}

// SetLogger does nothing.
func (l *Local) SetLogger(logger log.Modular) {}

// Close stops the Local object from aggregating metrics and cleans up
// resources.
func (l *Local) Close() error {
	return nil
}

//------------------------------------------------------------------------------
