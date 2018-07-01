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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

// LocalStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type LocalStat struct {
	value *int64
}

// Incr increments a metric by an amount.
func (l *LocalStat) Incr(count int64) error {
	atomic.AddInt64(l.value, count)
	return nil
}

// Decr decrements a metric by an amount.
func (l *LocalStat) Decr(count int64) error {
	atomic.AddInt64(l.value, -count)
	return nil
}

// Timing sets a timing metric.
func (l *LocalStat) Timing(delta int64) error {
	atomic.StoreInt64(l.value, delta)
	return nil
}

// Gauge sets a gauge metric.
func (l *LocalStat) Gauge(value int64) error {
	atomic.StoreInt64(l.value, value)
	return nil
}

//------------------------------------------------------------------------------

// Local is a metrics aggregator that stores metrics locally.
type Local struct {
	flatCounters map[string]*int64
	flatTimings  map[string]*int64

	sync.Mutex
}

// NewLocal creates and returns a new Local aggregator.
func NewLocal() *Local {
	return &Local{
		flatCounters: map[string]*int64{},
		flatTimings:  map[string]*int64{},
	}
}

//------------------------------------------------------------------------------

// GetCounters returns a map of metric paths to counters.
func (l *Local) GetCounters() map[string]int64 {
	l.Lock()
	localFlatCounters := make(map[string]int64, len(l.flatCounters))
	for k := range l.flatCounters {
		localFlatCounters[k] = atomic.LoadInt64(l.flatCounters[k])
	}
	l.Unlock()
	return localFlatCounters
}

// GetTimings returns a map of metric paths to timers.
func (l *Local) GetTimings() map[string]int64 {
	l.Lock()
	localFlatTimings := make(map[string]int64, len(l.flatTimings))
	for k := range l.flatTimings {
		localFlatTimings[k] = atomic.LoadInt64(l.flatTimings[k])
	}
	l.Unlock()
	return localFlatTimings
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (l *Local) GetCounter(path ...string) StatCounter {
	dotPath := strings.Join(path, ".")

	l.Lock()
	ptr, exists := l.flatCounters[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		l.flatCounters[dotPath] = ptr
	}
	l.Unlock()

	return &LocalStat{
		value: ptr,
	}
}

// GetTimer returns a stat timer object for a path.
func (l *Local) GetTimer(path ...string) StatTimer {
	dotPath := strings.Join(path, ".")

	l.Lock()
	ptr, exists := l.flatTimings[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		l.flatTimings[dotPath] = ptr
	}
	l.Unlock()

	return &LocalStat{
		value: ptr,
	}
}

// GetGauge returns a stat gauge object for a path.
func (l *Local) GetGauge(path ...string) StatGauge {
	dotPath := strings.Join(path, ".")

	l.Lock()
	ptr, exists := l.flatCounters[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		l.flatCounters[dotPath] = ptr
	}
	l.Unlock()

	return &LocalStat{
		value: ptr,
	}
}

// Incr increments a stat by a value.
func (l *Local) Incr(stat string, value int64) error {
	l.Lock()
	if ptr, exists := l.flatCounters[stat]; !exists {
		ctr := value
		l.flatCounters[stat] = &ctr
	} else {
		atomic.AddInt64(ptr, value)
	}
	l.Unlock()
	return nil
}

// Decr decrements a stat by a value.
func (l *Local) Decr(stat string, value int64) error {
	l.Lock()
	if ptr, exists := l.flatCounters[stat]; !exists {
		ctr := -value
		l.flatCounters[stat] = &ctr
	} else {
		atomic.AddInt64(ptr, -value)
	}
	l.Unlock()
	return nil
}

// Timing sets a stat representing a duration.
func (l *Local) Timing(stat string, delta int64) error {
	l.Lock()
	if ptr, exists := l.flatTimings[stat]; !exists {
		ctr := delta
		l.flatTimings[stat] = &ctr
	} else {
		atomic.StoreInt64(ptr, delta)
	}
	l.Unlock()
	return nil
}

// Gauge sets a stat as a gauge value.
func (l *Local) Gauge(stat string, value int64) error {
	l.Lock()
	if ptr, exists := l.flatCounters[stat]; !exists {
		ctr := value
		l.flatTimings[stat] = &ctr
	} else {
		atomic.StoreInt64(ptr, value)
	}
	l.Unlock()
	return nil
}

// SetLogger does nothing.
func (l *Local) SetLogger(log log.Modular) {}

// Close stops the Local object from aggregating metrics and cleans up
// resources.
func (l *Local) Close() error {
	return nil
}

//------------------------------------------------------------------------------
