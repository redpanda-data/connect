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

	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

// FlatStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type FlatStat struct {
	path string
	f    Flat
}

// Incr increments a metric by an amount.
func (f *FlatStat) Incr(count int64) error {
	f.f.Incr(f.path, count)
	return nil
}

// Decr decrements a metric by an amount.
func (f *FlatStat) Decr(count int64) error {
	f.f.Decr(f.path, count)
	return nil
}

// Timing sets a timing metric.
func (f *FlatStat) Timing(delta int64) error {
	f.f.Timing(f.path, delta)
	return nil
}

// Gauge sets a gauge metric.
func (f *FlatStat) Gauge(value int64) error {
	f.f.Gauge(f.path, value)
	return nil
}

//------------------------------------------------------------------------------

// WrappedFlat implements the entire Type interface around a Flat type.
type WrappedFlat struct {
	f Flat
}

// WrapFlat creates a Type around a Flat implementation.
func WrapFlat(f Flat) Type {
	return &WrappedFlat{
		f: f,
	}
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *WrappedFlat) GetCounter(path ...string) StatCounter {
	return &FlatStat{
		path: strings.Join(path, "."),
		f:    h.f,
	}
}

// GetTimer returns a stat timer object for a path.
func (h *WrappedFlat) GetTimer(path ...string) StatTimer {
	return &FlatStat{
		path: strings.Join(path, "."),
		f:    h.f,
	}
}

// GetGauge returns a stat gauge object for a path.
func (h *WrappedFlat) GetGauge(path ...string) StatGauge {
	return &FlatStat{
		path: strings.Join(path, "."),
		f:    h.f,
	}
}

// Incr increments a stat by a value.
func (h *WrappedFlat) Incr(stat string, value int64) error {
	return h.f.Incr(stat, value)
}

// Decr decrements a stat by a value.
func (h *WrappedFlat) Decr(stat string, value int64) error {
	return h.f.Decr(stat, value)
}

// Timing sets a stat representing a duration.
func (h *WrappedFlat) Timing(stat string, delta int64) error {
	return h.f.Timing(stat, delta)
}

// Gauge sets a stat as a gauge value.
func (h *WrappedFlat) Gauge(stat string, value int64) error {
	return h.f.Gauge(stat, value)
}

// SetLogger does nothing.
func (h *WrappedFlat) SetLogger(log log.Modular) {
}

// Close stops the WrappedFlat object from aggregating metrics and cleans up
// resources.
func (h *WrappedFlat) Close() error {
	return h.f.Close()
}

//------------------------------------------------------------------------------
