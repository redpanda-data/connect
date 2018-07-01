// Copyright (c) 2014 Ashley Jeffs
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

import "github.com/Jeffail/benthos/lib/log"

//------------------------------------------------------------------------------

// DudStat implements the Stat interface but doesn't actual do anything.
type DudStat struct{}

// Incr does nothing.
func (d DudStat) Incr(count int64) error { return nil }

// Decr does nothing.
func (d DudStat) Decr(count int64) error { return nil }

// Timing does nothing.
func (d DudStat) Timing(delta int64) error { return nil }

// Gauge does nothing.
func (d DudStat) Gauge(value int64) error { return nil }

//------------------------------------------------------------------------------

// DudType implements the Type interface but doesn't actual do anything.
type DudType struct {
	ID int
}

// Noop returns a DudType for discarding metrics.
func Noop() DudType {
	return DudType{}
}

// GetCounter returns a DudStat.
func (d DudType) GetCounter(path ...string) StatCounter { return DudStat{} }

// GetTimer returns a DudStat.
func (d DudType) GetTimer(path ...string) StatTimer { return DudStat{} }

// GetGauge returns a DudStat.
func (d DudType) GetGauge(path ...string) StatGauge { return DudStat{} }

// SetLogger does nothing.
func (d DudType) SetLogger(log log.Modular) {}

// Incr does nothing.
func (d DudType) Incr(path string, count int64) error { return nil }

// Decr does nothing.
func (d DudType) Decr(path string, count int64) error { return nil }

// Timing does nothing.
func (d DudType) Timing(path string, delta int64) error { return nil }

// Gauge does nothing.
func (d DudType) Gauge(path string, value int64) error { return nil }

// Close does nothing.
func (d DudType) Close() error { return nil }

//------------------------------------------------------------------------------
