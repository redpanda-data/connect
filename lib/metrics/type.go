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

import (
	"net/http"

	"github.com/Jeffail/benthos/v3/lib/log"
)

//------------------------------------------------------------------------------

// StatCounter is a representation of a single counter metric stat. Interactions
// with this stat are thread safe.
type StatCounter interface {
	// Incr increments a counter by an amount.
	Incr(count int64) error
}

// StatTimer is a representation of a single timer metric stat. Interactions
// with this stat are thread safe.
type StatTimer interface {
	// Timing sets a timing metric.
	Timing(delta int64) error
}

// StatGauge is a representation of a single gauge metric stat. Interactions
// with this stat are thread safe.
type StatGauge interface {
	// Set sets the value of a gauge metric.
	Set(value int64) error

	// Incr increments a gauge by an amount.
	Incr(count int64) error

	// Decr decrements a gauge by an amount.
	Decr(count int64) error
}

//------------------------------------------------------------------------------

// StatCounterVec creates StatCounters with dynamic labels.
type StatCounterVec interface {
	// With returns a StatCounter with a set of label values.
	With(labelValues ...string) StatCounter
}

// StatTimerVec creates StatTimers with dynamic labels.
type StatTimerVec interface {
	// With returns a StatTimer with a set of label values.
	With(labelValues ...string) StatTimer
}

// StatGaugeVec creates StatGauges with dynamic labels.
type StatGaugeVec interface {
	// With returns a StatGauge with a set of label values.
	With(labelValues ...string) StatGauge
}

//------------------------------------------------------------------------------

// Type is an interface for metrics aggregation.
type Type interface {
	// GetCounter returns an editable counter stat for a given path.
	GetCounter(path string) StatCounter

	// GetCounterVec returns an editable counter stat for a given path with
	// labels, these labels must be consistent with any other metrics registered
	// on the same path.
	GetCounterVec(path string, labelNames []string) StatCounterVec

	// GetTimer returns an editable timer stat for a given path.
	GetTimer(path string) StatTimer

	// GetTimerVec returns an editable timer stat for a given path with labels,
	// these labels must be consistent with any other metrics registered on the
	// same path.
	GetTimerVec(path string, labelNames []string) StatTimerVec

	// GetGauge returns an editable gauge stat for a given path.
	GetGauge(path string) StatGauge

	// GetGaugeVec returns an editable gauge stat for a given path with labels,
	// these labels must be consistent with any other metrics registered on the
	// same path.
	GetGaugeVec(path string, labelNames []string) StatGaugeVec

	// SetLogger sets the logging mechanism of the metrics type.
	SetLogger(log log.Modular)

	// Close stops aggregating stats and cleans up resources.
	Close() error
}

//------------------------------------------------------------------------------

// WithHandlerFunc is an interface for metrics types that can expose their
// metrics through an HTTP HandlerFunc endpoint. If a Type can be cast into
// WithHandlerFunc then you should register its endpoint to the an HTTP server.
type WithHandlerFunc interface {
	HandlerFunc() http.HandlerFunc
}

//------------------------------------------------------------------------------
