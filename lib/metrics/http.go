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
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	constructors["http_server"] = typeSpec{
		constructor: NewHTTP,
		description: `
Benthos can host its own stats endpoint, where a GET request will receive a JSON
blob of all metrics tracked within Benthos.`,
	}
}

//------------------------------------------------------------------------------

// Errors for the HTTP type.
var (
	ErrTimedOut = errors.New("timed out")
)

//------------------------------------------------------------------------------

// HTTP is an object with capability to hold internal stats as a JSON endpoint.
type HTTP struct {
	local      *Local
	timestamp  time.Time
	pathPrefix string
}

// NewHTTP creates and returns a new HTTP object.
func NewHTTP(config Config, opts ...func(Type)) (Type, error) {
	t := &HTTP{
		local:      NewLocal(),
		timestamp:  time.Now(),
		pathPrefix: config.Prefix,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics as a JSON blob.
func (h *HTTP) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Since(h.timestamp).String()
		goroutines := runtime.NumGoroutine()

		counters := h.local.GetCounters()
		timings := h.local.GetTimings()

		obj := gabs.New()
		for k, v := range counters {
			obj.SetP(v, k)
		}
		for k, v := range timings {
			obj.SetP(v, k)
			obj.SetP(time.Duration(v).String(), k+"_readable")
		}
		obj.SetP(fmt.Sprintf("%v", uptime), "uptime")
		obj.SetP(goroutines, "goroutines")

		if len(h.pathPrefix) > 0 {
			rootObj := gabs.New()
			rootObj.SetP(obj.Data(), h.pathPrefix)
			obj = rootObj
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(obj.Bytes())
	}
}

// GetCounter returns a stat counter object for a path.
func (h *HTTP) GetCounter(path ...string) StatCounter {
	return h.local.GetCounter(path...)
}

// GetTimer returns a stat timer object for a path.
func (h *HTTP) GetTimer(path ...string) StatTimer {
	return h.local.GetTimer(path...)
}

// GetGauge returns a stat gauge object for a path.
func (h *HTTP) GetGauge(path ...string) StatGauge {
	return h.local.GetGauge(path...)
}

// Incr increments a stat by a value.
func (h *HTTP) Incr(stat string, value int64) error {
	return h.local.Incr(stat, value)
}

// Decr decrements a stat by a value.
func (h *HTTP) Decr(stat string, value int64) error {
	return h.local.Decr(stat, value)
}

// Timing sets a stat representing a duration.
func (h *HTTP) Timing(stat string, delta int64) error {
	return h.local.Timing(stat, delta)
}

// Gauge sets a stat as a gauge value.
func (h *HTTP) Gauge(stat string, value int64) error {
	return h.local.Gauge(stat, value)
}

// SetLogger does nothing.
func (h *HTTP) SetLogger(log log.Modular) {}

// Close stops the HTTP object from aggregating metrics and cleans up resources.
func (h *HTTP) Close() error {
	return nil
}

//------------------------------------------------------------------------------
