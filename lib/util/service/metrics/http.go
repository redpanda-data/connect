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
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

// HTTPStat is a representation of a single metric stat. Interactions with this
// stat are thread safe.
type HTTPStat struct {
	value *int64
}

// Incr increments a metric by an amount.
func (h *HTTPStat) Incr(count int64) error {
	atomic.AddInt64(h.value, count)
	return nil
}

// Decr decrements a metric by an amount.
func (h *HTTPStat) Decr(count int64) error {
	atomic.AddInt64(h.value, -count)
	return nil
}

// Timing sets a timing metric.
func (h *HTTPStat) Timing(delta int64) error {
	atomic.StoreInt64(h.value, delta)
	return nil
}

// Gauge sets a gauge metric.
func (h *HTTPStat) Gauge(value int64) error {
	atomic.StoreInt64(h.value, value)
	return nil
}

//------------------------------------------------------------------------------

// HTTP is an object with capability to hold internal stats as a JSON endpoint.
type HTTP struct {
	flatCounters map[string]*int64
	flatTimings  map[string]*int64
	pathPrefix   string
	timestamp    time.Time

	sync.Mutex
}

// NewHTTP creates and returns a new HTTP object.
func NewHTTP(config Config) (Type, error) {
	t := &HTTP{
		flatCounters: map[string]*int64{},
		flatTimings:  map[string]*int64{},
		pathPrefix:   config.Prefix,
		timestamp:    time.Now(),
	}

	return t, nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics as a JSON blob.
func (h *HTTP) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Since(h.timestamp).String()
		goroutines := runtime.NumGoroutine()

		h.Lock()
		localFlatCounters := make(map[string]int64, len(h.flatCounters))
		for k := range h.flatCounters {
			localFlatCounters[k] = atomic.LoadInt64(h.flatCounters[k])
		}

		localFlatTimings := make(map[string]int64, len(h.flatTimings))
		for k := range h.flatTimings {
			localFlatTimings[k] = atomic.LoadInt64(h.flatTimings[k])
		}
		h.Unlock()

		obj := gabs.New()
		for k, v := range localFlatCounters {
			obj.SetP(v, k)
		}
		for k, v := range localFlatTimings {
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
	dotPath := strings.Join(path, ".")

	h.Lock()
	ptr, exists := h.flatCounters[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		h.flatCounters[dotPath] = ptr
	}
	h.Unlock()

	return &HTTPStat{
		value: ptr,
	}
}

// GetTimer returns a stat timer object for a path.
func (h *HTTP) GetTimer(path ...string) StatTimer {
	dotPath := strings.Join(path, ".")

	h.Lock()
	ptr, exists := h.flatTimings[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		h.flatTimings[dotPath] = ptr
	}
	h.Unlock()

	return &HTTPStat{
		value: ptr,
	}
}

// GetGauge returns a stat gauge object for a path.
func (h *HTTP) GetGauge(path ...string) StatGauge {
	dotPath := strings.Join(path, ".")

	h.Lock()
	ptr, exists := h.flatCounters[dotPath]
	if !exists {
		var ctr int64
		ptr = &ctr
		h.flatCounters[dotPath] = ptr
	}
	h.Unlock()

	return &HTTPStat{
		value: ptr,
	}
}

// Incr increments a stat by a value.
func (h *HTTP) Incr(stat string, value int64) error {
	h.Lock()
	if ptr, exists := h.flatCounters[stat]; !exists {
		ctr := value
		h.flatCounters[stat] = &ctr
	} else {
		atomic.AddInt64(ptr, value)
	}
	h.Unlock()
	return nil
}

// Decr decrements a stat by a value.
func (h *HTTP) Decr(stat string, value int64) error {
	h.Lock()
	if ptr, exists := h.flatCounters[stat]; !exists {
		ctr := -value
		h.flatCounters[stat] = &ctr
	} else {
		atomic.AddInt64(ptr, -value)
	}
	h.Unlock()
	return nil
}

// Timing sets a stat representing a duration.
func (h *HTTP) Timing(stat string, delta int64) error {
	h.Lock()
	if ptr, exists := h.flatTimings[stat]; !exists {
		ctr := delta
		h.flatTimings[stat] = &ctr
	} else {
		atomic.StoreInt64(ptr, delta)
	}
	h.Unlock()
	return nil
}

// Gauge sets a stat as a gauge value.
func (h *HTTP) Gauge(stat string, value int64) error {
	h.Lock()
	if ptr, exists := h.flatCounters[stat]; !exists {
		ctr := value
		h.flatTimings[stat] = &ctr
	} else {
		atomic.StoreInt64(ptr, value)
	}
	h.Unlock()
	return nil
}

// Close stops the HTTP object from aggregating metrics and cleans up resources.
func (h *HTTP) Close() error {
	return nil
}

//------------------------------------------------------------------------------
