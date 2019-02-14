// Copyright (c) 2019 Daniel Rubenstein
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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

func init() {
	constructors[TypeWhiteList] = typeSpec{
		constructor: NewWhitelist,
		description: `
Whitelist a certain set of paths around a child metric collector.

### Patterns and paths

Whitelists can be one of two options, paths or regular expression patterns.
A metric path's eligibility is strictly additive - it only has to pass a
single path or a single pattern for it to be included.

An entry in a Whitelist's ` + "`paths`" + `field will check using prefix
matching. This can be used, for example to allow all metrics from the ` +
			"`output`" + `stats object to be pushed to the child metric collector.

An entry in a Whitelist's ` + "`patterns`" + `field will check using Go's
` + "`regexp.MatchString`" + ` function, so any submatch in the final path will
result in the metric being allowed. To anchor a pattern to the start or end of
the word, you might use the ` + "`^`" + ` or ` + "`$`" + ` regex operators.`,
	}
}

//------------------------------------------------------------------------------

// WhitelistConfig allows for the placement of filtering rules to only allow
// select metrics to be displayed or retrieved. It consists of a set of
// prefixes (direct string comparison) that are checked, and a standard
// metrics configuration that is wrapped by the whitelist.
type WhitelistConfig struct {
	Paths    []string `json:"paths" yaml:"paths"`
	Patterns []string `json:"patterns" yaml:"patterns"`
	Child    *Config  `json:"child" yaml:"child"`
}

// NewWhitelistConfig returns the default configuration for a whitelist
func NewWhitelistConfig() WhitelistConfig {
	return WhitelistConfig{
		Paths:    []string{},
		Patterns: []string{},
		Child:    nil,
	}
}

//------------------------------------------------------------------------------

type dummyWhitelistConfig struct {
	Paths    []string    `json:"paths" yaml:"paths"`
	Patterns []string    `json:"patterns" yaml:"patterns"`
	Child    interface{} `json:"child" yaml:"child"`
}

// MarshalJSON prints an empty object instead of nil.
func (w WhitelistConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyWhitelistConfig{
		Paths:    w.Paths,
		Patterns: w.Patterns,
		Child:    w.Child,
	}

	if w.Child == nil {
		dummy.Child = struct{}{}
	}

	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (w WhitelistConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyWhitelistConfig{
		Paths:    w.Paths,
		Patterns: w.Patterns,
		Child:    w.Child,
	}
	if w.Child == nil {
		dummy.Child = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// Whitelist is a statistics object that wraps a separate statistics object
// and only permits statistics that pass through the whitelist to be recorded.
type Whitelist struct {
	paths    []string
	patterns []*regexp.Regexp
	s        Type
}

// NewWhitelist creates and returns a new Whitelist object
func NewWhitelist(config Config, opts ...func(Type)) (Type, error) {
	if config.Whitelist.Child == nil {
		return nil, errors.New("cannot create a whitelist metric without a child")
	}
	if _, ok := constructors[config.Whitelist.Child.Type]; ok {
		child, err := New(*config.Whitelist.Child, opts...)
		if err != nil {
			return nil, err
		}

		w := &Whitelist{
			paths: config.Whitelist.Paths,
			s:     child,
		}
		w.patterns = make([]*regexp.Regexp, len(config.Whitelist.Patterns))
		for i, p := range config.Whitelist.Patterns {
			re, err := regexp.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("Invalid regular expression: '%s': %v", p, err)
			}
			w.patterns[i] = re
		}

		return w, nil
	}

	return nil, ErrInvalidMetricOutputType
}

//------------------------------------------------------------------------------

// allowPath checks whether or not a given path is in the allowed set of
// paths for the Whitelist metrics stat.
func (h *Whitelist) allowPath(path string) bool {
	for _, p := range h.paths {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	for _, re := range h.patterns {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *Whitelist) GetCounter(path string) StatCounter {
	if h.allowPath(path) {
		return h.s.GetCounter(path)
	}
	return DudStat{}
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *Whitelist) GetCounterVec(path string, n []string) StatCounterVec {
	if h.allowPath(path) {
		return h.s.GetCounterVec(path, n)
	}
	return fakeCounterVec(func() StatCounter {
		return DudStat{}
	})
}

// GetTimer returns a stat timer object for a path.
func (h *Whitelist) GetTimer(path string) StatTimer {
	if h.allowPath(path) {
		return h.s.GetTimer(path)
	}
	return DudStat{}
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *Whitelist) GetTimerVec(path string, n []string) StatTimerVec {
	if h.allowPath(path) {
		return h.s.GetTimerVec(path, n)
	}
	return fakeTimerVec(func() StatTimer {
		return DudStat{}
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *Whitelist) GetGauge(path string) StatGauge {
	if h.allowPath(path) {
		return h.s.GetGauge(path)
	}
	return DudStat{}
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *Whitelist) GetGaugeVec(path string, n []string) StatGaugeVec {
	if h.allowPath(path) {
		return h.s.GetGaugeVec(path, n)
	}
	return fakeGaugeVec(func() StatGauge {
		return DudStat{}
	})
}

// SetLogger sets the logger used to print connection errors.
func (h *Whitelist) SetLogger(log log.Modular) {
	h.s.SetLogger(log)
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (h *Whitelist) Close() error {
	return h.s.Close()
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics for appropriate
// child types
func (h *Whitelist) HandlerFunc() http.HandlerFunc {
	if wHandlerFunc, ok := h.s.(WithHandlerFunc); ok {
		return wHandlerFunc.HandlerFunc()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(501)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("The child of this whitelist does not support HTTP metrics."))
	}
}
