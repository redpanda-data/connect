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

	"github.com/Jeffail/benthos/lib/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRename] = TypeSpec{
		constructor: NewRename,
		description: `
Rename metric paths as they are registered.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

The ` + "`prefix`" + ` field in a metrics config is ignored by this type. Please
configure a prefix at the child level.

### ` + "`by_regexp`" + `

An array of objects of the form ` + "`{\"pattern\":\"foo\",\"value\":\"bar\"}`" + `
where each pattern will be parsed as RE2 regular expressions, these expressions
are tested against each metric path, where all occurrences will be replaced with
the specified value. Inside the value $ signs are interpreted as submatch
expansions, e.g. $1 represents the first submatch.

To replace the paths 'foo.bar.zap' and 'foo.baz.zap' with 'zip.bar' and
'zip.baz' respectively we could use this config:

` + "``` yaml" + `
rename:
  by_regexp:
  - pattern: "foo\\.([a-z]*)\\.zap"
    value: "zip.$1"
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var childSanit interface{}
			var err error
			if conf.Rename.Child != nil {
				if childSanit, err = SanitiseConfig(*conf.Rename.Child); err != nil {
					return nil, err
				}
			} else {
				childSanit = struct{}{}
			}
			return map[string]interface{}{
				"by_regexp": conf.Rename.ByRegexp,
				"child":     childSanit,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// RenameByRegexpConfig contains config fields for a rename by regular
// expression pattern.
type RenameByRegexpConfig struct {
	Pattern string `json:"pattern" yaml:"pattern"`
	Value   string `json:"value" yaml:"value"`
}

// RenameConfig contains config fields for the Rename metric type.
type RenameConfig struct {
	ByRegexp []RenameByRegexpConfig `json:"by_regexp" yaml:"by_regexp"`
	Child    *Config                `json:"child" yaml:"child"`
}

// NewRenameConfig returns a RenameConfig with default values.
func NewRenameConfig() RenameConfig {
	return RenameConfig{
		ByRegexp: []RenameByRegexpConfig{},
		Child:    nil,
	}
}

//------------------------------------------------------------------------------

type dummyRenameConfig struct {
	ByRegexp []RenameByRegexpConfig `json:"by_regexp" yaml:"by_regexp"`
	Child    interface{}            `json:"child" yaml:"child"`
}

// MarshalJSON prints an empty object instead of nil.
func (w RenameConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyRenameConfig{
		ByRegexp: w.ByRegexp,
		Child:    w.Child,
	}
	if w.Child == nil {
		dummy.Child = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (w RenameConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyRenameConfig{
		ByRegexp: w.ByRegexp,
		Child:    w.Child,
	}
	if w.Child == nil {
		dummy.Child = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

type renameByRegexp struct {
	expression *regexp.Regexp
	value      string
}

// Rename is a statistics object that wraps a separate statistics object
// and only permits statistics that pass through the whitelist to be recorded.
type Rename struct {
	byRegexp []renameByRegexp
	s        Type
}

// NewRename creates and returns a new Rename object
func NewRename(config Config, opts ...func(Type)) (Type, error) {
	if config.Rename.Child == nil {
		return nil, errors.New("cannot create a rename metric without a child")
	}

	child, err := New(*config.Rename.Child, opts...)
	if err != nil {
		return nil, err
	}

	r := &Rename{
		s: child,
	}

	for _, p := range config.Rename.ByRegexp {
		re, err := regexp.Compile(p.Pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression: '%s': %v", p, err)
		}
		r.byRegexp = append(r.byRegexp, renameByRegexp{
			expression: re,
			value:      p.Value,
		})
	}

	return r, nil
}

//------------------------------------------------------------------------------

// renamePath checks whether or not a given path is in the allowed set of
// paths for the Rename metrics stat.
func (r *Rename) renamePath(path string) string {
	for _, rr := range r.byRegexp {
		path = rr.expression.ReplaceAllString(path, rr.value)
	}
	return path
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (r *Rename) GetCounter(path string) StatCounter {
	return r.s.GetCounter(r.renamePath(path))
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (r *Rename) GetCounterVec(path string, n []string) StatCounterVec {
	return r.s.GetCounterVec(r.renamePath(path), n)
}

// GetTimer returns a stat timer object for a path.
func (r *Rename) GetTimer(path string) StatTimer {
	return r.s.GetTimer(r.renamePath(path))
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (r *Rename) GetTimerVec(path string, n []string) StatTimerVec {
	return r.s.GetTimerVec(r.renamePath(path), n)
}

// GetGauge returns a stat gauge object for a path.
func (r *Rename) GetGauge(path string) StatGauge {
	return r.s.GetGauge(r.renamePath(path))
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (r *Rename) GetGaugeVec(path string, n []string) StatGaugeVec {
	return r.s.GetGaugeVec(r.renamePath(path), n)
}

// SetLogger sets the logger used to print connection errors.
func (r *Rename) SetLogger(log log.Modular) {
	r.s.SetLogger(log)
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (r *Rename) Close() error {
	return r.s.Close()
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics for appropriate
// child types
func (r *Rename) HandlerFunc() http.HandlerFunc {
	if wHandlerFunc, ok := r.s.(WithHandlerFunc); ok {
		return wHandlerFunc.HandlerFunc()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(501)
		w.Write([]byte("The child of this rename does not support HTTP metrics."))
	}
}

//------------------------------------------------------------------------------
