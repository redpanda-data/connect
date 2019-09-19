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

	"github.com/Jeffail/benthos/v3/lib/log"
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

### ` + "`by_regexp`" + `

An array of objects of the form:

` + "```yaml" + `
  - pattern: "foo\\.([a-z]*)\\.([a-z]*)"
    value: "foo.$1"
    to_label:
      bar: $2
` + "```" + `

Where each pattern will be parsed as an RE2 regular expression, these
expressions are tested against each metric path, where all occurrences will be
replaced with the specified value. Inside the value $ signs are interpreted as
submatch expansions, e.g. $1 represents the first submatch.

The field ` + "`to_label`" + ` may contain any number of key/value pairs to be
added to a metric as labels, where the value may contain submatches from the
provided pattern. This allows you to extract (left-most) matched segments of the
renamed path into the label values.

For example, in order to replace the paths 'foo.bar.0.zap' and 'foo.baz.1.zap'
with 'zip.bar' and 'zip.baz' respectively, and store the respective values '0'
and '1' under the label key 'index' we could use this config:

` + "```yaml" + `
rename:
  by_regexp:
  - pattern: "foo\\.([a-z]*)\\.([a-z]*)\\.zap"
    value: "zip.$1"
    to_label:
      index: $2
` + "```" + `

These labels will only be injected into metrics registered without pre-existing
labels. Therefore it's currently not possible to combine labels registered from
the ` + "[`metric` processor](../processors/README.md#metric)" + ` with labels
set via renaming.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
are renamed enable logging at the TRACE level.`,
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
	Pattern string            `json:"pattern" yaml:"pattern"`
	Value   string            `json:"value" yaml:"value"`
	Labels  map[string]string `json:"to_label" yaml:"to_label"`
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
	labels     map[string]string
}

// Rename is a statistics object that wraps a separate statistics object
// and only permits statistics that pass through the whitelist to be recorded.
type Rename struct {
	byRegexp []renameByRegexp
	s        Type
	log      log.Modular
}

// NewRename creates and returns a new Rename object
func NewRename(config Config, opts ...func(Type)) (Type, error) {
	if config.Rename.Child == nil {
		return nil, errors.New("cannot create a rename metric without a child")
	}

	child, err := New(*config.Rename.Child)
	if err != nil {
		return nil, err
	}

	r := &Rename{
		s:   child,
		log: log.Noop(),
	}

	for _, opt := range opts {
		opt(r)
	}

	for _, p := range config.Rename.ByRegexp {
		re, err := regexp.Compile(p.Pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression: '%s': %v", p, err)
		}
		r.byRegexp = append(r.byRegexp, renameByRegexp{
			expression: re,
			value:      p.Value,
			labels:     p.Labels,
		})
	}

	return r, nil
}

//------------------------------------------------------------------------------

// renamePath checks whether or not a given path is in the allowed set of
// paths for the Rename metrics stat.
func (r *Rename) renamePath(path string) (string, map[string]string) {
	renamed := false
	labels := map[string]string{}
	for _, rr := range r.byRegexp {
		newPath := rr.expression.ReplaceAllString(path, rr.value)
		if newPath != path {
			renamed = true
			r.log.Tracef("Renamed metric path '%v' to '%v' as per regexp '%v'\n", path, newPath, rr.expression.String())
		}
		if rr.labels != nil && len(rr.labels) > 0 {
			// Extract only the matching segment of the path (left-most)
			leftPath := rr.expression.FindString(path)
			if len(leftPath) > 0 {
				for k, v := range rr.labels {
					v = rr.expression.ReplaceAllString(leftPath, v)
					labels[k] = v
					r.log.Tracef("Renamed label '%v' to '%v' as per regexp '%v'\n", k, v, rr.expression.String())
				}
			}
		}
		path = newPath
	}
	if !renamed {
		r.log.Tracef("Registered metric path '%v' unchanged\n", path)
	}
	return path, labels
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (r *Rename) GetCounter(path string) StatCounter {
	rpath, labels := r.renamePath(path)
	if len(labels) == 0 {
		return r.s.GetCounter(rpath)
	}
	names, values := make([]string, 0, len(labels)), make([]string, 0, len(labels))
	for k, v := range labels {
		names = append(names, k)
		values = append(values, v)
	}
	return r.s.GetCounterVec(rpath, names).With(values...)
}

// GetCounterVec returns a stat counter object for a path with the labels
// and values.
func (r *Rename) GetCounterVec(path string, n []string) StatCounterVec {
	rpath, _ := r.renamePath(path)
	return r.s.GetCounterVec(rpath, n)
}

// GetTimer returns a stat timer object for a path.
func (r *Rename) GetTimer(path string) StatTimer {
	rpath, labels := r.renamePath(path)
	if len(labels) == 0 {
		return r.s.GetTimer(rpath)
	}

	names, values := make([]string, 0, len(labels)), make([]string, 0, len(labels))
	for k, v := range labels {
		names = append(names, k)
		values = append(values, v)
	}
	return r.s.GetTimerVec(rpath, names).With(values...)
}

// GetTimerVec returns a stat timer object for a path with the labels
// and values.
func (r *Rename) GetTimerVec(path string, n []string) StatTimerVec {
	rpath, _ := r.renamePath(path)
	return r.s.GetTimerVec(rpath, n)
}

// GetGauge returns a stat gauge object for a path.
func (r *Rename) GetGauge(path string) StatGauge {
	rpath, labels := r.renamePath(path)
	if len(labels) == 0 {
		return r.s.GetGauge(rpath)
	}

	names, values := make([]string, 0, len(labels)), make([]string, 0, len(labels))
	for k, v := range labels {
		names = append(names, k)
		values = append(values, v)
	}
	return r.s.GetGaugeVec(rpath, names).With(values...)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (r *Rename) GetGaugeVec(path string, n []string) StatGaugeVec {
	rpath, _ := r.renamePath(path)
	return r.s.GetGaugeVec(rpath, n)
}

// SetLogger sets the logger used to print connection errors.
func (r *Rename) SetLogger(log log.Modular) {
	r.log = log.NewModule(".rename")
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
