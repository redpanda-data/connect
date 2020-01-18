package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWhiteList] = TypeSpec{
		constructor: NewWhitelist,
		Description: `
Whitelist metric paths within Benthos so that only matching metric paths are
aggregated by a child metric target.

Whitelists can either be path prefixes or regular expression patterns, if either
a path prefix or regular expression matches a metric path it will be included.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

### Paths

An entry in the ` + "`paths`" + ` field will check using prefix matching. This
can be used, for example, to allow the child specific metrics paths from an
output broker with the path ` + "`output.broker`" + `.

### Patterns

An entry in the ` + "`patterns`" + ` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow all latency based metrics with the pattern ` + "`.*\\.latency`" + `.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
pass your whitelists enable logging at the TRACE level.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var childSanit interface{}
			var err error
			if conf.Whitelist.Child != nil {
				if childSanit, err = SanitiseConfig(*conf.Whitelist.Child); err != nil {
					return nil, err
				}
			} else {
				childSanit = struct{}{}
			}
			return map[string]interface{}{
				"paths":    conf.Whitelist.Paths,
				"patterns": conf.Whitelist.Patterns,
				"child":    childSanit,
			}, nil
		},
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
	log      log.Modular
}

// NewWhitelist creates and returns a new Whitelist object
func NewWhitelist(config Config, opts ...func(Type)) (Type, error) {
	if config.Whitelist.Child == nil {
		return nil, errors.New("cannot create a whitelist metric without a child")
	}

	child, err := New(*config.Whitelist.Child)
	if err != nil {
		return nil, err
	}

	w := &Whitelist{
		paths:    config.Whitelist.Paths,
		patterns: make([]*regexp.Regexp, len(config.Whitelist.Patterns)),
		s:        child,
		log:      log.Noop(),
	}

	for _, opt := range opts {
		opt(w)
	}

	for i, p := range config.Whitelist.Patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression: '%s': %v", p, err)
		}
		w.patterns[i] = re
	}

	return w, nil
}

//------------------------------------------------------------------------------

// allowPath checks whether or not a given path is in the allowed set of
// paths for the Whitelist metrics stat.
func (h *Whitelist) allowPath(path string) bool {
	for _, p := range h.paths {
		if strings.HasPrefix(path, p) {
			h.log.Tracef("Allowing metric path '%v' as per whitelisted path prefix '%v'\n", path, p)
			return true
		}
	}
	for _, re := range h.patterns {
		if re.MatchString(path) {
			h.log.Tracef("Allowing metric path '%v' as per whitelisted pattern '%v'\n", path, re.String())
			return true
		}
	}
	h.log.Tracef("Rejecting metric path '%v'\n", path)
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
	return fakeCounterVec(func([]string) StatCounter {
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
	return fakeTimerVec(func([]string) StatTimer {
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
	return fakeGaugeVec(func([]string) StatGauge {
		return DudStat{}
	})
}

// SetLogger sets the logger used to print connection errors.
func (h *Whitelist) SetLogger(log log.Modular) {
	h.log = log.NewModule(".whitelist")
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
		w.Write([]byte("The child of this whitelist does not support HTTP metrics."))
	}
}

//------------------------------------------------------------------------------
