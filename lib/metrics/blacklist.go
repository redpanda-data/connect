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
	Constructors[TypeBlackList] = TypeSpec{
		constructor: NewBlacklist,
		Description: `
Blacklist metric paths within Benthos so that they are not aggregated by a child
metric target.

Blacklists can either be path prefixes or regular expression patterns, if either
a path prefix or regular expression matches a metric path it will be excluded.

Metrics must be matched using dot notation even if the chosen output uses a
different form. For example, the path would be 'foo.bar' rather than 'foo_bar'
even when sending metrics to Prometheus.

### Paths

An entry in the ` + "`paths`" + ` field will check using prefix matching. This
can be used, for example, to allow none of the child specific metrics paths from
an output broker with the path ` + "`output.broker`" + `.

### Patterns

An entry in the ` + "`patterns`" + ` field will be parsed as an RE2 regular
expression and tested against each metric path. This can be used, for example,
to allow none of the latency based metrics with the pattern
` + "`.*\\.latency`" + `.

### Debugging

In order to see logs breaking down which metrics are registered and whether they
are blocked by your blacklists enable logging at the TRACE level.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var childSanit interface{}
			var err error
			if conf.Blacklist.Child != nil {
				if childSanit, err = SanitiseConfig(*conf.Blacklist.Child); err != nil {
					return nil, err
				}
			} else {
				childSanit = struct{}{}
			}
			return map[string]interface{}{
				"paths":    conf.Blacklist.Paths,
				"patterns": conf.Blacklist.Patterns,
				"child":    childSanit,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// BlacklistConfig allows for the placement of filtering rules to only allow
// metrics that are not matched to be displayed or retrieved. It has a set of
// prefixes (direct string comparison) that are checked as well as a set of
// regular expressions for more precise control over metrics. It also has a
// metrics configuration that is wrapped by the Blacklist.
type BlacklistConfig struct {
	Paths    []string `json:"paths" yaml:"paths"`
	Patterns []string `json:"patterns" yaml:"patterns"`
	Child    *Config  `json:"child" yaml:"child"`
}

// NewBlacklistConfig returns the default configuration for a Blacklist
func NewBlacklistConfig() BlacklistConfig {
	return BlacklistConfig{
		Paths:    []string{},
		Patterns: []string{},
		Child:    nil,
	}
}

//------------------------------------------------------------------------------

type dummyBlacklistConfig struct {
	Paths    []string    `json:"paths" yaml:"paths"`
	Patterns []string    `json:"patterns" yaml:"patterns"`
	Child    interface{} `json:"child" yaml:"child"`
}

// MarshalJSON prints an empty object instead of nil.
func (w BlacklistConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyBlacklistConfig{
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
func (w BlacklistConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyBlacklistConfig{
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

// Blacklist is a statistics object that wraps a separate statistics object
// and only permits statistics that pass through the Blacklist to be recorded.
type Blacklist struct {
	paths    []string
	patterns []*regexp.Regexp
	s        Type
	log      log.Modular
}

// NewBlacklist creates and returns a new Blacklist object
func NewBlacklist(config Config, opts ...func(Type)) (Type, error) {
	if config.Blacklist.Child == nil {
		return nil, errors.New("cannot create a Blacklist metric without a child")
	}

	child, err := New(*config.Blacklist.Child)
	if err != nil {
		return nil, err
	}

	b := &Blacklist{
		paths:    config.Blacklist.Paths,
		patterns: make([]*regexp.Regexp, len(config.Blacklist.Patterns)),
		s:        child,
		log:      log.Noop(),
	}

	for _, opt := range opts {
		opt(b)
	}

	for i, p := range config.Blacklist.Patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression: '%s': %v", p, err)
		}
		b.patterns[i] = re
	}

	return b, nil
}

//------------------------------------------------------------------------------

// allowPath checks whether or not a given path is in the allowed set of
// paths for the Blacklist metrics stat.
func (h *Blacklist) rejectPath(path string) bool {
	for _, p := range h.paths {
		if strings.HasPrefix(path, p) {
			h.log.Tracef("Rejecting metric path '%v' according to blacklisted path prefix '%v'\n", path, p)
			return true
		}
	}
	for _, pat := range h.patterns {
		if pat.MatchString(path) {
			h.log.Tracef("Rejecting metric path '%v' according to blacklisted pattern '%v'\n", path, pat.String())
			return true
		}
	}
	h.log.Tracef("Allowing metric path '%v'\n", path)
	return false
}

//------------------------------------------------------------------------------

// GetCounter returns a stat counter object for a path.
func (h *Blacklist) GetCounter(path string) StatCounter {
	if h.rejectPath(path) {
		return DudStat{}
	}
	return h.s.GetCounter(path)
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *Blacklist) GetCounterVec(path string, n []string) StatCounterVec {
	if h.rejectPath(path) {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	return h.s.GetCounterVec(path, n)
}

// GetTimer returns a stat timer object for a path.
func (h *Blacklist) GetTimer(path string) StatTimer {
	if h.rejectPath(path) {
		return DudStat{}
	}
	return h.s.GetTimer(path)
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *Blacklist) GetTimerVec(path string, n []string) StatTimerVec {
	if h.rejectPath(path) {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	return h.s.GetTimerVec(path, n)
}

// GetGauge returns a stat gauge object for a path.
func (h *Blacklist) GetGauge(path string) StatGauge {
	if h.rejectPath(path) {
		return DudStat{}
	}
	return h.s.GetGauge(path)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *Blacklist) GetGaugeVec(path string, n []string) StatGaugeVec {
	if h.rejectPath(path) {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	return h.s.GetGaugeVec(path, n)
}

// SetLogger sets the logger used to print connection errors.
func (h *Blacklist) SetLogger(log log.Modular) {
	h.log = log.NewModule(".blacklist")
	h.s.SetLogger(log)
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (h *Blacklist) Close() error {
	return h.s.Close()
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics for appropriate
// child types
func (h *Blacklist) HandlerFunc() http.HandlerFunc {
	if wHandlerFunc, ok := h.s.(WithHandlerFunc); ok {
		return wHandlerFunc.HandlerFunc()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(501)
		w.Write([]byte("The child of this Blacklist does not support HTTP metrics."))
	}
}

//------------------------------------------------------------------------------
