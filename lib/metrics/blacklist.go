package metrics

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
)

// BlacklistConfig allows for the placement of filtering rules to only allow
// metrics that are not matched to be displayed or retrieved. It has a set of
// prefixes (direct string comparison) that are checked as well as a set of
// regular expressions for more precise control over metrics. It also has a
// metrics configuration that is wrapped by the Blacklist.

//------------------------------------------------------------------------------

func init() {
	constructors[TypeBlackList] = typeSpec{
		constructor: NewBlacklist,
		description: `
Blacklist a certain set of paths around a child metric collector.

### Patterns and paths

Blacklists can be one of two options, paths or regular expression patterns.
A metric path's eligibility is strictly additive - it only has to pass a
single path or a single pattern for it to be not included.

An entry in a Blacklist's ` + "`paths`" + `field will check using prefix
matching. This can be used, for example to allow none of the  metrics from the
` + "`output`" + `stats object to be pushed to the child metric collector.

An entry in a Blacklist's ` + "`patterns`" + `field will check using Go's
` + "`regexp.MatchString`" + ` function, so any submatch in the final path will
result in the metric being allowed. To anchor a pattern to the start or end of
the word, you might use the ` + "`^`" + ` or ` + "`$`" + ` regex operators.`,
	}
}

//

//------------------------------------------------------------------------------

// BlacklistConfig is a list of configs...
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
	patterns []string
	s        Type
}

// NewBlacklist creates and returns a new Blacklist object
func NewBlacklist(config Config, opts ...func(Type)) (Type, error) {
	if config.Blacklist.Child == nil {
		return nil, errors.New("cannot create a Blacklist metric without a child")
	}
	if c, ok := constructors[config.Blacklist.Child.Type]; ok {
		child, err := c.constructor(*config.Blacklist.Child, opts...)
		if err != nil {
			return nil, err
		}

		w := &Blacklist{
			paths:    config.Blacklist.Paths,
			patterns: config.Blacklist.Patterns,
			s:        child,
		}

		return w, nil
	}

	return nil, ErrInvalidMetricOutputType
}

//------------------------------------------------------------------------------

// allowPath checks whether or not a given path is in the allowed set of
// paths for the Blacklist metrics stat.
func (h *Blacklist) rejectPath(path string) bool {
	for _, p := range h.paths {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	for _, pat := range h.patterns {
		matched, err := regexp.MatchString(pat, path)
		if err != nil {
			// TODO: how should we really handle this?
			return false
		}
		if matched {
			return true
		}
	}

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
		return fakeCounterVec(func() StatCounter {
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
		return fakeTimerVec(func() StatTimer {
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
		return fakeGaugeVec(func() StatGauge {
			return DudStat{}
		})

	}

	return h.s.GetGaugeVec(path, n)
}

// SetLogger sets the logger used to print connection errors.
func (h *Blacklist) SetLogger(log log.Modular) {
	h.s.SetLogger(log)
}

// Close stops the Statsd object from aggregating metrics and cleans up
// resources.
func (h *Blacklist) Close() error {
	h.s.Close()
	return nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics for appropriate
// child types
func (h *Blacklist) HandlerFunc() http.HandlerFunc {

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if wHandlerFunc, ok := h.s.(WithHandlerFunc); ok {
		return wHandlerFunc.HandlerFunc()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("The child of this Blacklist does not support HTTP metrics."))
	}
}
