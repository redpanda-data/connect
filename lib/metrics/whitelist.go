package metrics

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
)

// WhitelistConfig allows for the placement of filtering rules to only allow
// select metrics to be displayed or retrieved. It consists of a set of
// prefixes (direct string comparison) that are checked, and a standard
// metrics configuration that is wrapped by the whitelist.

//------------------------------------------------------------------------------

func init() {
	constructors[TypeWhiteList] = typeSpec{
		constructor: NewWhitelist,
		description: `
Whitelist metrics

Extensive docs to come.`,
	}
}

//

//------------------------------------------------------------------------------

// WhitelistConfig is a list of configs...
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
		Paths: w.Paths,
		Child: w.Child,
	}

	if w.Child == nil {
		dummy.Child = struct{}{}
	}

	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (w WhitelistConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyWhitelistConfig{
		Paths: w.Paths,
		Child: w.Child,
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
	patterns []string
	s        Type
}

// NewWhitelist creates and returns a new Whitelist object
func NewWhitelist(config Config, opts ...func(Type)) (Type, error) {
	if config.Whitelist.Child == nil {
		return nil, errors.New("cannot create a whitelist metric without a child")
	}
	if c, ok := constructors[config.Whitelist.Child.Type]; ok {
		child, err := c.constructor(*config.Whitelist.Child, opts...)
		if err != nil {
			return nil, err
		}

		w := &Whitelist{
			paths:    config.Whitelist.Paths,
			patterns: config.Whitelist.Patterns,
			s:        child,
		}

		return w, nil
	}

	return nil, ErrInvalidMetricOutputType
}

//------------------------------------------------------------------------------

// allowPath checks whether or not a given path is in the allowed set of
// paths for the Whitelist metrics stat.
func (h *Whitelist) allowPath(path string) bool {
	for _, v := range h.paths {
		if strings.HasPrefix(path, v) {
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
	h.s.Close()
	return nil
}

//------------------------------------------------------------------------------

// HandlerFunc returns an http.HandlerFunc for accessing metrics for appropriate
// child types
func (h *Whitelist) HandlerFunc() http.HandlerFunc {

	// If we want to expose a JSON stats endpoint we register the endpoints.
	if wHandlerFunc, ok := h.s.(WithHandlerFunc); ok {
		return wHandlerFunc.HandlerFunc()
	}

	return func(w http.ResponseWriter, r *http.Request) {
		// w.WriteHeader(400)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("The child of this whitelist does not support HTTP metrics."))
	}
}
