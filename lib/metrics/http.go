package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPServer] = TypeSpec{
		constructor: NewHTTP,
		Description: `
It is possible to expose metrics without an aggregator service by having Benthos
serve them as a JSON object at the endpoints ` + "`/stats` and `/metrics`" + `.
This is useful for quickly debugging a pipeline.

The object takes the form of a hierarchical representation of the dot paths for
each metric combined. So, for example, if Benthos exposed two metric counters
` + "`foo.bar` and `bar.baz`" + ` then the resulting object might look like
this:

` + "``` json" + `
{
	"foo": {
		"bar": 9
	},
	"bar": {
		"baz": 3
	}
}
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// Errors for the HTTP type.
var (
	ErrTimedOut = errors.New("timed out")
)

//------------------------------------------------------------------------------

// HTTPConfig contains configuration parameters for the HTTP metrics aggregator.
type HTTPConfig struct {
	Prefix string `json:"prefix" yaml:"prefix"`
}

// NewHTTPConfig returns a new HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Prefix: "benthos",
	}
}

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
		pathPrefix: config.HTTP.Prefix,
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
func (h *HTTP) GetCounter(path string) StatCounter {
	return h.local.GetCounter(path)
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *HTTP) GetCounterVec(path string, n []string) StatCounterVec {
	return fakeCounterVec(func([]string) StatCounter {
		return h.local.GetCounter(path)
	})
}

// GetTimer returns a stat timer object for a path.
func (h *HTTP) GetTimer(path string) StatTimer {
	return h.local.GetTimer(path)
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *HTTP) GetTimerVec(path string, n []string) StatTimerVec {
	return fakeTimerVec(func([]string) StatTimer {
		return h.local.GetTimer(path)
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *HTTP) GetGauge(path string) StatGauge {
	return h.local.GetGauge(path)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *HTTP) GetGaugeVec(path string, n []string) StatGaugeVec {
	return fakeGaugeVec(func([]string) StatGauge {
		return h.local.GetGauge(path)
	})
}

// SetLogger does nothing.
func (h *HTTP) SetLogger(log log.Modular) {}

// Close stops the HTTP object from aggregating metrics and cleans up resources.
func (h *HTTP) Close() error {
	return nil
}

//------------------------------------------------------------------------------
