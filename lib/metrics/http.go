package metrics

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPServer] = TypeSpec{
		constructor: NewHTTP,
		Summary: `
Serves metrics as [JSON object](#object-format) with the service wide HTTP
service at the endpoints ` + "`/stats` and `/metrics`" + `.`,
		Description: `
This metrics type is useful for debugging as it provides a human readable format
that you can parse with tools such as ` + "`jq`" + ``,
		Footnotes: `
## Object Format

The metrics object takes the form of a hierarchical representation of the dot
paths for each metric combined. So, for example, if Benthos exposed two metric
counters ` + "`foo.bar` and `bar.baz`" + ` then the resulting object might look
like this:

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
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("prefix", "A string prefix to add to all metrics."),
			pathMappingDocs(false, false),
		},
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
	Prefix      string `json:"prefix" yaml:"prefix"`
	PathMapping string `json:"path_mapping" yaml:"path_mapping"`
}

// NewHTTPConfig returns a new HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Prefix:      "benthos",
		PathMapping: "",
	}
}

//------------------------------------------------------------------------------

// HTTP is an object with capability to hold internal stats as a JSON endpoint.
type HTTP struct {
	local       *Local
	log         log.Modular
	timestamp   time.Time
	pathPrefix  string
	pathMapping *pathMapping
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
	var err error
	if t.pathMapping, err = newPathMapping(config.HTTP.PathMapping, t.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}
	return t, nil
}

//------------------------------------------------------------------------------

func (h *HTTP) getPath(path string) string {
	path = h.pathMapping.mapPathNoTags(path)
	if len(h.pathPrefix) > 0 && len(path) > 0 {
		path = h.pathPrefix + "." + path
	}
	return path
}

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

		w.Header().Set("Content-Type", "application/json")
		w.Write(obj.Bytes())
	}
}

// GetCounter returns a stat counter object for a path.
func (h *HTTP) GetCounter(path string) StatCounter {
	if path = h.getPath(path); len(path) == 0 {
		return DudStat{}
	}
	return h.local.GetCounter(path)
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (h *HTTP) GetCounterVec(path string, n []string) StatCounterVec {
	if path = h.getPath(path); len(path) == 0 {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	return fakeCounterVec(func([]string) StatCounter {
		return h.local.GetCounter(path)
	})
}

// GetTimer returns a stat timer object for a path.
func (h *HTTP) GetTimer(path string) StatTimer {
	if path = h.getPath(path); len(path) == 0 {
		return DudStat{}
	}
	return h.local.GetTimer(path)
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (h *HTTP) GetTimerVec(path string, n []string) StatTimerVec {
	if path = h.getPath(path); len(path) == 0 {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	return fakeTimerVec(func([]string) StatTimer {
		return h.local.GetTimer(path)
	})
}

// GetGauge returns a stat gauge object for a path.
func (h *HTTP) GetGauge(path string) StatGauge {
	if path = h.getPath(path); len(path) == 0 {
		return DudStat{}
	}
	return h.local.GetGauge(path)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (h *HTTP) GetGaugeVec(path string, n []string) StatGaugeVec {
	if path = h.getPath(path); len(path) == 0 {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	return fakeGaugeVec(func([]string) StatGauge {
		return h.local.GetGauge(path)
	})
}

// SetLogger does nothing.
func (h *HTTP) SetLogger(log log.Modular) {
	h.log = log
}

// Close stops the HTTP object from aggregating metrics and cleans up resources.
func (h *HTTP) Close() error {
	return nil
}

//------------------------------------------------------------------------------
