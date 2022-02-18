package metrics

import (
	"net/http"
	"runtime"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

func init() {
	Constructors["json_api"] = TypeSpec{
		constructor: newJSONAPI,
		Summary:     `Serves metrics as JSON object with the service wide HTTP service at the endpoints ` + "`/stats` and `/metrics`" + `.`,
		Description: `
This metrics type is useful for debugging as it provides a human readable format that you can parse with tools such as ` + "`jq`" + ``,
		config: docs.FieldComponent().HasType(docs.FieldTypeObject),
	}
}

//------------------------------------------------------------------------------

// JSONAPIConfig contains configuration parameters for the JSON API metrics aggregator.
type JSONAPIConfig struct{}

// NewJSONAPIConfig returns a new JSONAPIConfig with default values.
func NewJSONAPIConfig() JSONAPIConfig {
	return JSONAPIConfig{}
}

//------------------------------------------------------------------------------

type jsonAPIMetrics struct {
	local     *Local
	log       log.Modular
	timestamp time.Time
}

func newJSONAPI(config Config, log log.Modular) (Type, error) {
	t := &jsonAPIMetrics{
		local:     NewLocal(),
		timestamp: time.Now(),
		log:       log,
	}
	return t, nil
}

//------------------------------------------------------------------------------

func (h *jsonAPIMetrics) HandlerFunc() http.HandlerFunc {
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
		obj.SetP(uptime, "uptime")
		obj.SetP(goroutines, "goroutines")

		w.Header().Set("Content-Type", "application/json")
		w.Write(obj.Bytes())
	}
}

func (h *jsonAPIMetrics) GetCounter(path string) StatCounter {
	return h.local.GetCounter(path)
}

func (h *jsonAPIMetrics) GetCounterVec(path string, n ...string) StatCounterVec {
	return h.local.GetCounterVec(path, n...)
}

func (h *jsonAPIMetrics) GetTimer(path string) StatTimer {
	return h.local.GetTimer(path)
}

func (h *jsonAPIMetrics) GetTimerVec(path string, n ...string) StatTimerVec {
	return h.local.GetTimerVec(path, n...)
}

func (h *jsonAPIMetrics) GetGauge(path string) StatGauge {
	return h.local.GetGauge(path)
}

func (h *jsonAPIMetrics) GetGaugeVec(path string, n ...string) StatGaugeVec {
	return h.local.GetGaugeVec(path, n...)
}

func (h *jsonAPIMetrics) Close() error {
	return nil
}
