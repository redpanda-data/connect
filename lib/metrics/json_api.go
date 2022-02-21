package metrics

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
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
		values := map[string]interface{}{}
		for k, v := range h.local.GetCounters() {
			values[k] = v
		}
		for k, v := range h.local.GetTimings() {
			ps := v.Percentiles([]float64{0.5, 0.9, 0.99})
			values[k] = struct {
				P50 float64 `json:"p50"`
				P90 float64 `json:"p90"`
				P99 float64 `json:"p99"`
			}{
				P50: ps[0],
				P90: ps[1],
				P99: ps[2],
			}
		}

		jBytes, err := json.Marshal(values)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jBytes)
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
