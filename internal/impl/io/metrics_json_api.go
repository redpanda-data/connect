package io

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func init() {
	_ = bundle.AllMetrics.Add(newJSONAPI,
		docs.ComponentSpec{
			Name:    "json_api",
			Type:    docs.TypeMetrics,
			Summary: `Serves metrics as JSON object with the service wide HTTP service at the endpoints ` + "`/stats` and `/metrics`" + `.`,
			Description: `
This metrics type is useful for debugging as it provides a human readable format that you can parse with tools such as ` + "`jq`" + ``,
			Config: docs.FieldObject("", "").HasDefault(struct{}{}),
		})
}

//------------------------------------------------------------------------------

type jsonAPIMetrics struct {
	local     *metrics.Local
	log       log.Modular
	timestamp time.Time
}

func newJSONAPI(config metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
	t := &jsonAPIMetrics{
		local:     metrics.NewLocal(),
		timestamp: time.Now(),
		log:       nm.Logger(),
	}
	return t, nil
}

//------------------------------------------------------------------------------

func (h *jsonAPIMetrics) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := map[string]any{}
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
		_, _ = w.Write(jBytes)
	}
}

func (h *jsonAPIMetrics) GetCounter(path string) metrics.StatCounter {
	return h.local.GetCounter(path)
}

func (h *jsonAPIMetrics) GetCounterVec(path string, n ...string) metrics.StatCounterVec {
	return h.local.GetCounterVec(path, n...)
}

func (h *jsonAPIMetrics) GetTimer(path string) metrics.StatTimer {
	return h.local.GetTimer(path)
}

func (h *jsonAPIMetrics) GetTimerVec(path string, n ...string) metrics.StatTimerVec {
	return h.local.GetTimerVec(path, n...)
}

func (h *jsonAPIMetrics) GetGauge(path string) metrics.StatGauge {
	return h.local.GetGauge(path)
}

func (h *jsonAPIMetrics) GetGaugeVec(path string, n ...string) metrics.StatGaugeVec {
	return h.local.GetGaugeVec(path, n...)
}

func (h *jsonAPIMetrics) Close() error {
	return nil
}
