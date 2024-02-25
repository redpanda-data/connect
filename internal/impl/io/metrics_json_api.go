package io

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterMetricsExporter("json_api", service.NewConfigSpec().
		Stable().
		Summary(`Serves metrics as JSON object with the service wide HTTP service at the endpoints `+"`/stats` and `/metrics`"+`.`).
		Description(`This metrics type is useful for debugging as it provides a human readable format that you can parse with tools such as `+"`jq`"+``).
		Field(service.NewObjectField("").Default(map[string]any{})),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return newJSONAPI(log)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jsonAPIMetrics struct {
	local     *metrics.Local
	timestamp time.Time
}

func newJSONAPI(logger *service.Logger) (*jsonAPIMetrics, error) {
	return &jsonAPIMetrics{
		local:     metrics.NewLocal(),
		timestamp: time.Now(),
	}, nil
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

func (h *jsonAPIMetrics) NewCounterCtor(path string, n ...string) service.MetricsExporterCounterCtor {
	tmp := h.local.GetCounterVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterCounter {
		return tmp.With(labelValues...)
	}
}

func (h *jsonAPIMetrics) NewTimerCtor(path string, n ...string) service.MetricsExporterTimerCtor {
	tmp := h.local.GetTimerVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterTimer {
		return tmp.With(labelValues...)
	}
}

func (h *jsonAPIMetrics) NewGaugeCtor(path string, n ...string) service.MetricsExporterGaugeCtor {
	tmp := h.local.GetGaugeVec(path, n...)
	return func(labelValues ...string) service.MetricsExporterGauge {
		return tmp.With(labelValues...)
	}
}

func (h *jsonAPIMetrics) Close(context.Context) error {
	return nil
}
