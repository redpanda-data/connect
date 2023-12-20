package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterMetricsExporter("none", service.NewConfigSpec().
		Stable().
		Summary(`Disable metrics entirely.`).
		Field(service.NewObjectField("").Default(map[string]any{})),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return noopMetrics{}, nil
		})
	if err != nil {
		panic(err)
	}
}

type noopMetrics struct{}

func (n noopMetrics) NewCounterCtor(name string, labelKeys ...string) service.MetricsExporterCounterCtor {
	return func(labelValues ...string) service.MetricsExporterCounter {
		return n
	}
}

func (n noopMetrics) NewTimerCtor(name string, labelKeys ...string) service.MetricsExporterTimerCtor {
	return func(labelValues ...string) service.MetricsExporterTimer {
		return n
	}
}

func (n noopMetrics) NewGaugeCtor(name string, labelKeys ...string) service.MetricsExporterGaugeCtor {
	return func(labelValues ...string) service.MetricsExporterGauge {
		return n
	}
}

func (n noopMetrics) Close(ctx context.Context) error {
	return nil
}

func (n noopMetrics) Incr(count int64)          {}
func (n noopMetrics) IncrFloat64(count float64) {}
func (n noopMetrics) Timing(delta int64)        {}
func (n noopMetrics) Set(value int64)           {}
func (n noopMetrics) SetFloat64(value float64)  {}
