package pure

import (
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterOtelTracerProvider(
		"none", service.NewConfigSpec().
			Stable().
			Summary(`Do not send tracing events anywhere.`).
			Field(
				service.NewObjectField("").Default(map[string]any{}),
			),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			return noop.NewTracerProvider(), nil
		})
	if err != nil {
		panic(err)
	}
}
