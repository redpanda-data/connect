package otlp

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"google.golang.org/grpc"

	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bundle"

	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func init() {
	bundle.AllTracers.Add(NewOtlp, docs.ComponentSpec{
		Name:    "open_telemetry_collector",
		Type:    docs.TypeTracer,
		Status:  docs.StatusExperimental,
		Summary: `Send tracing events to a [OpenTelemetry](https://opentelemetry.io/docs/collector/) collector.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("http", "A list of http collectors.").
				Array().
				WithChildren(
					docs.FieldString("url", "The URL of a collector to send tracing events to.").HasDefault("localhost:4318"),
				),
			docs.FieldString("grpc", "A list of grpc collectors.").
				Array().
				WithChildren(
					docs.FieldString("url", "The URL of a collector to send tracing events to.").HasDefault("localhost:4317"),
				),
			docs.FieldString("tags", "A map of tags to add to all tracing spans.").Map().Advanced().HasDefault(map[string]interface{}{}),
		),
	})
}

//------------------------------------------------------------------------------

func NewOtlp(config tracer.Config, nm bundle.NewManagement) (trace.TracerProvider, error) {
	ctx := context.TODO()
	var opts []tracesdk.TracerProviderOption

	opts, err := addGrpcCollectors(config.OtlpConfig.Grpc, ctx, opts)

	if err != nil {
		return nil, err
	}

	opts, err = addHttpCollectors(config.OtlpConfig.Http, ctx, opts)

	if err != nil {
		return nil, err
	}
	var attrs []attribute.KeyValue

	for k, v := range config.OtlpConfig.Tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	opts = append(opts, tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)))

	return tracesdk.NewTracerProvider(opts...), nil
}

func addGrpcCollectors(collectors []tracer.Collector, ctx context.Context, opts []tracesdk.TracerProviderOption) ([]tracesdk.TracerProviderOption, error) {
	for _, c := range collectors {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(c.Url),
			otlptracegrpc.WithDialOption(grpc.WithBlock()),
		))

		if err != nil {
			return nil, err
		}

		opts = append(opts, tracesdk.WithBatcher(exp))
	}
	return opts, nil
}

func addHttpCollectors(collectors []tracer.Collector, ctx context.Context, opts []tracesdk.TracerProviderOption) ([]tracesdk.TracerProviderOption, error) {
	for _, c := range collectors {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		exp, err := otlptrace.New(ctx, otlptracehttp.NewClient(
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(c.Url),
		))

		if err != nil {
			return nil, err
		}

		opts = append(opts, tracesdk.WithBatcher(exp))
	}
	return opts, nil
}
