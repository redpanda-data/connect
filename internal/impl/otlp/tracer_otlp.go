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

	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	spec := service.NewConfigSpec().
		Summary("Send tracing events to an [Open Telemetry collector](https://opentelemetry.io/docs/collector/).").
		Field(service.NewObjectListField("http",
			service.NewURLField("url").
				Description("The URL of a collector to send tracing events to.").
				Default("localhost:4318"),
		).Description("A list of http collectors.")).
		Field(service.NewObjectListField("grpc",
			service.NewURLField("url").
				Description("The URL of a collector to send tracing events to.").
				Default("localhost:4317"),
		).Description("A list of grpc collectors.")).
		Field(service.NewStringMapField("tags").
			Description("A map of tags to add to all tracing spans.").
			Default(map[string]string{}).
			Advanced())

	err := service.RegisterOtelTracerProvider(
		"open_telemetry_collector",
		spec,
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			c, err := newOtlpConfig(conf)
			if err != nil {
				return nil, err
			}
			return newOtlp(c)
		})
	if err != nil {
		panic(err)
	}
}

type collector struct {
	url string
}

type otlp struct {
	grpc []collector
	http []collector
	tags map[string]string
}

func newOtlpConfig(conf *service.ParsedConfig) (*otlp, error) {
	http, err := collectors(conf, "http")
	if err != nil {
		return nil, err
	}

	grpc, err := collectors(conf, "grpc")
	if err != nil {
		return nil, err
	}

	tags, err := conf.FieldStringMap("tags")
	if err != nil {
		return nil, err
	}

	return &otlp{
		grpc,
		http,
		tags,
	}, nil
}

func collectors(conf *service.ParsedConfig, name string) ([]collector, error) {
	list, err := conf.FieldObjectList(name)
	if err != nil {
		return nil, err
	}
	collectors := make([]collector, 0, len(list))
	for _, pc := range list {
		u, err := pc.FieldString("url")
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, collector{u})
	}
	return collectors, nil
}

//------------------------------------------------------------------------------

func newOtlp(config *otlp) (trace.TracerProvider, error) {
	ctx := context.TODO()
	var opts []tracesdk.TracerProviderOption

	opts, err := addGrpcCollectors(ctx, config.grpc, opts)
	if err != nil {
		return nil, err
	}

	opts, err = addHTTPCollectors(ctx, config.http, opts)

	if err != nil {
		return nil, err
	}
	var attrs []attribute.KeyValue

	for k, v := range config.tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, ok := config.tags[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String("benthos"))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := config.tags[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(cli.Version))
		}
	}

	opts = append(opts, tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)))

	return tracesdk.NewTracerProvider(opts...), nil
}

func addGrpcCollectors(ctx context.Context, collectors []collector, opts []tracesdk.TracerProviderOption) ([]tracesdk.TracerProviderOption, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	for _, c := range collectors {
		exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(
			otlptracegrpc.WithInsecure(),
			otlptracegrpc.WithEndpoint(c.url),
			otlptracegrpc.WithDialOption(grpc.WithBlock()),
		))
		if err != nil {
			return nil, err
		}
		opts = append(opts, tracesdk.WithBatcher(exp))
	}
	return opts, nil
}

func addHTTPCollectors(ctx context.Context, collectors []collector, opts []tracesdk.TracerProviderOption) ([]tracesdk.TracerProviderOption, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	for _, c := range collectors {
		exp, err := otlptrace.New(ctx, otlptracehttp.NewClient(
			otlptracehttp.WithInsecure(),
			otlptracehttp.WithEndpoint(c.url),
		))
		if err != nil {
			return nil, err
		}
		opts = append(opts, tracesdk.WithBatcher(exp))
	}
	return opts, nil
}
