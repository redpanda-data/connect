package otlp

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/public/service"
)

func oltpSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Send tracing events to an [Open Telemetry collector](https://opentelemetry.io/docs/collector/).").
		Field(service.NewObjectListField("http",
			service.NewStringField("address").
				Description("The endpoint of a collector to send tracing events to.").
				Optional().
				Example("localhost:4318"),
			service.NewStringField("url").
				Description("The URL of a collector to send tracing events to.").
				Deprecated().
				Default("localhost:4318"),
			service.NewBoolField("secure").
				Description("Connect to the collector over HTTPS").
				Default(false),
		).Description("A list of http collectors.")).
		Field(service.NewObjectListField("grpc",
			service.NewURLField("address").
				Description("The endpoint of a collector to send tracing events to.").
				Optional().
				Example("localhost:4317"),
			service.NewURLField("url").
				Description("The URL of a collector to send tracing events to.").
				Deprecated().
				Default("localhost:4317"),
			service.NewBoolField("secure").
				Description("Connect to the collector with client transport security").
				Default(false),
		).Description("A list of grpc collectors.")).
		Field(service.NewStringMapField("tags").
			Description("A map of tags to add to all tracing spans.").
			Default(map[string]any{}).
			Advanced()).
		Field(service.NewObjectField("sampling",
			service.NewBoolField("enabled").
				Description("Whether to enable sampling.").
				Default(false),
			service.NewFloatField("ratio").
				Description("Sets the ratio of traces to sample.").
				Examples(0.85, 0.5).
				Optional()).
			Description("Settings for trace sampling. Sampling is recommended for high-volume production workloads.").
			Version("4.25.0"))
}

func init() {
	err := service.RegisterOtelTracerProvider(
		"open_telemetry_collector", oltpSpec(),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			c, err := oltpConfigFromParsed(conf)
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
	address string
	secure  bool
}

type sampleConfig struct {
	enabled bool
	ratio   float64
}

type otlp struct {
	grpc     []collector
	http     []collector
	tags     map[string]string
	sampling sampleConfig
}

func oltpConfigFromParsed(conf *service.ParsedConfig) (*otlp, error) {
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

	sampling, err := sampleConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &otlp{
		grpc,
		http,
		tags,
		sampling,
	}, nil
}

func collectors(conf *service.ParsedConfig, name string) ([]collector, error) {
	list, err := conf.FieldObjectList(name)
	if err != nil {
		return nil, err
	}
	collectors := make([]collector, 0, len(list))
	for _, pc := range list {
		u, _ := pc.FieldString("address")
		if u == "" {
			if u, _ = pc.FieldString("url"); u == "" {
				return nil, errors.New("an address must be specified")
			}
		}

		secure, err := pc.FieldBool("secure")
		if err != nil {
			return nil, err
		}

		collectors = append(collectors, collector{
			address: u,
			secure:  secure,
		})
	}
	return collectors, nil
}

func sampleConfigFromParsed(conf *service.ParsedConfig) (sampleConfig, error) {
	conf = conf.Namespace("sampling")
	enabled, err := conf.FieldBool("enabled")
	if err != nil {
		return sampleConfig{}, err
	}

	var ratio float64
	if conf.Contains("ratio") {
		if ratio, err = conf.FieldFloat("ratio"); err != nil {
			return sampleConfig{}, err
		}
	}

	return sampleConfig{
		enabled: enabled,
		ratio:   ratio,
	}, nil
}

//------------------------------------------------------------------------------

func newOtlp(config *otlp) (trace.TracerProvider, error) {
	ctx := context.TODO()
	var opts []tracesdk.TracerProviderOption

	if config.sampling.enabled {
		opts = append(opts, tracesdk.WithSampler(tracesdk.TraceIDRatioBased(config.sampling.ratio)))
	}

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
		clientOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(c.address),
		}

		if !c.secure {
			clientOpts = append(clientOpts, otlptracegrpc.WithInsecure())
		}

		exp, err := otlptrace.New(ctx, otlptracegrpc.NewClient(clientOpts...))
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
		clientOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(c.address),
		}

		if !c.secure {
			clientOpts = append(clientOpts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptrace.New(ctx, otlptracehttp.NewClient(clientOpts...))
		if err != nil {
			return nil, err
		}
		opts = append(opts, tracesdk.WithBatcher(exp))
	}
	return opts, nil
}
