package tracer

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJaeger] = TypeSpec{
		constructor: NewJaeger,
		Summary: `
Send tracing events to a [Jaeger](https://www.jaegertracing.io/) agent or collector.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("agent_address", "The address of a Jaeger agent to send tracing events to.", "jaeger-agent:6831"),
			docs.FieldCommon("collector_url", "The URL of a Jaeger collector to send tracing events to. If set, this will override `agent_address`.",
				"https://jaeger-collector:14268/api/traces").AtVersion("3.38.0"),
			docs.FieldCommon("sampler_type", "The sampler type to use.").HasAnnotatedOptions(
				"const", "A constant decision for all traces, either 1 or 0.",
				// "probabilistic", "The sampler makes a random sampling decision with the probability of sampling equal to the value of sampler param.",
				// "ratelimiting", "The sampler uses a leaky bucket rate limiter to ensure that traces are sampled with a certain constant rate.",
				// "remote", "The sampler consults Jaeger agent for the appropriate sampling strategy to use in the current service.",
			),
			docs.FieldFloat("sampler_param", "A parameter to use for sampling. This field is unused for some sampling types.").Advanced(),
			docs.FieldString("tags", "A map of tags to add to tracing spans.").Map().Advanced(),
			docs.FieldCommon("flush_interval", "The period of time between each flush of tracing spans."),
		},
	}
}

//------------------------------------------------------------------------------

// JaegerConfig is config for the Jaeger metrics type.
type JaegerConfig struct {
	AgentAddress  string            `json:"agent_address" yaml:"agent_address"`
	CollectorURL  string            `json:"collector_url" yaml:"collector_url"`
	SamplerType   string            `json:"sampler_type" yaml:"sampler_type"`
	SamplerParam  float64           `json:"sampler_param" yaml:"sampler_param"`
	Tags          map[string]string `json:"tags" yaml:"tags"`
	FlushInterval string            `json:"flush_interval" yaml:"flush_interval"`
}

// NewJaegerConfig creates an JaegerConfig struct with default values.
func NewJaegerConfig() JaegerConfig {
	return JaegerConfig{
		AgentAddress:  "",
		CollectorURL:  "",
		SamplerType:   "const",
		SamplerParam:  1.0,
		Tags:          map[string]string{},
		FlushInterval: "",
	}
}

//------------------------------------------------------------------------------

// Jaeger is a tracer with the capability to push spans to a Jaeger instance.
type Jaeger struct {
	prov *tracesdk.TracerProvider
}

// NewJaeger creates and returns a new Jaeger object.
func NewJaeger(config Config, opts ...func(Type)) (Type, error) {
	j := &Jaeger{}

	for _, opt := range opts {
		opt(j)
	}

	var sampler tracesdk.Sampler
	if sType := config.Jaeger.SamplerType; len(sType) > 0 {
		// TODO: https://github.com/open-telemetry/opentelemetry-go-contrib/pull/936
		switch strings.ToLower(sType) {
		case "const":
			sampler = tracesdk.TraceIDRatioBased(config.Jaeger.SamplerParam)
		case "probabilistic":
			return nil, fmt.Errorf("probabalistic sampling is no longer available")
		case "ratelimiting":
			return nil, fmt.Errorf("rate limited sampling is no longer available")
		case "remote":
			return nil, fmt.Errorf("remote sampling is no longer available")
		default:
			return nil, fmt.Errorf("unrecognised sampler type: %v", sType)
		}
	}

	// Create the Jaeger exporter
	var epOpt jaeger.EndpointOption
	if config.Jaeger.CollectorURL != "" {
		epOpt = jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.Jaeger.CollectorURL))
	} else {
		agentURL, err := url.Parse(config.Jaeger.AgentAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to parse jaeger agent address: %w", err)
		}
		agentOpts := []jaeger.AgentEndpointOption{
			jaeger.WithAgentHost(agentURL.Host),
		}
		if p := agentURL.Port(); p != "" {
			agentOpts = append(agentOpts, jaeger.WithAgentPort(agentURL.Port()))
		}
		epOpt = jaeger.WithAgentEndpoint(agentOpts...)
	}

	exp, err := jaeger.New(epOpt)
	if err != nil {
		return nil, err
	}

	var attrs []attribute.KeyValue
	if tags := config.Jaeger.Tags; len(tags) > 0 {
		for k, v := range config.Jaeger.Tags {
			attrs = append(attrs, attribute.String(k, v))
		}
	}

	var batchOpts []tracesdk.BatchSpanProcessorOption
	if i := config.Jaeger.FlushInterval; len(i) > 0 {
		flushInterval, err := time.ParseDuration(i)
		if err != nil {
			return nil, fmt.Errorf("failed to parse flush interval '%s': %v", i, err)
		}
		batchOpts = append(batchOpts, tracesdk.WithBatchTimeout(flushInterval))
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp, batchOpts...),
		tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
		tracesdk.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)

	// TODO: I'm so confused, these APIs are a nightmare.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	j.prov = tp
	return j, nil
}

//------------------------------------------------------------------------------

// Close stops the tracer.
func (j *Jaeger) Close() error {
	if j.prov != nil {
		_ = j.prov.Shutdown(context.Background())
		j.prov = nil
	}
	return nil
}
