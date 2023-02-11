package jaeger

import (
	"fmt"
	"net"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

var exporterInitFn = func(epOpt jaeger.EndpointOption) (tracesdk.SpanExporter, error) { return jaeger.New(epOpt) }

func init() {
	_ = bundle.AllTracers.Add(NewJaeger, docs.ComponentSpec{
		Name:    "jaeger",
		Type:    docs.TypeTracer,
		Status:  docs.StatusStable,
		Summary: `Send tracing events to a [Jaeger](https://www.jaegertracing.io/) agent or collector.`,
		Config: docs.FieldObject("", "").WithChildren(
			docs.FieldString("agent_address", "The address of a Jaeger agent to send tracing events to.", "jaeger-agent:6831").HasDefault(""),
			docs.FieldURL("collector_url", "The URL of a Jaeger collector to send tracing events to. If set, this will override `agent_address`.",
				"https://jaeger-collector:14268/api/traces").HasDefault("").AtVersion("3.38.0"),
			docs.FieldString("sampler_type", "The sampler type to use.").HasAnnotatedOptions(
				"const", "Sample a percentage of traces. 1 or more means all traces are sampled, 0 means no traces are sampled and anything in between means a percentage of traces are sampled. Tuning the sampling rate is recommended for high-volume production workloads.",
				// "probabilistic", "The sampler makes a random sampling decision with the probability of sampling equal to the value of sampler param.",
				// "ratelimiting", "The sampler uses a leaky bucket rate limiter to ensure that traces are sampled with a certain constant rate.",
				// "remote", "The sampler consults Jaeger agent for the appropriate sampling strategy to use in the current service.",
			).HasDefault("const"),
			docs.FieldFloat("sampler_param", "A parameter to use for sampling. This field is unused for some sampling types.").Advanced().HasDefault(1.0),
			docs.FieldString("tags", "A map of tags to add to tracing spans.").Map().Advanced().HasDefault(map[string]any{}),
			docs.FieldString("flush_interval", "The period of time between each flush of tracing spans.").HasDefault(""),
		),
	})
}

//------------------------------------------------------------------------------

// NewJaeger creates and returns a new Jaeger object.
func NewJaeger(config tracer.Config, _ bundle.NewManagement) (trace.TracerProvider, error) {
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
		agentOpts, err := getAgentOpts(config.Jaeger.AgentAddress)
		if err != nil {
			return nil, err
		}

		epOpt = jaeger.WithAgentEndpoint(agentOpts...)
	}

	exp, err := exporterInitFn(epOpt)
	if err != nil {
		return nil, err
	}

	var attrs []attribute.KeyValue
	for k, v := range config.Jaeger.Tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, ok := config.Jaeger.Tags[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String("benthos"))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := config.Jaeger.Tags[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(cli.Version))
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

	return tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp, batchOpts...),
		tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
		tracesdk.WithSampler(sampler),
	), nil
}

func getAgentOpts(agentAddress string) ([]jaeger.AgentEndpointOption, error) {
	var agentOpts []jaeger.AgentEndpointOption
	if strings.Contains(agentAddress, ":") {
		agentHost, agentPort, err := net.SplitHostPort(agentAddress)
		if err != nil {
			return agentOpts, err
		}
		agentOpts = append(agentOpts, jaeger.WithAgentHost(agentHost), jaeger.WithAgentPort(agentPort))
	} else {
		agentOpts = append(agentOpts, jaeger.WithAgentHost(agentAddress))
	}

	return agentOpts, nil
}
