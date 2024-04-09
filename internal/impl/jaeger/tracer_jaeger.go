package jaeger

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger" //nolint:staticcheck
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	jtFieldAgentAddress  = "agent_address"
	jtFieldCollectorURL  = "collector_url"
	jtFieldSamplerType   = "sampler_type"
	jtFieldSamplerParam  = "sampler_param"
	jtFieldTags          = "tags"
	jtFieldFlushInterval = "flush_interval"
)

type jaegerConfig struct {
	AgentAddress  string
	CollectorURL  string
	SamplerType   string
	SamplerParam  float64
	Tags          map[string]string
	FlushInterval string
}

func jaegerConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Send tracing events to a [Jaeger](https://www.jaegertracing.io/) agent or collector.").
		Fields(
			service.NewStringField(jtFieldAgentAddress).
				Description("The address of a Jaeger agent to send tracing events to.").
				Example("jaeger-agent:6831").
				Default(""),
			service.NewStringField(jtFieldCollectorURL).
				Description("The URL of a Jaeger collector to send tracing events to. If set, this will override `agent_address`.").
				Example("https://jaeger-collector:14268/api/traces").
				Version("3.38.0").
				Default(""),
			service.NewStringAnnotatedEnumField(jtFieldSamplerType, map[string]string{
				"const": "Sample a percentage of traces. 1 or more means all traces are sampled, 0 means no traces are sampled and anything in between means a percentage of traces are sampled. Tuning the sampling rate is recommended for high-volume production workloads.",
				// "probabilistic", "The sampler makes a random sampling decision with the probability of sampling equal to the value of sampler param.",
				// "ratelimiting", "The sampler uses a leaky bucket rate limiter to ensure that traces are sampled with a certain constant rate.",
				// "remote", "The sampler consults Jaeger agent for the appropriate sampling strategy to use in the current service.",
			}).
				Description("The sampler type to use.").
				Default("const"),
			service.NewFloatField(jtFieldSamplerParam).
				Description("A parameter to use for sampling. This field is unused for some sampling types.").
				Default(1.0).
				Advanced(),
			service.NewStringMapField(jtFieldTags).
				Description("A map of tags to add to tracing spans.").
				Advanced().
				Default(map[string]any{}),
			service.NewDurationField(jtFieldFlushInterval).
				Description("The period of time between each flush of tracing spans.").
				Optional(),
		)
}

var exporterInitFn = func(epOpt jaeger.EndpointOption) (tracesdk.SpanExporter, error) { return jaeger.New(epOpt) }

func init() {
	err := service.RegisterOtelTracerProvider("jaeger", jaegerConfigSpec(), func(conf *service.ParsedConfig) (p trace.TracerProvider, err error) {
		jConf := jaegerConfig{}
		if jConf.AgentAddress, err = conf.FieldString(jtFieldAgentAddress); err != nil {
			return
		}
		if jConf.CollectorURL, err = conf.FieldString(jtFieldCollectorURL); err != nil {
			return
		}
		if jConf.SamplerType, err = conf.FieldString(jtFieldSamplerType); err != nil {
			return
		}
		if jConf.SamplerParam, err = conf.FieldFloat(jtFieldSamplerParam); err != nil {
			return
		}
		if jConf.Tags, err = conf.FieldStringMap(jtFieldTags); err != nil {
			return
		}
		jConf.FlushInterval, _ = conf.FieldString(jtFieldFlushInterval)
		return NewJaeger(jConf)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// NewJaeger creates and returns a new Jaeger object.
func NewJaeger(config jaegerConfig) (trace.TracerProvider, error) {
	var sampler tracesdk.Sampler
	if sType := config.SamplerType; sType != "" {
		// TODO: https://github.com/open-telemetry/opentelemetry-go-contrib/pull/936
		switch strings.ToLower(sType) {
		case "const":
			sampler = tracesdk.TraceIDRatioBased(config.SamplerParam)
		case "probabilistic":
			return nil, errors.New("probabalistic sampling is no longer available")
		case "ratelimiting":
			return nil, errors.New("rate limited sampling is no longer available")
		case "remote":
			return nil, errors.New("remote sampling is no longer available")
		default:
			return nil, fmt.Errorf("unrecognised sampler type: %v", sType)
		}
	}

	// Create the Jaeger exporter
	var epOpt jaeger.EndpointOption
	if config.CollectorURL != "" {
		epOpt = jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(config.CollectorURL))
	} else {
		agentOpts, err := getAgentOpts(config.AgentAddress)
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
	for k, v := range config.Tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, ok := config.Tags[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String("benthos"))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := config.Tags[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(cli.Version))
		}
	}

	var batchOpts []tracesdk.BatchSpanProcessorOption
	if i := config.FlushInterval; i != "" {
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
