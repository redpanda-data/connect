package gcp

import (
	"fmt"
	"time"

	gcptrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	ctFieldProject       = "project"
	ctFieldSamplingRatio = "sampling_ratio"
	ctFieldTags          = "tags"
	ctFieldFlushInterval = "flush_interval"
)

func cloudTraceSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.2.0").
		Summary(`Send tracing events to a [Google Cloud Trace](https://cloud.google.com/trace).`).
		Fields(
			service.NewStringField(ctFieldProject).
				Description("The google project with Cloud Trace API enabled. If this is omitted then the Google Cloud SDK will attempt auto-detect it from the environment."),
			service.NewFloatField(ctFieldSamplingRatio).Description("Sets the ratio of traces to sample. Tuning the sampling ratio is recommended for high-volume production workloads.").
				Example(1.0).
				Default(1.0),
			service.NewStringMapField(ctFieldTags).
				Description("A map of tags to add to tracing spans.").
				Advanced().
				Default(map[string]any{}),
			service.NewDurationField(ctFieldFlushInterval).
				Description("The period of time between each flush of tracing spans.").
				Optional(),
		)
}

var _ gcptrace.Exporter

func init() {
	err := service.RegisterOtelTracerProvider("gcp_cloudtrace", cloudTraceSpec(), func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
		return cloudTraceFromParsed(conf)
	})
	if err != nil {
		panic(err)
	}
}

func cloudTraceFromParsed(conf *service.ParsedConfig) (trace.TracerProvider, error) {
	sampleRatio, err := conf.FieldFloat(ctFieldSamplingRatio)
	if err != nil {
		return nil, err
	}

	sampler := tracesdk.ParentBased(tracesdk.TraceIDRatioBased(sampleRatio))

	projID, err := conf.FieldString(ctFieldProject)
	if err != nil {
		return nil, err
	}

	exp, err := gcptrace.New(gcptrace.WithProjectID(projID))
	if err != nil {
		return nil, fmt.Errorf("failed to create cloud trace exporter: %w", err)
	}

	tags, err := conf.FieldStringMap(ctFieldTags)
	if err != nil {
		return nil, err
	}

	var attrs []attribute.KeyValue
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	var batchOpts []tracesdk.BatchSpanProcessorOption
	if i, _ := conf.FieldString(ctFieldFlushInterval); i != "" {
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
