package gcp

import (
	"context"
	"fmt"
	"time"

	gcptrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

var _ gcptrace.Exporter

//------------------------------------------------------------------------------

func init() {
	_ = bundle.AllTracers.Add(NewCloudTrace, docs.ComponentSpec{
		Name:    "gcp_cloudtrace",
		Type:    docs.TypeTracer,
		Status:  docs.StatusExperimental,
		Summary: `Send tracing events to a [Google Cloud Trace](https://cloud.google.com/trace).`,
		Config: docs.FieldObject("", "").WithChildren(
			docs.FieldString("project", "The google project with Cloud Trace API enabled. If this is ommitted then the Google Cloud SDK will attempt auto-detect it from the environment.").HasDefault(""),
			docs.FieldFloat("sampling_ratio", "Sets the ratio of traces to sample. Tuning the sampling ratio is recommended for high-volume production workloads.", 1.0).HasDefault(1.0),
			docs.FieldString("tags", "A map of tags to add to tracing spans.").Map().Advanced().HasDefault(map[string]interface{}{}),
			docs.FieldString("flush_interval", "The period of time between each flush of tracing spans.").HasDefault(""),
		),
	})
}

//------------------------------------------------------------------------------

type cloudTrace struct {
	prov *tracesdk.TracerProvider
}

// NewCloudTrace creates new Google Cloud Trace tracer.
func NewCloudTrace(config tracer.Config) (tracer.Type, error) {
	ct := &cloudTrace{}

	sampler := tracesdk.ParentBased(tracesdk.TraceIDRatioBased(config.CloudTrace.SamplingRatio))

	exp, err := gcptrace.New(gcptrace.WithProjectID(config.CloudTrace.Project))
	if err != nil {
		return nil, fmt.Errorf("failed to create cloud trace exporter: %w", err)
	}

	var attrs []attribute.KeyValue
	for k, v := range config.CloudTrace.Tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	var batchOpts []tracesdk.BatchSpanProcessorOption
	if i := config.CloudTrace.FlushInterval; len(i) > 0 {
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

	// TODO: I'm so confused, these APIs are a nightmare.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	otel.SetTracerProvider(tp)

	ct.prov = tp

	return ct, nil
}

//------------------------------------------------------------------------------

// Close stops the tracer.
func (ct *cloudTrace) Close() error {
	prov := ct.prov
	if prov == nil {
		return nil
	}

	sig := shutdown.NewSignaller()
	ctx, cancel := sig.CloseAtLeisureCtx(context.Background())
	defer cancel()

	if err := ct.prov.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown trace provider: %w", err)
	}

	ct.prov = nil

	return nil
}
