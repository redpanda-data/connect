// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/tracing"
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
		Summary(`Send tracing events to a https://cloud.google.com/trace[Google Cloud Trace^].`).
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
	service.MustRegisterOtelTracerProvider("gcp_cloudtrace", cloudTraceSpec(), func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
		return cloudTraceFromParsed(conf)
	})

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
		tracesdk.WithIDGenerator(tracing.NewIDGenerator()),
		tracesdk.WithBatcher(exp, batchOpts...),
		tracesdk.WithResource(resource.NewWithAttributes(semconv.SchemaURL, attrs...)),
		tracesdk.WithSampler(sampler),
	), nil
}
