// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/tracing"
)

func otlpTracerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Send tracing events to an https://opentelemetry.io/docs/collector/[Open Telemetry collector^].").
		Fields(
			service.NewStringField(otFieldService).
				Default("benthos").
				Description("The name of the service in traces."),
		).
		Fields(collectorListFields()...).
		Fields(
			tagsField(),
			service.NewObjectField("sampling",
				service.NewBoolField("enabled").
					Description("Whether to enable sampling.").
					Default(false),
				service.NewFloatField("ratio").
					Description("Sets the ratio of traces to sample.").
					Examples(0.85, 0.5).
					Optional()).
				Description("Settings for trace sampling. Sampling is recommended for high-volume production workloads.").
				Version("4.25.0"),
		)
}

func init() {
	service.MustRegisterOtelTracerProvider(
		"open_telemetry_collector", otlpTracerSpec(),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			c, err := otlpTracerConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newOtlpTracer(c)
		})
}

type sampleConfig struct {
	enabled bool
	ratio   float64
}

type otlpTracer struct {
	serviceName   string
	engineVersion string
	grpc          []collector
	http          []collector
	tags          map[string]string
	sampling      sampleConfig
}

func otlpTracerConfigFromParsed(conf *service.ParsedConfig) (*otlpTracer, error) {
	serviceName, err := conf.FieldString(otFieldService)
	if err != nil {
		return nil, err
	}

	http, err := parseCollectors(conf, otFieldHTTP)
	if err != nil {
		return nil, err
	}

	grpc, err := parseCollectors(conf, otFieldGRPC)
	if err != nil {
		return nil, err
	}

	tags, err := conf.FieldStringMap(otFieldTags)
	if err != nil {
		return nil, err
	}

	sampling, err := sampleConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &otlpTracer{
		serviceName:   serviceName,
		engineVersion: conf.EngineVersion(),
		grpc:          grpc,
		http:          http,
		tags:          tags,
		sampling:      sampling,
	}, nil
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

func newOtlpTracer(config *otlpTracer) (trace.TracerProvider, error) {
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

	opts = append(
		opts,
		tracesdk.WithIDGenerator(tracing.NewIDGenerator()),
		tracesdk.WithResource(newResource(config.tags, config.serviceName, config.engineVersion)),
	)

	return tracesdk.NewTracerProvider(opts...), nil
}

func addGrpcCollectors(ctx context.Context, collectors []collector, opts []tracesdk.TracerProviderOption) ([]tracesdk.TracerProviderOption, error) {
	ctx, cancel := context.WithTimeout(ctx, otExporterTimeout)
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
	ctx, cancel := context.WithTimeout(ctx, otExporterTimeout)
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
