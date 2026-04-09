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
	"fmt"
	"net/http"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func otlpMetricsSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Send metrics to an https://opentelemetry.io/docs/collector/[Open Telemetry collector^].").
		Fields(
			service.NewStringField(otFieldService).
				Default("benthos").
				Description("The name of the service in metrics."),
		).
		Fields(collectorListFields()...).
		Fields(tagsField())
}

func init() {
	service.MustRegisterMetricsExporter(
		"open_telemetry_collector", otlpMetricsSpec(),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return newOtlpMetrics(conf, log)
		})
}

type otlpMetrics struct {
	provider *metricsdk.MeterProvider
	meter    metric.Meter
}

func newOtlpMetrics(conf *service.ParsedConfig, _ *service.Logger) (*otlpMetrics, error) {
	serviceName, err := conf.FieldString(otFieldService)
	if err != nil {
		return nil, err
	}

	httpCollectors, err := parseCollectors(conf, otFieldHTTP)
	if err != nil {
		return nil, err
	}

	grpcCollectors, err := parseCollectors(conf, otFieldGRPC)
	if err != nil {
		return nil, err
	}

	tags, err := conf.FieldStringMap(otFieldTags)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), otExporterTimeout)
	defer cancel()

	var opts []metricsdk.Option

	for _, c := range grpcCollectors {
		clientOpts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(c.address),
		}
		if !c.secure {
			clientOpts = append(clientOpts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating gRPC metric exporter for %s: %w", c.address, err)
		}
		opts = append(opts, metricsdk.WithReader(metricsdk.NewPeriodicReader(exp)))
	}

	for _, c := range httpCollectors {
		clientOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(c.address),
		}
		if !c.secure {
			clientOpts = append(clientOpts, otlpmetrichttp.WithInsecure())
		}
		exp, err := otlpmetrichttp.New(ctx, clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating HTTP metric exporter for %s: %w", c.address, err)
		}
		opts = append(opts, metricsdk.WithReader(metricsdk.NewPeriodicReader(exp)))
	}

	res := newResource(tags, serviceName, conf.EngineVersion())
	opts = append(opts, metricsdk.WithResource(res))

	provider := metricsdk.NewMeterProvider(opts...)

	return &otlpMetrics{
		provider: provider,
		meter:    provider.Meter(serviceName),
	}, nil
}

func (o *otlpMetrics) NewCounterCtor(path string, labelKeys ...string) service.MetricsExporterCounterCtor {
	counter, err := o.meter.Int64Counter(path)
	if err != nil {
		return func(...string) service.MetricsExporterCounter { return &otlpCounter{} }
	}
	return func(labelValues ...string) service.MetricsExporterCounter {
		return &otlpCounter{
			counter: counter,
			attrOpt: metric.WithAttributes(zipAttrs(labelKeys, labelValues)...),
		}
	}
}

func (o *otlpMetrics) NewTimerCtor(path string, labelKeys ...string) service.MetricsExporterTimerCtor {
	histogram, err := o.meter.Int64Histogram(path)
	if err != nil {
		return func(...string) service.MetricsExporterTimer { return &otlpTimer{} }
	}
	return func(labelValues ...string) service.MetricsExporterTimer {
		return &otlpTimer{
			histogram: histogram,
			attrOpt:   metric.WithAttributes(zipAttrs(labelKeys, labelValues)...),
		}
	}
}

func (o *otlpMetrics) NewGaugeCtor(path string, labelKeys ...string) service.MetricsExporterGaugeCtor {
	return func(labelValues ...string) service.MetricsExporterGauge {
		attrOpt := metric.WithAttributes(zipAttrs(labelKeys, labelValues)...)
		g := &otlpGauge{}
		gauge, err := o.meter.Int64ObservableGauge(path,
			metric.WithInt64Callback(func(_ context.Context, obs metric.Int64Observer) error {
				obs.Observe(g.value.Load(), attrOpt)
				return nil
			}),
		)
		if err != nil {
			return g
		}
		g.gauge = gauge
		return g
	}
}

func (*otlpMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (o *otlpMetrics) Close(ctx context.Context) error {
	return o.provider.Shutdown(ctx)
}

func zipAttrs(keys, values []string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for i, k := range keys {
		if i < len(values) {
			attrs = append(attrs, attribute.String(k, values[i]))
		}
	}
	return attrs
}

type otlpCounter struct {
	counter metric.Int64Counter
	attrOpt metric.AddOption
}

func (c *otlpCounter) Incr(count int64) {
	if c.counter != nil {
		c.counter.Add(context.Background(), count, c.attrOpt)
	}
}

type otlpTimer struct {
	histogram metric.Int64Histogram
	attrOpt   metric.RecordOption
}

func (t *otlpTimer) Timing(delta int64) {
	if t.histogram != nil {
		t.histogram.Record(context.Background(), delta, t.attrOpt)
	}
}

type otlpGauge struct {
	gauge metric.Int64ObservableGauge
	value atomic.Int64
}

func (g *otlpGauge) Set(value int64) {
	g.value.Store(value)
}
