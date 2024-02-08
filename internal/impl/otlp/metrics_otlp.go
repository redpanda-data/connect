package otlp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/public/service"
)

func ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Send metrics to an [Open Telemetry collector](https://opentelemetry.io/docs/collector/).").
		Field(service.NewObjectListField("http",
			service.NewURLField("url").
				Description("The URL of a collector to send metrics to.").
				Default("localhost:4318"),
			service.NewBoolField("secure").
				Description("Connect to the collector over HTTPS").
				Default(false),
		).Description("A list of http collectors.")).
		Field(service.NewObjectListField("grpc",
			service.NewURLField("url").
				Description("The URL of a collector to send metrics to.").
				Default("localhost:4317"),
			service.NewBoolField("secure").
				Description("Connect to the collector with client transport security").
				Default(false),
		).Description("A list of grpc collectors.")).
		Field(service.NewStringMapField("labels").
			Description("A map of labels to add to all metrics.").
			Default(map[string]any{}).
			Advanced(),
		).
		Field(service.NewFloatListField("histogram_buckets").
			Description("Timing metrics histogram buckets (in seconds). If left empty defaults to [ 0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000 ] as per the Open Telemetry specification.").
			Default([]any{}).
			Advanced(),
		)
}

func init() {
	err := service.RegisterMetricsExporter(
		"open_telemetry_collector",
		ConfigSpec(),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			c, err := NewOtlpMetricsConfig(conf)
			if err != nil {
				return nil, err
			}
			return NewOtlpMetricsExporter(c, log)
		})
	if err != nil {
		panic(err)
	}
}

type otlpMetricsConfig struct {
	grpc    []collector
	http    []collector
	buckets []float64
	labels  map[string]string
}

type OtlpMetricsExporter struct {
	log          *service.Logger
	grpcProvider *metricsdk.MeterProvider
	httpProvider *metricsdk.MeterProvider
	grpcMeter    metric.Meter
	httpMeter    metric.Meter
	buckets      []float64
}

func NewOtlpMetricsConfig(conf *service.ParsedConfig) (*otlpMetricsConfig, error) {
	http, err := getMetricCollectors(conf, "http")
	if err != nil {
		return nil, err
	}

	grpc, err := getMetricCollectors(conf, "grpc")
	if err != nil {
		return nil, err
	}

	labels, err := conf.FieldStringMap("labels")
	if err != nil {
		return nil, err
	}

	var buckets []float64
	buckets, err = conf.FieldFloatList("buckets")
	if err != nil {
		buckets = []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}
	}

	return &otlpMetricsConfig{
		grpc:    grpc,
		http:    http,
		labels:  labels,
		buckets: buckets,
	}, nil
}

func getMetricCollectors(conf *service.ParsedConfig, name string) ([]collector, error) {
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

		secure, err := pc.FieldBool("secure")
		if err != nil {
			return nil, err
		}

		collectors = append(collectors, collector{
			url:    u,
			secure: secure,
		})
	}
	return collectors, nil
}

func NewOtlpMetricsExporter(config *otlpMetricsConfig, log *service.Logger) (om *OtlpMetricsExporter, err error) {
	om = &OtlpMetricsExporter{
		log:          log,
		grpcProvider: nil,
		httpProvider: nil,
		grpcMeter:    nil,
		httpMeter:    nil,
		buckets:      nil,
	}

	ctx := context.TODO()
	var attrs []attribute.KeyValue
	for k, v := range config.labels {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, ok := config.labels[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String("benthos"))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := config.labels[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(cli.Version))
		}
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, attrs...)

	if len(config.grpc) > 0 {
		grpcProvider, err := addMetricCollectors(ctx, config.grpc, res, "grpc")
		if err != nil {
			return nil, err
		}
		om.grpcProvider = grpcProvider
		om.grpcMeter = grpcProvider.Meter("benthos")
	}

	if len(config.http) > 0 {
		httpProvider, err := addMetricCollectors(ctx, config.http, res, "http")
		if err != nil {
			return nil, err
		}
		om.httpProvider = httpProvider
		om.httpMeter = httpProvider.Meter("benthos")
	}
	om.buckets = config.buckets
	return om, nil
}

func addMetricCollectors(ctx context.Context, collectors []collector, res *resource.Resource, proto string) (*metricsdk.MeterProvider, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	var meterOptions []metricsdk.Option

	switch proto {
	case "http":
		for _, c := range collectors {
			clientOpts := []otlpmetrichttp.Option{
				otlpmetrichttp.WithEndpoint(c.url),
			}

			if !c.secure {
				clientOpts = append(clientOpts, otlpmetrichttp.WithInsecure())
			}
			clientOpts = append(clientOpts, otlpmetrichttp.WithTemporalitySelector(deltaSelector))

			exp, err := otlpmetrichttp.New(ctx, clientOpts...)
			if err != nil {
				return nil, err
			}
			meterOptions = append(meterOptions, metricsdk.WithReader(metricsdk.NewPeriodicReader(exp)))
		}
	case "grpc":
		for _, c := range collectors {
			clientOpts := []otlpmetricgrpc.Option{
				otlpmetricgrpc.WithEndpoint(c.url),
			}

			if !c.secure {
				clientOpts = append(clientOpts, otlpmetricgrpc.WithInsecure())
			}
			clientOpts = append(clientOpts, otlpmetricgrpc.WithTemporalitySelector(deltaSelector))

			exp, err := otlpmetricgrpc.New(ctx, clientOpts...)
			if err != nil {
				return nil, err
			}
			meterOptions = append(meterOptions, metricsdk.WithReader(metricsdk.NewPeriodicReader(exp)))
		}
	default:
		return nil, errors.New("unsupported OTEL Collector protocol, use 'http' or 'grpc'")
	}
	meterOptions = append(meterOptions, metricsdk.WithResource(res))
	metricCollector := metricsdk.NewMeterProvider(meterOptions...)
	return metricCollector, nil
}

func deltaSelector(metricsdk.InstrumentKind) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

type otlpCounter struct {
	httpCounter metric.Int64Counter
	grpcCounter metric.Int64Counter
	labels      []attribute.KeyValue
}

func (om *OtlpMetricsExporter) NewCounterCtor(path string, labelNames ...string) service.MetricsExporterCounterCtor {
	return func(labelValues ...string) service.MetricsExporterCounter {
		var attrs []attribute.KeyValue
		for index, label := range labelNames {
			attrs = append(attrs, attribute.String(label, labelValues[index]))
		}

		var httpCounter metric.Int64Counter
		var grpcCounter metric.Int64Counter
		var err error

		if om.httpMeter != nil {
			httpCounter, err = om.httpMeter.Int64Counter(
				path,
			)
			if err != nil {
				return nil
			}
		}

		if om.grpcMeter != nil {
			grpcCounter, err = om.grpcMeter.Int64Counter(
				path,
			)
			if err != nil {
				return nil
			}
		}

		return &otlpCounter{
			httpCounter: httpCounter,
			grpcCounter: grpcCounter,
			labels:      attrs,
		}
	}
}

func (c *otlpCounter) Incr(count int64) {
	ctx := context.TODO()
	if c.httpCounter != nil {
		c.httpCounter.Add(ctx, count, metric.WithAttributes(c.labels...))
	}
	if c.grpcCounter != nil {
		c.grpcCounter.Add(ctx, count, metric.WithAttributes(c.labels...))
	}
}

type otlpTimer struct {
	httpTimer metric.Int64Histogram
	grpcTimer metric.Int64Histogram
	labels    []attribute.KeyValue
}

func (om *OtlpMetricsExporter) NewTimerCtor(path string, labelNames ...string) service.MetricsExporterTimerCtor {
	return func(labelValues ...string) service.MetricsExporterTimer {
		var attrs []attribute.KeyValue
		for index, label := range labelNames {
			attrs = append(attrs, attribute.String(label, labelValues[index]))
		}

		var httpTimer metric.Int64Histogram
		var grpcTimer metric.Int64Histogram
		var err error

		if om.httpMeter != nil {
			httpTimer, err = om.httpMeter.Int64Histogram(
				path,
				metric.WithExplicitBucketBoundaries(om.buckets...),
			)
			if err != nil {
				return nil
			}
		}

		if om.grpcMeter != nil {
			grpcTimer, err = om.grpcMeter.Int64Histogram(
				path,
				metric.WithExplicitBucketBoundaries(om.buckets...),
			)
			if err != nil {
				return nil
			}
		}
		return &otlpTimer{
			httpTimer: httpTimer,
			grpcTimer: grpcTimer,
			labels:    attrs,
		}
	}
}

func (c *otlpTimer) Timing(delta int64) {
	ctx := context.TODO()
	if c.httpTimer != nil {
		c.httpTimer.Record(ctx, delta, metric.WithAttributes(c.labels...))
	}
	if c.grpcTimer != nil {
		c.grpcTimer.Record(ctx, delta, metric.WithAttributes(c.labels...))
	}
}

type otlpGauge struct {
	httpGauge     metric.Int64ObservableGauge
	grpcGauge     metric.Int64ObservableGauge
	httpGaugeChan chan int64
	grpcGaugeChan chan int64
	labels        []attribute.KeyValue
}

func (om *OtlpMetricsExporter) NewGaugeCtor(path string, labelNames ...string) service.MetricsExporterGaugeCtor {
	return func(labelValues ...string) service.MetricsExporterGauge {
		var attrs []attribute.KeyValue
		for index, label := range labelNames {
			attrs = append(attrs, attribute.String(label, labelValues[index]))
		}

		var httpGauge metric.Int64ObservableGauge
		var grpcGauge metric.Int64ObservableGauge
		var grpcGaugeChan chan int64
		var grpcLatestChan chan int64
		var httpGaugeChan chan int64
		var httpLatestChan chan int64
		var err error

		if om.httpMeter != nil {
			httpGauge, err = om.httpMeter.Int64ObservableGauge(
				path,
			)
			if err != nil {
				return nil
			}
			httpGaugeChan = make(chan int64, 10)
			httpLatestChan = make(chan int64, 1)
			httpLatestChan <- 0
			_, err = om.httpMeter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					var last int64
					if len(httpGaugeChan) == 0 {
						last = <-httpLatestChan
						httpLatestChan <- last
						o.ObserveInt64(httpGauge, last)
						return nil
					}

					var value int64 = 0
					for len(httpGaugeChan) > 0 {
						value = <-httpGaugeChan
					}
					o.ObserveInt64(httpGauge, value)
					<-httpLatestChan
					httpLatestChan <- value
					return nil
				},
				httpGauge,
			)
			if err != nil {
				return nil
			}
		}

		if om.grpcMeter != nil {
			grpcGauge, err = om.grpcMeter.Int64ObservableGauge(
				path,
			)
			if err != nil {
				return nil
			}
			grpcGaugeChan = make(chan int64, 10)
			grpcLatestChan = make(chan int64, 1)
			grpcLatestChan <- 0
			_, err = om.grpcMeter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					var last int64
					if len(grpcGaugeChan) == 0 {
						last = <-grpcLatestChan
						grpcLatestChan <- last
						o.ObserveInt64(grpcGauge, last)
						return nil
					}

					var value int64 = 0
					for len(grpcGaugeChan) > 0 {
						value = <-grpcGaugeChan
					}
					o.ObserveInt64(grpcGauge, value)
					<-grpcLatestChan
					grpcLatestChan <- value
					return nil
				},
				grpcGauge,
			)
			if err != nil {
				return nil
			}
		}

		return &otlpGauge{
			httpGauge:     httpGauge,
			grpcGauge:     grpcGauge,
			httpGaugeChan: httpGaugeChan,
			grpcGaugeChan: grpcGaugeChan,
			labels:        attrs,
		}
	}
}

func (g *otlpGauge) Set(value int64) {
	if g.httpGauge != nil {
		g.httpGaugeChan <- value
	}
	if g.grpcGauge != nil {
		g.grpcGaugeChan <- value
	}
}

func (om *OtlpMetricsExporter) Close(context.Context) error {
	ctx := context.TODO()
	if om.grpcProvider != nil {
		if err := om.grpcProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("error shutting down grpc meter provider: %w", err)
		}
	}
	if om.httpProvider != nil {
		if err := om.httpProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("error shutting down http meter provider: %w", err)
		}
	}
	return nil
}
