// Copyright 2025 Redpanda Data, Inc.
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

package redpanda

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.9.0"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	exporter "github.com/redpanda-data/common-go/redpanda-otel-exporter"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/serviceaccount"
	"github.com/redpanda-data/connect/v4/internal/tracing"
)

func tracerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Send tracing events to a Redpanda Message Broker.").
		Fields(kafka.FranzConnectionFields()...).
		Fields(kafka.FranzProducerFields()...).
		Fields(
			service.NewStringField("topic").
				Default("otel-traces").
				Description("The name of the topic to emit spans to"),
			service.NewStringAnnotatedEnumField("format", map[string]string{
				exporter.SerializationFormatJSON.String():     "Emit in OTLP JSON Format",
				exporter.SerializationFormatProtobuf.String(): "Emit in OTLP Protobuf Format",
			}).
				Description("The serialization format for individual spans in the topic.").
				Default(exporter.SerializationFormatJSON.String()).
				Advanced(),
			service.NewStringField("service").
				Default("redpanda-connect").
				Description("The name of the service in traces."),
			service.NewStringMapField("tags").
				Description("A map of tags to add to all tracing spans.").
				Default(map[string]any{}).
				Advanced(),
			service.NewBoolField("use_redpanda_cloud_service_account").
				Description("Use the Redpanda Cloud service account for authentication. This is a Redpanda Cloud only feature that uses OAuth2 credentials provided via CLI flags (--x-redpanda-cloud-service-account-*). When enabled, explicit SASL configuration is not required.").
				Default(false).
				Advanced(),
			service.NewObjectField("sampling",
				service.NewBoolField("enabled").
					Description("Whether to enable sampling.").
					Default(false),
				service.NewFloatField("ratio").
					Description("Sets the ratio of traces to sample.").
					Examples(0.05, 0.85, 0.5).
					Optional()).
				Description("Settings for trace sampling. Sampling is recommended for high-volume production workloads."),
		)
}

func init() {
	service.MustRegisterOtelTracerProvider(
		"redpanda", tracerSpec(),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			c, err := tracerConfigFromParsed(conf, conf.Resources().Logger())
			if err != nil {
				return nil, err
			}
			return newTracer(c)
		})
}

type tracerSampleConfig struct {
	enabled bool
	ratio   float64
}

type tracer struct {
	serviceName   string
	engineVersion string
	tags          map[string]string
	sampling      tracerSampleConfig
	brokers       []string
	topic         string
	opts          []kgo.Opt
	format        exporter.SerializationFormat
}

func tracerConfigFromParsed(conf *service.ParsedConfig, logger *service.Logger) (*tracer, error) {
	serviceName, err := conf.FieldString("service")
	if err != nil {
		return nil, err
	}

	brokers, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}

	topic, err := conf.FieldString("topic")
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

	formatStr, err := conf.FieldString("format")
	if err != nil {
		return nil, err
	}
	var format exporter.SerializationFormat
	if formatStr == exporter.SerializationFormatJSON.String() {
		format = exporter.SerializationFormatJSON
	} else if formatStr == exporter.SerializationFormatProtobuf.String() {
		format = exporter.SerializationFormatProtobuf
	} else {
		return nil, fmt.Errorf("unknown `format` value: %q", formatStr)
	}

	useCloudServiceAccount, err := conf.FieldBool("use_redpanda_cloud_service_account")
	if err != nil {
		return nil, err
	}

	// Validate conflicting auth configuration before parsing
	if useCloudServiceAccount {
		if conf.Contains("sasl") {
			// Check if SASL was actually configured (not just the field existing)
			saslList, _ := conf.FieldObjectList("sasl")
			if len(saslList) > 0 {
				return nil, errors.New("use_redpanda_cloud_service_account cannot be used together with explicit sasl configuration")
			}
		}
	}

	connDeets, err := kafka.FranzConnectionDetailsFromConfig(conf, logger)
	if err != nil {
		return nil, err
	}

	producerOpts, err := kafka.FranzProducerOptsFromConfig(conf)
	if err != nil {
		return nil, err
	}

	// Build kafka options
	kafkaOpts := slices.Concat(connDeets.FranzOpts(), producerOpts)

	// Handle cloud service account authentication
	if useCloudServiceAccount {

		// Get the OAuth2 token source from the global service account singleton
		tokenSource, err := serviceaccount.GetTokenSource()
		if err != nil {
			return nil, fmt.Errorf("failed to get Redpanda Cloud service account token source: %w (this feature requires --x-redpanda-cloud-service-account-* CLI flags)", err)
		}

		// Create OAuth SASL mechanism using the token source
		oauthMech := oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			token, err := tokenSource.Token()
			if err != nil {
				return oauth.Auth{}, fmt.Errorf("failed to obtain OAuth2 token: %w", err)
			}
			return oauth.Auth{
				Token: token.AccessToken,
			}, nil
		})

		// Prepend the OAuth mechanism to kafka options
		kafkaOpts = append([]kgo.Opt{kgo.SASL(oauthMech)}, kafkaOpts...)
	}

	return &tracer{
		serviceName:   serviceName,
		topic:         topic,
		engineVersion: conf.EngineVersion(),
		tags:          tags,
		sampling:      sampling,
		brokers:       brokers,
		opts:          kafkaOpts,
		format:        format,
	}, nil
}

func sampleConfigFromParsed(conf *service.ParsedConfig) (tracerSampleConfig, error) {
	conf = conf.Namespace("sampling")
	enabled, err := conf.FieldBool("enabled")
	if err != nil {
		return tracerSampleConfig{}, err
	}

	var ratio float64
	if conf.Contains("ratio") {
		if ratio, err = conf.FieldFloat("ratio"); err != nil {
			return tracerSampleConfig{}, err
		}
	}

	return tracerSampleConfig{
		enabled: enabled,
		ratio:   ratio,
	}, nil
}

//------------------------------------------------------------------------------

func newTracer(config *tracer) (trace.TracerProvider, error) {
	var attrs []attribute.KeyValue
	for k, v := range config.tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	var res *resource.Resource
	if _, ok := config.tags[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String(config.serviceName))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := config.tags[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(config.engineVersion))
		}
		res = resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	}
	exporter, err := exporter.NewTraceExporter(
		exporter.WithBrokers(config.brokers...),
		exporter.WithTopic(config.topic),
		exporter.WithResource(res),
		exporter.WithSerializationFormat(config.format),
		exporter.WithKafkaOptions(config.opts...),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create trace exporter: %w", err)
	}
	var opts []tracesdk.TracerProviderOption
	opts = append(opts, tracesdk.WithBatcher(exporter))
	if config.sampling.enabled {
		opts = append(opts, tracesdk.WithSampler(tracesdk.TraceIDRatioBased(config.sampling.ratio)))
	}
	opts = append(
		opts,
		tracesdk.WithIDGenerator(tracing.NewIDGenerator()),
	)
	if res != nil {
		opts = append(opts, tracesdk.WithResource(res))
	}
	return tracesdk.NewTracerProvider(opts...), nil
}
