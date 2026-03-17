// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/license"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/redpanda"
)

func producerConfig(transport string, encoding Encoding, broker, srURL, topic string) string {
	port := "4318"

	inputType := "otlp_http"
	if transport == "grpc" {
		port = "4317"
		inputType = "otlp_grpc"
	}

	return fmt.Sprintf(`
logger:
  level: DEBUG

input:
  %s:
    address: "0.0.0.0:%s"
    encoding: "%s"
    schema_registry:
      url: "%s"

output:
  redpanda:
    seed_brokers: ["%s"]
    topic: "%s"
    max_in_flight: 1
    batching:
      count: 10
    metadata:
      include_patterns: ["otel_.*"]
`, inputType, port, encoding, srURL, broker, topic)
}

func consumerConfig(transport, broker, srURL, topic, collectorEndpoint string) string {
	var outputType, outputConfig string
	if transport == "grpc" {
		outputType = "otlp_grpc"
		outputConfig = fmt.Sprintf(`  %s:
    endpoint: "%s"`, outputType, collectorEndpoint)
	} else {
		outputType = "otlp_http"
		outputConfig = fmt.Sprintf(`  %s:
    endpoint: "http://%s"
    content_type: "json"`, outputType, collectorEndpoint)
	}

	return fmt.Sprintf(`
logger:
  level: DEBUG

input:
  redpanda:
    seed_brokers: ["%s"]
    topics: ["%s"]
    consumer_group: "otlp-integration-test"
    start_from_oldest: true

pipeline:
  processors:
    - schema_registry_decode:
        url: "%s"

output:
%s
`, broker, topic, srURL, outputConfig)
}

func otelgenCommand(signalType SignalType, transport string, rate int, duration time.Duration) []string {
	cmd := []string{
		signalType.String() + "s", // telemetrygen expects plural forms: traces, logs, metrics
		"--rate", fmt.Sprintf("%d", rate),
		"--duration", duration.String(),
		"--workers", "1",
		"--otlp-insecure",
	}
	if transport == "grpc" {
		cmd = append(cmd, "--otlp-endpoint", "host.docker.internal:4317")
	} else {
		cmd = append(cmd, "--otlp-http", "--otlp-endpoint", "host.docker.internal:4318")
	}

	return cmd
}

var (
	soakDuration = flag.Duration("soak-duration", 15*time.Second, "Duration for soak test")
	soakRate     = flag.Int("soak-rate", 100, "Rate of messages per second for soak test")
)

func TestIntegrationOTLPWithSchemaRegistry(t *testing.T) {
	integration.CheckSkip(t)

	tests := []struct {
		signalType SignalType
		encoding   Encoding
		transport  string
	}{
		{SignalTypeTrace, EncodingJSON, "http"},
		{SignalTypeTrace, EncodingProtobuf, "http"},
		{SignalTypeTrace, EncodingJSON, "grpc"},
		{SignalTypeTrace, EncodingProtobuf, "grpc"},
		{SignalTypeLog, EncodingJSON, "http"},
		{SignalTypeLog, EncodingProtobuf, "http"},
		{SignalTypeLog, EncodingJSON, "grpc"},
		{SignalTypeLog, EncodingProtobuf, "grpc"},
		{SignalTypeMetric, EncodingJSON, "http"},
		{SignalTypeMetric, EncodingProtobuf, "http"},
		{SignalTypeMetric, EncodingJSON, "grpc"},
		{SignalTypeMetric, EncodingProtobuf, "grpc"},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s_%s_%s", tc.signalType, tc.transport, tc.encoding), func(t *testing.T) {
			t.Log("Given: Redpanda with Schema Registry")
			seed, srURL := startRedpandaWithSchemaRegistry(t)
			t.Logf("Redpanda broker: %s", seed)
			t.Logf("Schema Registry: %s", srURL)

			topic := fmt.Sprintf("otlp-%s-%s-%s", tc.signalType, tc.encoding, tc.transport)
			t.Logf("And: topic %s is created", topic)
			createTopic(t, seed, topic)

			t.Log("And: OTel Collector")
			collectorHTTP, collectorGRPC, collectorContainer := startOtelCollectorContainerWithDebugExporter(t, tc.signalType)
			t.Logf("OTel Collector endpoints - HTTP: %s, gRPC: %s", collectorHTTP, collectorGRPC)

			t.Log("When: generating telemetry data and sending to Redpanda via Benthos pipeline")
			ps := startStream(t, producerConfig(tc.transport, tc.encoding, seed, srURL, topic))
			runOtelgen(t, otelgenCommand(tc.signalType, tc.transport, *soakRate, *soakDuration))
			require.NoError(t, ps.StopWithin(3*time.Second))

			t.Log("And: reading from Redpanda and sending to OTel Collector via pipeline")
			collectorEndpoint := collectorHTTP
			if tc.transport == "grpc" {
				collectorEndpoint = collectorGRPC
			}
			cs := startStream(t, consumerConfig(tc.transport, seed, srURL, topic, collectorEndpoint))

			t.Log("Then: OTel Collector should eventually contain expected data")
			assert.Eventually(t, func() bool {
				expected := *soakRate * int(*soakDuration) / int(time.Second)
				tolerance := 0.2 // 20% tolerance for batching and timing

				n := countCollectedRows(t, collectorContainer, tc.signalType)
				t.Logf("Current count: %d, expected: %d (±%.0f%%)", n, expected, tolerance*100)

				// Check if count is within acceptable range
				lower := float64(expected) * (1 - tolerance)
				upper := float64(expected) * (1 + tolerance)
				return float64(n) >= lower && float64(n) <= upper
			}, 30*time.Second, 1*time.Second, "Expected signal count not reached in time")

			require.NoError(t, cs.StopWithin(3*time.Second))
		})
	}
}

func startRedpandaWithSchemaRegistry(t *testing.T) (brokers, srURL string) {
	t.Helper()

	container, err := redpanda.Run(t.Context(), "docker.redpanda.com/redpandadata/redpanda:latest")
	require.NoError(t, err, "failed to start redpanda container")
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	brokers, err = container.KafkaSeedBroker(t.Context())
	require.NoError(t, err, "failed to get kafka seed broker")
	srURL, err = container.SchemaRegistryAddress(t.Context())
	require.NoError(t, err, "failed to get schema registry address")

	return
}

func createTopic(t *testing.T, seed, topic string) {
	t.Log("When: Creating topic with single partition")
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(seed),
	)
	require.NoError(t, err)
	defer kafkaClient.Close()

	adminClient := kadm.NewClient(kafkaClient)
	_, err = adminClient.CreateTopics(t.Context(), 1, 1, nil, topic)
	require.NoError(t, err, "Failed to create topic")
}

func runOtelgen(t *testing.T, cmd []string) {
	ctx := t.Context()

	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      "ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest",
			Cmd:        cmd,
			WaitingFor: wait.ForExit().WithExitTimeout((*soakDuration) + 30*time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)

	state, err := container.State(ctx)
	require.NoError(t, err)

	logs, err := container.Logs(ctx)
	require.NoError(t, err)
	defer logs.Close()

	b, err := io.ReadAll(logs)
	require.NoError(t, err)
	if len(b) > 0 {
		t.Logf("otelgen logs:\n%s", string(b))
	}
	require.Equal(t, 0, state.ExitCode, "otelgen should complete successfully")
}

func startOtelCollectorContainerWithDebugExporter(t *testing.T, sig SignalType) (httpEndpoint, grpcEndpoint string, container testcontainers.Container) {
	t.Helper()

	conf := fmt.Sprintf(`
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  debug:
    verbosity: detailed
    sampling_initial: 1000
    sampling_thereafter: 1000

service:
  pipelines:
    %ss:
      receivers: [otlp]
      exporters: [debug]
`, sig.String())

	req := testcontainers.ContainerRequest{
		Image:        "otel/opentelemetry-collector-contrib:latest",
		ExposedPorts: []string{"4318/tcp", "4317/tcp"},
		WaitingFor:   wait.ForLog("Everything is ready").WithStartupTimeout(30 * time.Second),
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "",
				ContainerFilePath: "/etc/otel-config.yaml",
				FileMode:          0o644,
				Reader:            io.NopCloser(strings.NewReader(conf)),
			},
		},
		Cmd: []string{"--config=/etc/otel-config.yaml"},
	}

	ctx := t.Context()

	// Start container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate collector: %v", err)
		}
	})

	// Get mapped ports
	httpPort, err := container.MappedPort(ctx, "4318")
	require.NoError(t, err)
	grpcPort, err := container.MappedPort(ctx, "4317")
	require.NoError(t, err)

	httpEndpoint = fmt.Sprintf("localhost:%s", httpPort.Port())
	grpcEndpoint = fmt.Sprintf("localhost:%s", grpcPort.Port())
	return
}

func countCollectedRows(t *testing.T, container testcontainers.Container, signalType SignalType) int {
	t.Helper()

	ctx := t.Context()

	r, err := container.Logs(ctx)
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)

	// Count signal occurrences in debug exporter output
	// The debug exporter logs each signal with patterns like:
	// "Span #0" for traces, "LogRecord #0" for logs, "Metric #0" for metrics
	var signalPattern []byte
	switch signalType {
	case SignalTypeTrace:
		signalPattern = []byte("Span #")
	case SignalTypeLog:
		signalPattern = []byte("LogRecord #")
	case SignalTypeMetric:
		signalPattern = []byte("Metric #")
	}
	return bytes.Count(b, signalPattern)
}

func startStream(t *testing.T, confYAML string) *service.Stream {
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(confYAML))
	stream, err := sb.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("Pipeline error: %v", err)
		}
		t.Log("Pipeline shutdown")
	}()
	t.Cleanup(func() {
		if err := stream.StopWithin(3 * time.Second); err != nil {
			t.Logf("Failed to stop producer: %v", err)
		}
	})

	return stream
}
