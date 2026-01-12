// Copyright 2026 Redpanda Data, Inc.
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

package otlp_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
)

// createTestTracerProvider creates an OpenTelemetry TracerProvider configured to export to the given endpoint.
func createTestTracerProvider(ctx context.Context, endpoint string, opts ...otlptracegrpc.Option) (*sdktrace.TracerProvider, error) {
	defaultOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	}
	defaultOpts = append(defaultOpts, opts...)

	exporter, err := otlptracegrpc.New(ctx, defaultOpts...)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	return tp, nil
}

// createTestMeterProvider creates an OpenTelemetry MeterProvider configured to export to the given endpoint.
func createTestMeterProvider(ctx context.Context, endpoint string) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	return mp, nil
}

// createTestLoggerProvider creates an OpenTelemetry LoggerProvider configured to export to the given endpoint.
func createTestLoggerProvider(ctx context.Context, endpoint string) (*sdklog.LoggerProvider, error) {
	exporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(endpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	return lp, nil
}

func TestGRPCInputAuth(t *testing.T) {
	const testToken = "test-secret-token-grpc-67890"
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	pConf, err := otlp.GRPCInputSpec().ParseYAML(fmt.Sprintf(`
address: "%s"
auth_token: "%s"
`, address, testToken), nil)
	require.NoError(t, err)

	input, err := otlp.GRPCInputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	t.Run("missing_auth_metadata", func(t *testing.T) {
		// Create exporter without auth headers
		tp, err := createTestTracerProvider(t.Context(), address)
		require.NoError(t, err)
		defer tp.Shutdown(t.Context()) //nolint:errcheck

		tracer := tp.Tracer("test-service")
		_, span := tracer.Start(t.Context(), "test-span")
		span.End()

		// Try to flush - should fail with unauthenticated error
		err = tp.ForceFlush(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Unauthenticated")
	})

	t.Run("invalid_auth_token", func(t *testing.T) {
		// Create exporter with wrong token
		tp, err := createTestTracerProvider(t.Context(), address,
			otlptracegrpc.WithHeaders(map[string]string{
				"authorization": "Bearer wrong-token",
			}),
		)
		require.NoError(t, err)
		defer tp.Shutdown(t.Context()) //nolint:errcheck

		tracer := tp.Tracer("test-service")
		_, span := tracer.Start(t.Context(), "test-span")
		span.End()

		// Try to flush - should fail with unauthenticated error
		err = tp.ForceFlush(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Unauthenticated")
	})

	t.Run("malformed_auth_metadata", func(t *testing.T) {
		// Create exporter with malformed auth (missing "Bearer " prefix)
		tp, err := createTestTracerProvider(t.Context(), address,
			otlptracegrpc.WithHeaders(map[string]string{
				"authorization": testToken,
			}),
		)
		require.NoError(t, err)
		defer tp.Shutdown(t.Context()) //nolint:errcheck

		tracer := tp.Tracer("test-service")
		_, span := tracer.Start(t.Context(), "test-span")
		span.End()

		// Try to flush - should fail with unauthenticated error
		err = tp.ForceFlush(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Unauthenticated")
	})

	t.Run("valid_auth_token", func(t *testing.T) {
		// Create exporter with correct auth token
		tp, err := createTestTracerProvider(t.Context(), address,
			otlptracegrpc.WithHeaders(map[string]string{
				"authorization": "Bearer " + testToken,
			}),
		)
		require.NoError(t, err)
		defer tp.Shutdown(t.Context()) //nolint:errcheck

		received := make(chan service.MessageBatch, 1)
		readErr := make(chan error, 1)
		go func() {
			batch, aFn, err := input.ReadBatch(t.Context())
			aFn(t.Context(), nil) //nolint:errcheck

			if err != nil {
				readErr <- err
			} else {
				received <- batch
			}
		}()

		tracer := tp.Tracer("test-service")
		_, span := tracer.Start(t.Context(), "test-span")
		span.End()

		// Try to flush - should succeed
		err = tp.ForceFlush(t.Context())
		require.NoError(t, err)

		// Verify message was received
		select {
		case batch := <-received:
			require.NotEmpty(t, batch)
		case err := <-readErr:
			t.Fatalf("Error reading batch: %v", err)
		case <-time.After(grpcOpTimeout):
			t.Fatal("Timeout waiting for message")
		}
	})
}

func TestGRPCInputTraces(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testGRPCInput(t, address, otlp.SignalTypeTrace,
		func(ctx context.Context) error {
			tp, err := createTestTracerProvider(ctx, address)
			if err != nil {
				return err
			}
			defer tp.Shutdown(ctx) //nolint:errcheck

			// Create a tracer and generate a span with attributes and events
			tracer := tp.Tracer("grpc-test-service",
				trace.WithInstrumentationVersion("1.0.0"),
			)
			_, span := tracer.Start(ctx, "grpc-test-service-span")
			span.SetAttributes(
				attribute.String("http.method", "POST"),
				attribute.String("http.url", "/api/users"),
				attribute.Int64("http.status_code", 200),
				attribute.String("user.id", "12345"),
				attribute.Bool("cache.hit", true),
			)
			span.AddEvent("User authenticated", trace.WithAttributes(
				attribute.String("auth.method", "oauth2"),
				attribute.String("auth.provider", "google"),
			))
			span.AddEvent("Database query executed", trace.WithAttributes(
				attribute.String("db.system", "postgresql"),
				attribute.String("db.statement", "SELECT * FROM users WHERE id = ?"),
				attribute.Int64("db.rows_affected", 1),
			))
			span.End()

			// Force flush to ensure data is sent
			return tp.ForceFlush(ctx)
		},
		func(msgBytes []byte) {
			var span pb.Span
			require.NoError(t, proto.Unmarshal(msgBytes, &span))

			assert.Equal(t, "grpc-test-service-span", span.Name)
			assert.NotNil(t, span.Resource)
			assert.NotNil(t, span.Scope)

			// Validate resource attributes - should contain service.name
			foundService := false
			for _, kv := range span.Resource.Attributes {
				if kv.Key == "service.name" {
					foundService = true
					break
				}
			}
			assert.True(t, foundService, "service.name not found in resource attributes")

			// Validate span attributes
			attrMap := make(map[string]*pb.AnyValue)
			for _, kv := range span.Attributes {
				attrMap[kv.Key] = kv.Value
			}
			assert.Equal(t, "POST", attrMap["http.method"].GetStringValue())
			assert.Equal(t, "/api/users", attrMap["http.url"].GetStringValue())
			assert.Equal(t, int64(200), attrMap["http.status_code"].GetIntValue())
			assert.Equal(t, "12345", attrMap["user.id"].GetStringValue())
			assert.True(t, attrMap["cache.hit"].GetBoolValue())

			// Validate span events
			require.Len(t, span.Events, 2)
			assert.Equal(t, "User authenticated", span.Events[0].Name)
			assert.Equal(t, "Database query executed", span.Events[1].Name)
		},
	)
}

func TestGRPCInputLogs(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testGRPCInput(t, address, otlp.SignalTypeLog,
		func(ctx context.Context) error {
			lp, err := createTestLoggerProvider(ctx, address)
			if err != nil {
				return err
			}
			defer lp.Shutdown(ctx) //nolint:errcheck

			// Create a logger and emit a log record with attributes
			logger := lp.Logger("grpc-test-service")
			record := log.Record{}
			record.SetBody(log.StringValue("Test log message from grpc-test-service"))
			record.SetSeverity(log.SeverityInfo)
			record.SetSeverityText("INFO")
			record.AddAttributes(
				log.String("http.method", "POST"),
				log.String("http.url", "/api/users"),
				log.Int("http.status_code", 200),
				log.String("user.id", "12345"),
				log.String("request.id", "req-abc-123"),
				log.Float64("response.time_ms", 45.67),
			)
			logger.Emit(ctx, record)

			// Force flush to ensure data is sent
			return lp.ForceFlush(ctx)
		},
		func(msgBytes []byte) {
			var logRecord pb.LogRecord
			require.NoError(t, proto.Unmarshal(msgBytes, &logRecord))

			assert.NotNil(t, logRecord.Resource)
			assert.NotNil(t, logRecord.Scope)
			assert.Contains(t, logRecord.Body.GetStringValue(), "Test log message from grpc-test-service")
			assert.Equal(t, "INFO", logRecord.SeverityText)

			// Validate resource attributes
			foundService := false
			for _, kv := range logRecord.Resource.Attributes {
				if kv.Key == "service.name" {
					foundService = true
					break
				}
			}
			assert.True(t, foundService, "service.name not found in resource attributes")

			// Validate log attributes
			attrMap := make(map[string]*pb.AnyValue)
			for _, kv := range logRecord.Attributes {
				attrMap[kv.Key] = kv.Value
			}
			assert.Equal(t, "POST", attrMap["http.method"].GetStringValue())
			assert.Equal(t, "/api/users", attrMap["http.url"].GetStringValue())
			assert.Equal(t, int64(200), attrMap["http.status_code"].GetIntValue())
			assert.Equal(t, "12345", attrMap["user.id"].GetStringValue())
			assert.Equal(t, "req-abc-123", attrMap["request.id"].GetStringValue())
			assert.InDelta(t, 45.67, attrMap["response.time_ms"].GetDoubleValue(), 0.01)
		},
	)
}

func TestGRPCInputMetrics(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testGRPCInput(t, address, otlp.SignalTypeMetric,
		func(ctx context.Context) error {
			mp, err := createTestMeterProvider(ctx, address)
			if err != nil {
				return err
			}

			// Create a meter and record multiple metrics with attributes
			meter := mp.Meter("grpc-test-service",
				metric.WithInstrumentationVersion("1.0.0"),
			)

			// Counter metric
			counter, err := meter.Int64Counter("grpc-test-metric",
				metric.WithDescription("Number of requests processed"),
				metric.WithUnit("1"),
			)
			if err != nil {
				return err
			}
			counter.Add(ctx, 42, metric.WithAttributes(
				attribute.String("http.method", "POST"),
				attribute.String("http.route", "/api/users"),
				attribute.Int("http.status_code", 200),
			))

			// Histogram metric
			histogram, err := meter.Float64Histogram("request.duration",
				metric.WithDescription("Request duration in milliseconds"),
				metric.WithUnit("ms"),
			)
			if err != nil {
				return err
			}
			histogram.Record(ctx, 123.45, metric.WithAttributes(
				attribute.String("http.method", "POST"),
				attribute.String("http.route", "/api/users"),
			))

			// Gauge (UpDownCounter) metric
			upDownCounter, err := meter.Int64UpDownCounter("active.connections",
				metric.WithDescription("Number of active connections"),
				metric.WithUnit("1"),
			)
			if err != nil {
				return err
			}
			upDownCounter.Add(ctx, 5, metric.WithAttributes(
				attribute.String("connection.type", "websocket"),
			))

			// Shutdown forces all pending metrics to be exported immediately.
			// Unlike ForceFlush, Shutdown guarantees that all data is sent.
			return mp.Shutdown(ctx)
		},
		func(msgBytes []byte) {
			var metric pb.Metric
			require.NoError(t, proto.Unmarshal(msgBytes, &metric))

			assert.NotNil(t, metric.Resource)
			assert.NotNil(t, metric.Scope)
			assert.NotNil(t, metric.Data)

			// Validate resource attributes
			foundService := false
			for _, kv := range metric.Resource.Attributes {
				if kv.Key == "service.name" {
					foundService = true
					break
				}
			}
			assert.True(t, foundService, "service.name not found in resource attributes")

			// Validate metric based on name
			switch metric.Name {
			case "grpc-test-metric":
				assert.Equal(t, "Number of requests processed", metric.Description)
				assert.Equal(t, "1", metric.Unit)
				sum := metric.GetSum()
				require.NotNil(t, sum, "expected counter to have sum data")
				require.NotEmpty(t, sum.DataPoints)
				// Validate attributes on data point
				attrMap := make(map[string]*pb.AnyValue)
				for _, kv := range sum.DataPoints[0].Attributes {
					attrMap[kv.Key] = kv.Value
				}
				assert.Equal(t, "POST", attrMap["http.method"].GetStringValue())
				assert.Equal(t, "/api/users", attrMap["http.route"].GetStringValue())
				assert.Equal(t, int64(200), attrMap["http.status_code"].GetIntValue())

			case "request.duration":
				assert.Equal(t, "Request duration in milliseconds", metric.Description)
				assert.Equal(t, "ms", metric.Unit)
				histogram := metric.GetHistogram()
				require.NotNil(t, histogram, "expected histogram data")

			case "active.connections":
				assert.Equal(t, "Number of active connections", metric.Description)
				assert.Equal(t, "1", metric.Unit)
				sum := metric.GetSum()
				require.NotNil(t, sum, "expected gauge to have sum data")
			}
		},
	)
}

const grpcOpTimeout = 5 * time.Second

// testGRPCInput is a helper function to test gRPC input with different signal types.
func testGRPCInput(t *testing.T, address string, signalType otlp.SignalType, exportFn func(ctx context.Context) error, validateFn func(msgBytes []byte)) {
	t.Helper()

	pConf, err := otlp.GRPCInputSpec().ParseYAML(fmt.Sprintf(`
address: "%s"
`, address), nil)
	require.NoError(t, err)

	input, err := otlp.GRPCInputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	received := make(chan service.MessageBatch, 1)
	readErr := make(chan error, 1)
	go func() {
		batch, aFn, err := input.ReadBatch(t.Context())
		aFn(t.Context(), nil) //nolint:errcheck

		if err != nil {
			readErr <- err
		} else {
			received <- batch
		}
	}()

	// Give the input a moment to start listening
	time.Sleep(100 * time.Millisecond)

	// Send telemetry data using OpenTelemetry SDK exporters
	err = exportFn(t.Context())
	require.NoError(t, err)

	// Wait for message
	var batch service.MessageBatch
	select {
	case batch = <-received:
		// continue
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(grpcOpTimeout):
		t.Fatal("Timeout waiting for message")
	}

	// Assert batch content - expect protobuf messages
	require.NotEmpty(t, batch)

	// Validate each message
	for _, msg := range batch {
		// Check signal type metadata
		s, ok := msg.MetaGet(otlp.MetadataKeySignalType)
		require.True(t, ok)
		require.Equal(t, signalType.String(), s)

		// Unmarshal and validate message content
		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)
		validateFn(msgBytes)
	}
}
