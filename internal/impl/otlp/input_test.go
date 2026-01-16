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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const opTimeout = 5 * time.Second

func newGRPCTestTracerProvider(ctx context.Context, endpoint string, opts ...otlptracegrpc.Option) (*sdktrace.TracerProvider, error) {
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

func newGRPCTestLoggerProvider(ctx context.Context, endpoint string) (*sdklog.LoggerProvider, error) {
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

func newGRPCTestMeterProvider(ctx context.Context, endpoint string) (*sdkmetric.MeterProvider, error) {
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

func newHTTPTestTracerProvider(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	return tp, nil
}

func newHTTPTestLoggerProvider(ctx context.Context, endpoint string) (*sdklog.LoggerProvider, error) {
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(endpoint),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	return lp, nil
}

func newHTTPTestMeterProvider(ctx context.Context, endpoint string) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	return mp, nil
}

func TestGRPCInputAuth(t *testing.T) {
	const testToken = "test-secret-token-grpc-67890"
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
auth_token: "%s"`, address, testToken)
	input := startInput(t, otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed, yamlConfig)
	time.Sleep(100 * time.Millisecond)

	t.Run("missing_auth_metadata", func(t *testing.T) {
		// Create exporter without auth headers
		tp, err := newGRPCTestTracerProvider(t.Context(), address)
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
		tp, err := newGRPCTestTracerProvider(t.Context(), address,
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
		tp, err := newGRPCTestTracerProvider(t.Context(), address,
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
		tp, err := newGRPCTestTracerProvider(t.Context(), address,
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
		case <-time.After(opTimeout):
			t.Fatal("Timeout waiting for message")
		}
	})
}

func TestHTTPInputAuth(t *testing.T) {
	const testToken = "test-secret-token-12345"

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
auth_token: "%s"`, address, testToken)
	startInput(t, otlp.HTTPInputSpec(), otlp.HTTPInputFromParsed, yamlConfig)

	baseURL := "http://" + address

	t.Run("missing_auth_header", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		// No Authorization header

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("invalid_auth_token", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer wrong-token")

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("malformed_auth_header", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", testToken) // Missing "Bearer " prefix

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("valid_auth_token", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+testToken)

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should not be unauthorized (might be 400 for empty body, but not 401)
		assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

func TestHTTPInputEdgeCases(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"`, address)
	startInput(t, otlp.HTTPInputSpec(), otlp.HTTPInputFromParsed, yamlConfig)

	baseURL := "http://" + address

	t.Run("invalid_content_type", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/xml")

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
	})

	t.Run("malformed_json", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{invalid json")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("malformed_protobuf", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("invalid protobuf data")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/x-protobuf")

		client := &http.Client{Timeout: opTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestGRPCInput(t *testing.T) {
	tests := []struct {
		name       string
		signalType otlp.SignalType
		exportFn   func(ctx context.Context, address string) error
		validateFn func(t *testing.T, msgBytes []byte)
	}{
		{
			name:       "traces",
			signalType: otlp.SignalTypeTrace,
			exportFn: func(ctx context.Context, address string) error {
				tp, err := newGRPCTestTracerProvider(ctx, address)
				if err != nil {
					return err
				}
				defer tp.Shutdown(ctx) //nolint:errcheck

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

				return tp.ForceFlush(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var span pb.Span
				require.NoError(t, proto.Unmarshal(msgBytes, &span))

				assert.Equal(t, "grpc-test-service-span", span.Name)
				assert.NotNil(t, span.Resource)
				assert.NotNil(t, span.Scope)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(span.Resource.Attributes, "service.name"))

				// Validate span attributes
				attrs := attrMap(span.Attributes)
				assert.Equal(t, "POST", attrs["http.method"].GetStringValue())
				assert.Equal(t, "/api/users", attrs["http.url"].GetStringValue())
				assert.Equal(t, int64(200), attrs["http.status_code"].GetIntValue())
				assert.Equal(t, "12345", attrs["user.id"].GetStringValue())
				assert.True(t, attrs["cache.hit"].GetBoolValue())

				// Validate span events
				require.Len(t, span.Events, 2)
				assert.Equal(t, "User authenticated", span.Events[0].Name)
				assert.Equal(t, "Database query executed", span.Events[1].Name)
			},
		},
		{
			name:       "logs",
			signalType: otlp.SignalTypeLog,
			exportFn: func(ctx context.Context, address string) error {
				lp, err := newGRPCTestLoggerProvider(ctx, address)
				if err != nil {
					return err
				}
				defer lp.Shutdown(ctx) //nolint:errcheck

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

				return lp.ForceFlush(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var logRecord pb.LogRecord
				require.NoError(t, proto.Unmarshal(msgBytes, &logRecord))

				assert.NotNil(t, logRecord.Resource)
				assert.NotNil(t, logRecord.Scope)
				assert.Contains(t, logRecord.Body.GetStringValue(), "Test log message from grpc-test-service")
				assert.Equal(t, "INFO", logRecord.SeverityText)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(logRecord.Resource.Attributes, "service.name"))

				// Validate log attributes
				attrs := attrMap(logRecord.Attributes)
				assert.Equal(t, "POST", attrs["http.method"].GetStringValue())
				assert.Equal(t, "/api/users", attrs["http.url"].GetStringValue())
				assert.Equal(t, int64(200), attrs["http.status_code"].GetIntValue())
				assert.Equal(t, "12345", attrs["user.id"].GetStringValue())
				assert.Equal(t, "req-abc-123", attrs["request.id"].GetStringValue())
				assert.InDelta(t, 45.67, attrs["response.time_ms"].GetDoubleValue(), 0.01)
			},
		},
		{
			name:       "metrics",
			signalType: otlp.SignalTypeMetric,
			exportFn: func(ctx context.Context, address string) error {
				mp, err := newGRPCTestMeterProvider(ctx, address)
				if err != nil {
					return err
				}

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

				return mp.Shutdown(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var metric pb.Metric
				require.NoError(t, proto.Unmarshal(msgBytes, &metric))

				assert.NotNil(t, metric.Resource)
				assert.NotNil(t, metric.Scope)
				assert.NotNil(t, metric.Data)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(metric.Resource.Attributes, "service.name"))

				// Validate metric based on name
				switch metric.Name {
				case "grpc-test-metric":
					assert.Equal(t, "Number of requests processed", metric.Description)
					assert.Equal(t, "1", metric.Unit)
					sum := metric.GetSum()
					require.NotNil(t, sum, "expected counter to have sum data")
					require.NotEmpty(t, sum.DataPoints)
					attrs := attrMap(sum.DataPoints[0].Attributes)
					assert.Equal(t, "POST", attrs["http.method"].GetStringValue())
					assert.Equal(t, "/api/users", attrs["http.route"].GetStringValue())
					assert.Equal(t, int64(200), attrs["http.status_code"].GetIntValue())

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
		},
	}

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Helper()
			testInput(t, address, tt.signalType, tt.exportFn, tt.validateFn,
				otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed)
		})
	}
}

func TestHTTPInput(t *testing.T) {
	tests := []struct {
		name       string
		signalType otlp.SignalType
		exportFn   func(ctx context.Context, address string) error
		validateFn func(t *testing.T, msgBytes []byte)
	}{
		{
			name:       "traces",
			signalType: otlp.SignalTypeTrace,
			exportFn: func(ctx context.Context, address string) error {
				tp, err := newHTTPTestTracerProvider(ctx, address)
				if err != nil {
					return err
				}
				defer tp.Shutdown(ctx) //nolint:errcheck

				tracer := tp.Tracer("http-test-service",
					trace.WithInstrumentationVersion("1.0.0"),
				)
				_, span := tracer.Start(ctx, "http-test-service-span")
				span.SetAttributes(
					attribute.String("http.method", "GET"),
					attribute.String("http.url", "/api/products"),
					attribute.Int64("http.status_code", 200),
					attribute.String("user.id", "54321"),
					attribute.Bool("cache.hit", false),
				)
				span.AddEvent("Cache miss", trace.WithAttributes(
					attribute.String("cache.key", "product:123"),
				))
				span.AddEvent("Database query", trace.WithAttributes(
					attribute.String("db.system", "mysql"),
					attribute.Int64("db.rows_returned", 1),
				))
				span.End()

				return tp.ForceFlush(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var span pb.Span
				require.NoError(t, proto.Unmarshal(msgBytes, &span))

				assert.Equal(t, "http-test-service-span", span.Name)
				assert.NotNil(t, span.Resource)
				assert.NotNil(t, span.Scope)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(span.Resource.Attributes, "service.name"))

				// Validate span attributes
				attrs := attrMap(span.Attributes)
				assert.Equal(t, "GET", attrs["http.method"].GetStringValue())
				assert.Equal(t, "/api/products", attrs["http.url"].GetStringValue())
				assert.Equal(t, int64(200), attrs["http.status_code"].GetIntValue())
				assert.Equal(t, "54321", attrs["user.id"].GetStringValue())
				assert.False(t, attrs["cache.hit"].GetBoolValue())

				// Validate span events
				require.Len(t, span.Events, 2)
				assert.Equal(t, "Cache miss", span.Events[0].Name)
				assert.Equal(t, "Database query", span.Events[1].Name)
			},
		},
		{
			name:       "logs",
			signalType: otlp.SignalTypeLog,
			exportFn: func(ctx context.Context, address string) error {
				lp, err := newHTTPTestLoggerProvider(ctx, address)
				if err != nil {
					return err
				}
				defer lp.Shutdown(ctx) //nolint:errcheck

				logger := lp.Logger("http-test-service")
				record := log.Record{}
				record.SetBody(log.StringValue("Test log message from http-test-service"))
				record.SetSeverity(log.SeverityWarn)
				record.SetSeverityText("WARN")
				record.AddAttributes(
					log.String("http.method", "GET"),
					log.String("http.url", "/api/products"),
					log.Int("http.status_code", 404),
					log.String("user.id", "54321"),
					log.String("request.id", "req-xyz-789"),
					log.Float64("response.time_ms", 23.45),
				)
				logger.Emit(ctx, record)

				return lp.ForceFlush(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var logRecord pb.LogRecord
				require.NoError(t, proto.Unmarshal(msgBytes, &logRecord))

				assert.NotNil(t, logRecord.Resource)
				assert.NotNil(t, logRecord.Scope)
				assert.Contains(t, logRecord.Body.GetStringValue(), "Test log message from http-test-service")
				assert.Equal(t, "WARN", logRecord.SeverityText)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(logRecord.Resource.Attributes, "service.name"))

				// Validate log attributes
				attrs := attrMap(logRecord.Attributes)
				assert.Equal(t, "GET", attrs["http.method"].GetStringValue())
				assert.Equal(t, "/api/products", attrs["http.url"].GetStringValue())
				assert.Equal(t, int64(404), attrs["http.status_code"].GetIntValue())
				assert.Equal(t, "54321", attrs["user.id"].GetStringValue())
				assert.Equal(t, "req-xyz-789", attrs["request.id"].GetStringValue())
				assert.InDelta(t, 23.45, attrs["response.time_ms"].GetDoubleValue(), 0.01)
			},
		},
		{
			name:       "metrics",
			signalType: otlp.SignalTypeMetric,
			exportFn: func(ctx context.Context, address string) error {
				mp, err := newHTTPTestMeterProvider(ctx, address)
				if err != nil {
					return err
				}

				meter := mp.Meter("http-test-service",
					metric.WithInstrumentationVersion("1.0.0"),
				)

				// Counter metric
				counter, err := meter.Int64Counter("http-test-metric",
					metric.WithDescription("Number of HTTP requests"),
					metric.WithUnit("1"),
				)
				if err != nil {
					return err
				}
				counter.Add(ctx, 100, metric.WithAttributes(
					attribute.String("http.method", "GET"),
					attribute.String("http.route", "/api/products"),
					attribute.Int("http.status_code", 200),
				))

				// Histogram metric
				histogram, err := meter.Float64Histogram("http.request.duration",
					metric.WithDescription("HTTP request duration in milliseconds"),
					metric.WithUnit("ms"),
				)
				if err != nil {
					return err
				}
				histogram.Record(ctx, 234.56, metric.WithAttributes(
					attribute.String("http.method", "GET"),
					attribute.String("http.route", "/api/products"),
				))

				return mp.Shutdown(ctx)
			},
			validateFn: func(t *testing.T, msgBytes []byte) {
				var metric pb.Metric
				require.NoError(t, proto.Unmarshal(msgBytes, &metric))

				assert.NotNil(t, metric.Resource)
				assert.NotNil(t, metric.Scope)
				assert.NotNil(t, metric.Data)

				// Validate resource attributes
				assert.NotEmpty(t, attrGet(metric.Resource.Attributes, "service.name"))

				// Validate metric based on name
				switch metric.Name {
				case "http-test-metric":
					assert.Equal(t, "Number of HTTP requests", metric.Description)
					assert.Equal(t, "1", metric.Unit)
					sum := metric.GetSum()
					require.NotNil(t, sum, "expected counter to have sum data")
					require.NotEmpty(t, sum.DataPoints)
					attrs := attrMap(sum.DataPoints[0].Attributes)
					assert.Equal(t, "GET", attrs["http.method"].GetStringValue())
					assert.Equal(t, "/api/products", attrs["http.route"].GetStringValue())
					assert.Equal(t, int64(200), attrs["http.status_code"].GetIntValue())

				case "http.request.duration":
					assert.Equal(t, "HTTP request duration in milliseconds", metric.Description)
					assert.Equal(t, "ms", metric.Unit)
					histogram := metric.GetHistogram()
					require.NotNil(t, histogram, "expected histogram data")
				}
			},
		},
	}

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			testInput(t, address, tc.signalType, tc.exportFn, tc.validateFn,
				otlp.HTTPInputSpec(), otlp.HTTPInputFromParsed)
		})
	}
}

// testInput is a unified helper function to test inputs with different signal
// types and protocols.
func testInput(
	t *testing.T,
	address string,
	signalType otlp.SignalType,
	exportFn func(ctx context.Context, address string) error,
	validateFn func(t *testing.T, msgBytes []byte),
	inputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	inputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
) {
	t.Helper()

	yamlConfig := fmt.Sprintf(`address: "%s"`, address)
	input := startInput(t, inputSpec, inputCtor, yamlConfig)

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
	time.Sleep(100 * time.Millisecond)

	// Export data
	require.NoError(t, exportFn(t.Context(), address))

	// Wait for message
	var batch service.MessageBatch
	select {
	case batch = <-received:
		// continue
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(opTimeout):
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
		validateFn(t, msgBytes)
	}
}

// startInput is a helper that creates, connects, and returns an input with cleanup.
func startInput(
	t *testing.T,
	inputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	inputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
	yamlConfig string,
) service.BatchInput {
	t.Helper()

	pConf, err := inputSpec.ParseYAML(yamlConfig, nil)
	require.NoError(t, err)

	res := service.MockResources()
	license.InjectTestService(res)
	input, err := inputCtor(pConf, res)
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	return input
}
