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
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
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
)

// createTestTracerProviderHTTP creates an OpenTelemetry TracerProvider configured to export to the given endpoint via HTTP.
func createTestTracerProviderHTTP(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, error) {
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

// createTestMeterProviderHTTP creates an OpenTelemetry MeterProvider configured to export to the given endpoint via HTTP.
func createTestMeterProviderHTTP(ctx context.Context, endpoint string) (*sdkmetric.MeterProvider, error) {
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

// createTestLoggerProviderHTTP creates an OpenTelemetry LoggerProvider configured to export to the given endpoint via HTTP.
func createTestLoggerProviderHTTP(ctx context.Context, endpoint string) (*sdklog.LoggerProvider, error) {
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

func TestHTTPInputAuth(t *testing.T) {
	const testToken = "test-secret-token-12345"

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	pConf, err := otlp.HTTPInputSpec().ParseYAML(fmt.Sprintf(`
address: "%s"
auth_token: "%s"
`, address, testToken), nil)
	require.NoError(t, err)

	input, err := otlp.HTTPInputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	baseURL := "http://" + address

	t.Run("missing_auth_header", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")
		// No Authorization header

		client := &http.Client{Timeout: httpOpTimeout}
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

		client := &http.Client{Timeout: httpOpTimeout}
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

		client := &http.Client{Timeout: httpOpTimeout}
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

		client := &http.Client{Timeout: httpOpTimeout}
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

	pConf, err := otlp.HTTPInputSpec().ParseYAML(fmt.Sprintf(`
address: "%s"
`, address), nil)
	require.NoError(t, err)

	input, err := otlp.HTTPInputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	baseURL := "http://" + address

	t.Run("invalid_content_type", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{}")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/xml")

		client := &http.Client{Timeout: httpOpTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
	})

	t.Run("malformed_json", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("{invalid json")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: httpOpTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("malformed_protobuf", func(t *testing.T) {
		httpReq, err := http.NewRequestWithContext(t.Context(), "POST", baseURL+"/v1/traces", bytes.NewReader([]byte("invalid protobuf data")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/x-protobuf")

		client := &http.Client{Timeout: httpOpTimeout}
		resp, err := client.Do(httpReq)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestHTTPInputTraces(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testHTTPInput(t, address, otlp.SignalTypeTrace,
		func(ctx context.Context) error {
			tp, err := createTestTracerProviderHTTP(ctx, address)
			if err != nil {
				return err
			}
			defer tp.Shutdown(ctx) //nolint:errcheck

			// Create a tracer and generate a span with attributes and events
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

			// Force flush to ensure data is sent
			return tp.ForceFlush(ctx)
		},
		func(msgBytes []byte) {
			var span pb.Span
			require.NoError(t, proto.Unmarshal(msgBytes, &span))

			assert.Equal(t, "http-test-service-span", span.Name)
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
			assert.Equal(t, "GET", attrMap["http.method"].GetStringValue())
			assert.Equal(t, "/api/products", attrMap["http.url"].GetStringValue())
			assert.Equal(t, int64(200), attrMap["http.status_code"].GetIntValue())
			assert.Equal(t, "54321", attrMap["user.id"].GetStringValue())
			assert.False(t, attrMap["cache.hit"].GetBoolValue())

			// Validate span events
			require.Len(t, span.Events, 2)
			assert.Equal(t, "Cache miss", span.Events[0].Name)
			assert.Equal(t, "Database query", span.Events[1].Name)
		},
	)
}

func TestHTTPInputLogs(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testHTTPInput(t, address, otlp.SignalTypeLog,
		func(ctx context.Context) error {
			lp, err := createTestLoggerProviderHTTP(ctx, address)
			if err != nil {
				return err
			}
			defer lp.Shutdown(ctx) //nolint:errcheck

			// Create a logger and emit a log record with attributes
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

			// Force flush to ensure data is sent
			return lp.ForceFlush(ctx)
		},
		func(msgBytes []byte) {
			var logRecord pb.LogRecord
			require.NoError(t, proto.Unmarshal(msgBytes, &logRecord))

			assert.NotNil(t, logRecord.Resource)
			assert.NotNil(t, logRecord.Scope)
			assert.Contains(t, logRecord.Body.GetStringValue(), "Test log message from http-test-service")
			assert.Equal(t, "WARN", logRecord.SeverityText)

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
			assert.Equal(t, "GET", attrMap["http.method"].GetStringValue())
			assert.Equal(t, "/api/products", attrMap["http.url"].GetStringValue())
			assert.Equal(t, int64(404), attrMap["http.status_code"].GetIntValue())
			assert.Equal(t, "54321", attrMap["user.id"].GetStringValue())
			assert.Equal(t, "req-xyz-789", attrMap["request.id"].GetStringValue())
			assert.InDelta(t, 23.45, attrMap["response.time_ms"].GetDoubleValue(), 0.01)
		},
	)
}

func TestHTTPInputMetrics(t *testing.T) {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	testHTTPInput(t, address, otlp.SignalTypeMetric,
		func(ctx context.Context) error {
			mp, err := createTestMeterProviderHTTP(ctx, address)
			if err != nil {
				return err
			}

			// Create a meter and record multiple metrics with attributes
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
			case "http-test-metric":
				assert.Equal(t, "Number of HTTP requests", metric.Description)
				assert.Equal(t, "1", metric.Unit)
				sum := metric.GetSum()
				require.NotNil(t, sum, "expected counter to have sum data")
				require.NotEmpty(t, sum.DataPoints)
				// Validate attributes on data point
				attrMap := make(map[string]*pb.AnyValue)
				for _, kv := range sum.DataPoints[0].Attributes {
					attrMap[kv.Key] = kv.Value
				}
				assert.Equal(t, "GET", attrMap["http.method"].GetStringValue())
				assert.Equal(t, "/api/products", attrMap["http.route"].GetStringValue())
				assert.Equal(t, int64(200), attrMap["http.status_code"].GetIntValue())

			case "http.request.duration":
				assert.Equal(t, "HTTP request duration in milliseconds", metric.Description)
				assert.Equal(t, "ms", metric.Unit)
				histogram := metric.GetHistogram()
				require.NotNil(t, histogram, "expected histogram data")
			}
		},
	)
}

const httpOpTimeout = 5 * time.Second

// testHTTPInput is a helper function to test HTTP input with different signal types.
func testHTTPInput(t *testing.T, address string, signalType otlp.SignalType, exportFn func(ctx context.Context) error, validateFn func(msgBytes []byte)) {
	t.Helper()

	pConf, err := otlp.HTTPInputSpec().ParseYAML(fmt.Sprintf(`
address: "%s"
`, address), nil)
	require.NoError(t, err)

	input, err := otlp.HTTPInputFromParsed(pConf, service.MockResources())
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
	// Use a fresh context with timeout for export operations to avoid test context deadline issues
	exportCtx, cancel := context.WithTimeout(context.Background(), httpOpTimeout)
	defer cancel()
	err = exportFn(exportCtx)
	require.NoError(t, err)

	// Wait for message
	var batch service.MessageBatch
	select {
	case batch = <-received:
		// continue
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(httpOpTimeout):
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
