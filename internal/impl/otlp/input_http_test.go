// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
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

	pb "buf.build/gen/go/redpandadata/otel/protocolbuffers/go/redpanda/otel/v1"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
)

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

func TestHTTPInputAuth(t *testing.T) {
	const testToken = "test-secret-token-12345"

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
auth_token: "%s"
encoding: protobuf`, address, testToken)
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

	yamlConfig := fmt.Sprintf(`address: "%s"
encoding: protobuf`, address)
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
