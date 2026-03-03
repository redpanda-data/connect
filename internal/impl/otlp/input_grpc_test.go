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

	pb "buf.build/gen/go/redpandadata/otel/protocolbuffers/go/redpanda/otel/v1"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/gateway/gatewaytest"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
)

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

func TestGRPCInputAuth(t *testing.T) {
	const testToken = "test-secret-token-grpc-67890"
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
auth_token: "%s"
encoding: protobuf`, address, testToken)
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

func TestIntegrationGRPCInputAuthz(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: mockoidc provider")
	mockOIDC, issuerURL := gatewaytest.SetupMockOIDC(t)

	t.Log("And: JWT environment variables configured")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", issuerURL)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", authzAudience)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", authzOrgID)

	t.Log("And: OTLP gRPC input with allow_all policy")
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
encoding: protobuf`, address)
	input := startInput(t, otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed, yamlConfig,
		setupAuthz(authzGRPCResourceName, "testdata/policies/allow_all_grpc.yaml"))
	time.Sleep(100 * time.Millisecond)

	t.Log("And: User with valid token and permissions")
	user := &gatewaytest.RedpandaUser{
		Subject: "test-user",
		Email:   authzEmail,
		OrgID:   authzOrgID,
	}
	token := gatewaytest.AccessToken(t, mockOIDC, user)

	t.Log("When: OTLP gRPC client sends traces with valid JWT")
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

	tp, err := newGRPCTestTracerProvider(t.Context(), address,
		otlptracegrpc.WithHeaders(map[string]string{
			"authorization": "Bearer " + token,
		}),
	)
	require.NoError(t, err)
	defer tp.Shutdown(t.Context()) //nolint:errcheck

	tracer := tp.Tracer("authz-test-service")
	_, span := tracer.Start(t.Context(), "authz-test-span")
	span.SetAttributes(attribute.String("test.key", "test-value"))
	span.End()

	err = tp.ForceFlush(t.Context())
	require.NoError(t, err)

	t.Log("Then: Message is received successfully")
	select {
	case batch := <-received:
		require.NotEmpty(t, batch)
		t.Logf("Received batch with %d messages", len(batch))
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(opTimeout):
		t.Fatal("Timeout waiting for message")
	}
}

func TestGRPCInputAuthzUnauthenticated(t *testing.T) {
	integration.CheckSkip(t)

	t.Log("Given: mockoidc provider")
	_, issuerURL := gatewaytest.SetupMockOIDC(t)

	t.Log("And: JWT environment variables configured")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", issuerURL)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", authzAudience)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", authzOrgID)

	t.Log("And: OTLP gRPC input with allow_all policy")
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
encoding: protobuf`, address)
	startInput(t, otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed, yamlConfig,
		setupAuthz(authzGRPCResourceName, "testdata/policies/allow_all_grpc.yaml"))
	time.Sleep(100 * time.Millisecond)

	tests := []struct {
		name    string
		headers map[string]string
	}{
		{
			name:    "missing_token",
			headers: map[string]string{},
		},
		{
			name: "invalid_token",
			headers: map[string]string{
				"authorization": "Bearer invalid-token",
			},
		},
		{
			name: "malformed_auth_header",
			headers: map[string]string{
				"authorization": "invalid-format",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tp, err := newGRPCTestTracerProvider(t.Context(), address,
				otlptracegrpc.WithHeaders(tc.headers),
			)
			require.NoError(t, err)
			defer tp.Shutdown(t.Context()) //nolint:errcheck

			tracer := tp.Tracer("unauthenticated-service")
			_, span := tracer.Start(t.Context(), "unauthenticated-span")
			span.End()

			err = tp.ForceFlush(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "Unauthenticated")
		})
	}
}

func TestIntegrationGRPCInputAuthz_WrongOrg(t *testing.T) {
	integration.CheckSkip(t)

	const wrongOrgID = "wrong-org"

	t.Log("Given: mockoidc provider")
	mockOIDC, issuerURL := gatewaytest.SetupMockOIDC(t)

	t.Log("And: JWT environment variables configured")
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ISSUER_URL", issuerURL)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_AUDIENCE", authzAudience)
	t.Setenv("REDPANDA_CLOUD_GATEWAY_JWT_ORGANIZATION_ID", authzOrgID)

	t.Log("And: OTLP gRPC input with allow_all policy")
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	address := "127.0.0.1:" + strconv.Itoa(port)

	yamlConfig := fmt.Sprintf(`address: "%s"
encoding: protobuf`, address)
	startInput(t, otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed, yamlConfig,
		setupAuthz(authzGRPCResourceName, "testdata/policies/allow_all_grpc.yaml"))
	time.Sleep(100 * time.Millisecond)

	t.Log("And: User with token from wrong organization")
	user := &gatewaytest.RedpandaUser{
		Subject: "test-user",
		Email:   authzEmail,
		OrgID:   wrongOrgID,
	}
	token := gatewaytest.AccessToken(t, mockOIDC, user)

	t.Log("When: OTLP gRPC client sends traces with wrong org JWT")
	tp, err := newGRPCTestTracerProvider(t.Context(), address,
		otlptracegrpc.WithHeaders(map[string]string{
			"authorization": "Bearer " + token,
		}),
	)
	require.NoError(t, err)
	defer tp.Shutdown(t.Context()) //nolint:errcheck

	tracer := tp.Tracer("wrong-org-service")
	_, span := tracer.Start(t.Context(), "wrong-org-span")
	span.End()

	t.Log("Then: Request is rejected with authentication error")
	err = tp.ForceFlush(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unauthenticated")
}
