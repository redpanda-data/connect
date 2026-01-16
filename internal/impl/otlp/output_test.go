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
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
	"github.com/redpanda-data/connect/v4/internal/impl/otlp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// createTestSpan news a test span in Redpanda protobuf format.
func createTestSpan() *pb.Span {
	return &pb.Span{
		Name:    "output-test-span",
		TraceId: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanId:  []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		Resource: &pb.Resource{
			Attributes: []*pb.KeyValue{
				{
					Key: "service.name",
					Value: &pb.AnyValue{
						Value: &pb.AnyValue_StringValue{StringValue: "output-test-service"},
					},
				},
			},
		},
		Scope: &pb.InstrumentationScope{
			Name:    "output-test-scope",
			Version: "1.0.0",
		},
		Attributes: []*pb.KeyValue{
			{
				Key: "http.method",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "POST"},
				},
			},
			{
				Key: "http.url",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "/api/users"},
				},
			},
			{
				Key: "http.status_code",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_IntValue{IntValue: 200},
				},
			},
			{
				Key: "user.id",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "12345"},
				},
			},
			{
				Key: "cache.hit",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_BoolValue{BoolValue: true},
				},
			},
		},
		Events: []*pb.Span_Event{
			{
				Name: "User authenticated",
				Attributes: []*pb.KeyValue{
					{
						Key: "auth.method",
						Value: &pb.AnyValue{
							Value: &pb.AnyValue_StringValue{StringValue: "oauth2"},
						},
					},
					{
						Key: "auth.provider",
						Value: &pb.AnyValue{
							Value: &pb.AnyValue_StringValue{StringValue: "google"},
						},
					},
				},
			},
			{
				Name: "Database query executed",
				Attributes: []*pb.KeyValue{
					{
						Key: "db.system",
						Value: &pb.AnyValue{
							Value: &pb.AnyValue_StringValue{StringValue: "postgresql"},
						},
					},
					{
						Key: "db.statement",
						Value: &pb.AnyValue{
							Value: &pb.AnyValue_StringValue{StringValue: "SELECT * FROM users WHERE id = ?"},
						},
					},
					{
						Key: "db.rows_affected",
						Value: &pb.AnyValue{
							Value: &pb.AnyValue_IntValue{IntValue: 1},
						},
					},
				},
			},
		},
	}
}

// createTestLogRecord news a test log record in Redpanda protobuf format.
func createTestLogRecord() *pb.LogRecord {
	return &pb.LogRecord{
		Body: &pb.AnyValue{
			Value: &pb.AnyValue_StringValue{StringValue: "Test log message from output-test-service"},
		},
		SeverityText: "INFO",
		Resource: &pb.Resource{
			Attributes: []*pb.KeyValue{
				{
					Key: "service.name",
					Value: &pb.AnyValue{
						Value: &pb.AnyValue_StringValue{StringValue: "output-test-service"},
					},
				},
			},
		},
		Scope: &pb.InstrumentationScope{
			Name: "output-test-scope",
		},
		Attributes: []*pb.KeyValue{
			{
				Key: "http.method",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "POST"},
				},
			},
			{
				Key: "http.url",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "/api/users"},
				},
			},
			{
				Key: "http.status_code",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_IntValue{IntValue: 200},
				},
			},
			{
				Key: "user.id",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "12345"},
				},
			},
			{
				Key: "request.id",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_StringValue{StringValue: "req-abc-123"},
				},
			},
			{
				Key: "response.time_ms",
				Value: &pb.AnyValue{
					Value: &pb.AnyValue_DoubleValue{DoubleValue: 45.67},
				},
			},
		},
	}
}

// createTestMetric news a test metric in Redpanda protobuf format.
func createTestMetric() *pb.Metric {
	return &pb.Metric{
		Name:        "output-test-metric",
		Description: "Number of requests processed",
		Unit:        "1",
		Resource: &pb.Resource{
			Attributes: []*pb.KeyValue{
				{
					Key: "service.name",
					Value: &pb.AnyValue{
						Value: &pb.AnyValue_StringValue{StringValue: "output-test-service"},
					},
				},
			},
		},
		Scope: &pb.InstrumentationScope{
			Name:    "output-test-scope",
			Version: "1.0.0",
		},
		Data: &pb.Metric_Sum{
			Sum: &pb.Sum{
				DataPoints: []*pb.NumberDataPoint{
					{
						Attributes: []*pb.KeyValue{
							{
								Key: "http.method",
								Value: &pb.AnyValue{
									Value: &pb.AnyValue_StringValue{StringValue: "POST"},
								},
							},
							{
								Key: "http.route",
								Value: &pb.AnyValue{
									Value: &pb.AnyValue_StringValue{StringValue: "/api/users"},
								},
							},
							{
								Key: "http.status_code",
								Value: &pb.AnyValue{
									Value: &pb.AnyValue_IntValue{IntValue: 200},
								},
							},
						},
						Value: &pb.NumberDataPoint_AsInt{AsInt: 42},
					},
				},
				AggregationTemporality: pb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				IsMonotonic:            true,
			},
		},
	}
}

func TestGRPCOutput(t *testing.T) {
	span := createTestSpan()
	logRecord := createTestLogRecord()
	metric := createTestMetric()

	tests := []struct {
		name       string
		signalType otlp.SignalType
		newProto   func() proto.Message
		validateFn func(msgBytes []byte)
	}{
		{
			name:       "traces",
			signalType: otlp.SignalTypeTrace,
			newProto:   func() proto.Message { return span },
			validateFn: func(msgBytes []byte) {
				var got pb.Span
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, span)
			},
		},
		{
			name:       "logs",
			signalType: otlp.SignalTypeLog,
			newProto:   func() proto.Message { return logRecord },
			validateFn: func(msgBytes []byte) {
				var got pb.LogRecord
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, logRecord)
			},
		},
		{
			name:       "metrics",
			signalType: otlp.SignalTypeMetric,
			newProto:   func() proto.Message { return metric },
			validateFn: func(msgBytes []byte) {
				var got pb.Metric
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, metric)
			},
		},
	}

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	endpoint := "127.0.0.1:" + strconv.Itoa(port)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testOutput(t, endpoint, "", tt.signalType, tt.newProto, tt.validateFn,
				otlp.GRPCInputSpec(), otlp.GRPCInputFromParsed,
				otlp.GRPCOutputSpec(), otlp.GRPCOutputFromParsed)
		})
	}
}

func TestHTTPOutput(t *testing.T) {
	span := createTestSpan()
	logRecord := createTestLogRecord()
	metric := createTestMetric()

	tests := []struct {
		name       string
		signalType otlp.SignalType
		newProto   func() proto.Message
		validateFn func(msgBytes []byte)
	}{
		{
			name:       "traces",
			signalType: otlp.SignalTypeTrace,
			newProto:   func() proto.Message { return span },
			validateFn: func(msgBytes []byte) {
				var got pb.Span
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, span)
			},
		},
		{
			name:       "logs",
			signalType: otlp.SignalTypeLog,
			newProto:   func() proto.Message { return logRecord },
			validateFn: func(msgBytes []byte) {
				var got pb.LogRecord
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, logRecord)
			},
		},
		{
			name:       "metrics",
			signalType: otlp.SignalTypeMetric,
			newProto:   func() proto.Message { return metric },
			validateFn: func(msgBytes []byte) {
				var got pb.Metric
				require.NoError(t, proto.Unmarshal(msgBytes, &got))
				assert.EqualExportedValues(t, &got, metric)
			},
		},
	}

	port, err := integration.GetFreePort()
	require.NoError(t, err)
	endpoint := "127.0.0.1:" + strconv.Itoa(port)

	contentTypes := []string{"protobuf", "json"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, contentType := range contentTypes {
				t.Run(contentType, func(t *testing.T) {
					testOutput(t, endpoint, contentType, tt.signalType, tt.newProto, tt.validateFn,
						otlp.HTTPInputSpec(), otlp.HTTPInputFromParsed,
						otlp.HTTPOutputSpec(), otlp.HTTPOutputFromParsed)
				})
			}
		})
	}
}

// testOutput is a unified helper function to test outputs with different signal types.
func testOutput(
	t *testing.T,
	endpoint string,
	contentType string,
	signalType otlp.SignalType,
	newProto func() proto.Message,
	validateFn func(msgBytes []byte),
	inputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	inputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
	outputSpec interface {
		ParseYAML(yaml string, env *service.Environment) (*service.ParsedConfig, error)
	},
	outputCtor func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, error),
) {
	t.Helper()

	// Start input server
	inputConf, err := inputSpec.ParseYAML(fmt.Sprintf(`
address: "%s"
`, endpoint), nil)
	require.NoError(t, err)

	inputRes := service.MockResources()
	license.InjectTestService(inputRes)
	input, err := inputCtor(inputConf, inputRes)
	require.NoError(t, err)

	require.NoError(t, input.Connect(t.Context()))
	t.Cleanup(func() {
		if err := input.Close(context.Background()); err != nil {
			t.Logf("failed to close input: %v", err)
		}
	})

	// Create output
	var outputYAML string
	if contentType != "" {
		// HTTP output with content type
		outputYAML = fmt.Sprintf(`
endpoint: "http://%s"
content_type: "%s"
`, endpoint, contentType)
	} else {
		// gRPC output
		outputYAML = fmt.Sprintf(`
endpoint: "%s"
`, endpoint)
	}

	outputConf, err := outputSpec.ParseYAML(outputYAML, nil)
	require.NoError(t, err)

	outputRes := service.MockResources()
	license.InjectTestService(outputRes)
	output, err := outputCtor(outputConf, outputRes)
	require.NoError(t, err)

	require.NoError(t, output.Connect(t.Context()))
	t.Cleanup(func() {
		if err := output.Close(context.Background()); err != nil {
			t.Logf("failed to close output: %v", err)
		}
	})

	// Start reading in background
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

	// Send message
	protoMsg := newProto()
	msg, err := otlp.NewMessageWithSignalType(protoMsg, signalType)
	require.NoError(t, err)
	batch := service.MessageBatch{msg}
	require.NoError(t, output.WriteBatch(t.Context(), batch))

	// Wait for message
	const timeout = 5 * time.Second
	var receivedBatch service.MessageBatch
	select {
	case receivedBatch = <-received:
		// continue
	case err := <-readErr:
		t.Fatalf("Error reading batch: %v", err)
	case <-time.After(timeout):
		t.Fatal("Timeout waiting for message")
	}

	// Assert batch content
	require.NotEmpty(t, receivedBatch)
	for _, msg := range receivedBatch {
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
