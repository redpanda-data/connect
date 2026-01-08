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

package otlpconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

func createTestLogs() plogotlp.ExportRequest {
	logs := plog.NewLogs()

	// Resource 1
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	resource := rl.Resource()
	resource.Attributes().PutStr("service.name", "log-service")
	resource.Attributes().PutStr("host.name", "localhost")

	// Scope 1
	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	scope := sl.Scope()
	scope.SetName("test-logger")
	scope.SetVersion("v1.0.0")

	// Log record 1 - INFO level with string body
	log1 := sl.LogRecords().AppendEmpty()
	log1.SetTimestamp(pcommon.Timestamp(1609459200000000000))
	log1.SetObservedTimestamp(pcommon.Timestamp(1609459200100000000))
	log1.SetSeverityNumber(plog.SeverityNumberInfo)
	log1.SetSeverityText("INFO")
	log1.Body().SetStr("This is an info log message")
	log1.Attributes().PutStr("log.level", "info")
	log1.Attributes().PutStr("source", "test")

	// Log record 2 - ERROR level with trace context
	log2 := sl.LogRecords().AppendEmpty()
	log2.SetTimestamp(pcommon.Timestamp(1609459201000000000))
	log2.SetObservedTimestamp(pcommon.Timestamp(1609459201100000000))
	log2.SetSeverityNumber(plog.SeverityNumberError)
	log2.SetSeverityText("ERROR")
	log2.Body().SetStr("Error occurred")
	log2.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	log2.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	log2.Attributes().PutInt("error.code", 500)

	// Log record 3 - DEBUG level with map body
	log3 := sl.LogRecords().AppendEmpty()
	log3.SetTimestamp(pcommon.Timestamp(1609459202000000000))
	log3.SetSeverityNumber(plog.SeverityNumberDebug)
	log3.SetSeverityText("DEBUG")
	bodyMap := log3.Body().SetEmptyMap()
	bodyMap.PutStr("message", "Debug information")
	bodyMap.PutInt("counter", 42)
	bodyMap.PutBool("success", true)

	return plogotlp.NewExportRequestFromLogs(logs)
}

func TestLogsRoundtrip(t *testing.T) {
	// Create original request
	original := createTestLogs()

	// Convert to Redpanda
	redpandaLogs := LogsToRedpanda(original)
	require.Len(t, redpandaLogs, 3)

	// Verify first log
	log1 := &redpandaLogs[0]
	assert.Equal(t, pb.SeverityNumber_SEVERITY_NUMBER_INFO, log1.SeverityNumber)
	assert.Equal(t, "INFO", log1.SeverityText)
	assert.NotNil(t, log1.Body)
	assert.NotNil(t, log1.Resource)
	assert.NotNil(t, log1.Scope)

	// Verify second log has trace context
	log2 := &redpandaLogs[1]
	assert.Equal(t, pb.SeverityNumber_SEVERITY_NUMBER_ERROR, log2.SeverityNumber)
	assert.NotEmpty(t, log2.TraceId)
	assert.NotEmpty(t, log2.SpanId)
	assert.Len(t, log2.TraceId, 16)
	assert.Len(t, log2.SpanId, 8)

	// Verify third log has map body
	log3 := &redpandaLogs[2]
	assert.Equal(t, pb.SeverityNumber_SEVERITY_NUMBER_DEBUG, log3.SeverityNumber)
	assert.NotNil(t, log3.Body)

	// Convert back to OTLP
	reconstructed := LogsFromRedpanda(redpandaLogs)

	// Verify structure
	reconstructedLogs := reconstructed.Logs()
	assert.Equal(t, 1, reconstructedLogs.ResourceLogs().Len())

	rl := reconstructedLogs.ResourceLogs().At(0)
	v, ok := rl.Resource().Attributes().Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "log-service", v.Str())
	assert.Equal(t, 1, rl.ScopeLogs().Len())

	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, "test-logger", sl.Scope().Name())
	assert.Equal(t, 3, sl.LogRecords().Len())

	// Verify log details
	recLog1 := sl.LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberInfo, recLog1.SeverityNumber())
	assert.Equal(t, "INFO", recLog1.SeverityText())
	assert.Equal(t, "This is an info log message", recLog1.Body().Str())

	recLog2 := sl.LogRecords().At(1)
	assert.Equal(t, plog.SeverityNumberError, recLog2.SeverityNumber())
	assert.False(t, recLog2.TraceID().IsEmpty())
	assert.False(t, recLog2.SpanID().IsEmpty())

	recLog3 := sl.LogRecords().At(2)
	assert.Equal(t, plog.SeverityNumberDebug, recLog3.SeverityNumber())
	assert.Equal(t, pcommon.ValueTypeMap, recLog3.Body().Type())
	v, ok = recLog3.Body().Map().Get("message")
	assert.True(t, ok)
	assert.Equal(t, "Debug information", v.Str())
}

func TestSeverityNumbers(t *testing.T) {
	tests := []struct {
		name         string
		severity     plog.SeverityNumber
		severityText string
	}{
		{"unspecified", plog.SeverityNumberUnspecified, ""},
		{"trace", plog.SeverityNumberTrace, "TRACE"},
		{"trace2", plog.SeverityNumberTrace2, "TRACE2"},
		{"trace3", plog.SeverityNumberTrace3, "TRACE3"},
		{"trace4", plog.SeverityNumberTrace4, "TRACE4"},
		{"debug", plog.SeverityNumberDebug, "DEBUG"},
		{"debug2", plog.SeverityNumberDebug2, "DEBUG2"},
		{"debug3", plog.SeverityNumberDebug3, "DEBUG3"},
		{"debug4", plog.SeverityNumberDebug4, "DEBUG4"},
		{"info", plog.SeverityNumberInfo, "INFO"},
		{"info2", plog.SeverityNumberInfo2, "INFO2"},
		{"info3", plog.SeverityNumberInfo3, "INFO3"},
		{"info4", plog.SeverityNumberInfo4, "INFO4"},
		{"warn", plog.SeverityNumberWarn, "WARN"},
		{"warn2", plog.SeverityNumberWarn2, "WARN2"},
		{"warn3", plog.SeverityNumberWarn3, "WARN3"},
		{"warn4", plog.SeverityNumberWarn4, "WARN4"},
		{"error", plog.SeverityNumberError, "ERROR"},
		{"error2", plog.SeverityNumberError2, "ERROR2"},
		{"error3", plog.SeverityNumberError3, "ERROR3"},
		{"error4", plog.SeverityNumberError4, "ERROR4"},
		{"fatal", plog.SeverityNumberFatal, "FATAL"},
		{"fatal2", plog.SeverityNumberFatal2, "FATAL2"},
		{"fatal3", plog.SeverityNumberFatal3, "FATAL3"},
		{"fatal4", plog.SeverityNumberFatal4, "FATAL4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create log record
			logs := plog.NewLogs()
			rl := logs.ResourceLogs().AppendEmpty()
			sl := rl.ScopeLogs().AppendEmpty()
			log := sl.LogRecords().AppendEmpty()
			log.SetSeverityNumber(tt.severity)
			log.SetSeverityText(tt.severityText)
			log.Body().SetStr("test message")

			req := plogotlp.NewExportRequestFromLogs(logs)

			// Convert to Redpanda
			redpandaLogs := LogsToRedpanda(req)
			require.Len(t, redpandaLogs, 1)

			pbLog := &redpandaLogs[0]
			assert.Equal(t, int32(tt.severity), int32(pbLog.SeverityNumber))
			assert.Equal(t, tt.severityText, pbLog.SeverityText)

			// Convert back
			reconstructed := LogsFromRedpanda(redpandaLogs)

			recLogs := reconstructed.Logs()
			recLog := recLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			assert.Equal(t, tt.severity, recLog.SeverityNumber())
			assert.Equal(t, tt.severityText, recLog.SeverityText())
		})
	}
}

func TestLogBodyTypes(t *testing.T) {
	tests := []struct {
		name  string
		setup func(pcommon.Value)
	}{
		{
			name: "string body",
			setup: func(v pcommon.Value) {
				v.SetStr("simple log message")
			},
		},
		{
			name: "int body",
			setup: func(v pcommon.Value) {
				v.SetInt(42)
			},
		},
		{
			name: "map body",
			setup: func(v pcommon.Value) {
				m := v.SetEmptyMap()
				m.PutStr("key1", "value1")
				m.PutInt("key2", 123)
			},
		},
		{
			name: "array body",
			setup: func(v pcommon.Value) {
				s := v.SetEmptySlice()
				s.AppendEmpty().SetStr("item1")
				s.AppendEmpty().SetStr("item2")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := plog.NewLogs()
			rl := logs.ResourceLogs().AppendEmpty()
			sl := rl.ScopeLogs().AppendEmpty()
			log := sl.LogRecords().AppendEmpty()
			tt.setup(log.Body())

			req := plogotlp.NewExportRequestFromLogs(logs)

			// Roundtrip
			redpandaLogs := LogsToRedpanda(req)
			require.Len(t, redpandaLogs, 1)

			reconstructed := LogsFromRedpanda(redpandaLogs)

			recLogs := reconstructed.Logs()
			recLog := recLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

			// Verify body matches
			originalLog := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
			assert.Equal(t, originalLog.Body().Type(), recLog.Body().Type())
			assert.Equal(t, originalLog.Body().AsString(), recLog.Body().AsString())
		})
	}
}

func TestLogWithAllFields(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")

	resource := rl.Resource()
	resource.Attributes().PutStr("service.name", "full-test")
	resource.SetDroppedAttributesCount(5)

	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")

	scope := sl.Scope()
	scope.SetName("full-logger")
	scope.SetVersion("v2.0.0")
	scope.Attributes().PutStr("scope.attr", "value")
	scope.SetDroppedAttributesCount(3)

	log := sl.LogRecords().AppendEmpty()
	log.SetTimestamp(pcommon.Timestamp(1000000000))
	log.SetObservedTimestamp(pcommon.Timestamp(1001000000))
	log.SetSeverityNumber(plog.SeverityNumberWarn)
	log.SetSeverityText("WARN")
	log.Body().SetStr("Warning message")
	log.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	log.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	log.SetFlags(0x01)
	log.Attributes().PutStr("attr1", "value1")
	log.Attributes().PutInt("attr2", 42)
	log.SetDroppedAttributesCount(7)

	req := plogotlp.NewExportRequestFromLogs(logs)

	// Convert to Redpanda
	redpandaLogs := LogsToRedpanda(req)
	require.Len(t, redpandaLogs, 1)

	pbLog := &redpandaLogs[0]

	// Verify all fields
	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", pbLog.ResourceSchemaUrl)
	assert.Equal(t, uint32(5), pbLog.Resource.DroppedAttributesCount)
	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", pbLog.ScopeSchemaUrl)
	assert.Equal(t, "full-logger", pbLog.Scope.Name)
	assert.Equal(t, "v2.0.0", pbLog.Scope.Version)
	assert.Equal(t, uint32(3), pbLog.Scope.DroppedAttributesCount)

	assert.Equal(t, uint64(1000000000), pbLog.TimeUnixNano)
	assert.Equal(t, uint64(1001000000), pbLog.ObservedTimeUnixNano)
	assert.Equal(t, pb.SeverityNumber_SEVERITY_NUMBER_WARN, pbLog.SeverityNumber)
	assert.Equal(t, "WARN", pbLog.SeverityText)
	assert.NotEmpty(t, pbLog.TraceId)
	assert.NotEmpty(t, pbLog.SpanId)
	assert.Equal(t, uint32(0x01), pbLog.Flags)
	assert.Equal(t, uint32(7), pbLog.DroppedAttributesCount)

	// Convert back
	reconstructed := LogsFromRedpanda(redpandaLogs)

	recLogs := reconstructed.Logs()
	recLog := recLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	// Verify roundtrip
	assert.Equal(t, plog.SeverityNumberWarn, recLog.SeverityNumber())
	assert.Equal(t, "WARN", recLog.SeverityText())
	assert.Equal(t, "Warning message", recLog.Body().Str())
	assert.False(t, recLog.TraceID().IsEmpty())
	assert.False(t, recLog.SpanID().IsEmpty())
	assert.Equal(t, uint32(7), recLog.DroppedAttributesCount())
}

func TestEmptyLogsRequest(t *testing.T) {
	// Create empty request
	logs := plog.NewLogs()
	req := plogotlp.NewExportRequestFromLogs(logs)

	// Convert to Redpanda
	redpandaLogs := LogsToRedpanda(req)
	assert.Empty(t, redpandaLogs)

	// Convert back
	reconstructed := LogsFromRedpanda(redpandaLogs)
	assert.Equal(t, 0, reconstructed.Logs().ResourceLogs().Len())
}

func TestMultipleResourcesAndScopesLogs(t *testing.T) {
	logs := plog.NewLogs()

	// Resource 1, Scope 1
	rl1 := logs.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "service-1")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("scope-1")
	log1 := sl1.LogRecords().AppendEmpty()
	log1.Body().SetStr("log-1-1")

	// Resource 1, Scope 2
	sl2 := rl1.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("scope-2")
	log2 := sl2.LogRecords().AppendEmpty()
	log2.Body().SetStr("log-1-2")

	// Resource 2, Scope 1
	rl2 := logs.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "service-2")
	sl3 := rl2.ScopeLogs().AppendEmpty()
	sl3.Scope().SetName("scope-1")
	log3 := sl3.LogRecords().AppendEmpty()
	log3.Body().SetStr("log-2-1")

	req := plogotlp.NewExportRequestFromLogs(logs)

	// Convert to Redpanda
	redpandaLogs := LogsToRedpanda(req)
	assert.Len(t, redpandaLogs, 3)

	// Convert back
	reconstructed := LogsFromRedpanda(redpandaLogs)

	// Should have 2 resource logs
	recLogs := reconstructed.Logs()
	assert.Equal(t, 2, recLogs.ResourceLogs().Len())

	// Count total log records
	totalLogs := 0
	for i := 0; i < recLogs.ResourceLogs().Len(); i++ {
		rl := recLogs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			totalLogs += rl.ScopeLogs().At(j).LogRecords().Len()
		}
	}
	assert.Equal(t, 3, totalLogs)
}
