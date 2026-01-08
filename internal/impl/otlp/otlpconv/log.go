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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

// LogsCount counts the total number of log records in the request.
func LogsCount(req plogotlp.ExportRequest) int {
	logs := req.Logs()
	resourceLogs := logs.ResourceLogs()

	n := 0
	for i := range resourceLogs.Len() {
		scopeLogs := resourceLogs.At(i).ScopeLogs()
		for j := range scopeLogs.Len() {
			n += scopeLogs.At(j).LogRecords().Len()
		}
	}
	return n
}

// LogsToRedpandaFunc converts OTLP log export request to individual Redpanda log
// records via callback. Each log record from the batch becomes a self-contained
// message with embedded Resource/Scope. The callback receives a pointer to the
// log record and can process or store it. The callback returns true to continue
// processing or false to stop early.
func LogsToRedpandaFunc(req plogotlp.ExportRequest, cb func(*pb.LogRecord) bool) {
	logs := req.Logs()
	resourceLogs := logs.ResourceLogs()

	for i := range resourceLogs.Len() {
		rl := resourceLogs.At(i)
		resource := rl.Resource()
		resourceSchemaURL := rl.SchemaUrl()

		scopeLogs := rl.ScopeLogs()
		for j := range scopeLogs.Len() {
			sl := scopeLogs.At(j)
			scope := sl.Scope()
			scopeSchemaURL := sl.SchemaUrl()

			logRecords := sl.LogRecords()
			for k := range logRecords.Len() {
				var r pb.LogRecord
				logRecord := logRecords.At(k)
				logRecordToRedpanda(&r, &logRecord,
					resource, resourceSchemaURL, scope, scopeSchemaURL)
				if !cb(&r) {
					return
				}
			}
		}
	}
}

// LogsFromRedpanda converts individual Redpanda log records to OTLP log export
// request. Groups log records by Resource and Scope to create efficient batch
// structure. Since logs are already ordered by resource and scope from
// LogsToRedpanda, we detect changes sequentially.
func LogsFromRedpanda(logs []pb.LogRecord) plogotlp.ExportRequest {
	pLogs := plog.NewLogs()

	if len(logs) == 0 {
		return plogotlp.NewExportRequestFromLogs(pLogs)
	}

	var (
		curResourceLogs plog.ResourceLogs
		curScopeLogs    plog.ScopeLogs

		curResHash   = "-"
		curScopeHash = "-"
	)
	for i := range logs {
		log := &logs[i]
		resHash := ResourceHash(log.Resource)
		scopeHash := ScopeHash(log.Scope)

		// Check if resource changed
		if resHash != curResHash {
			curResourceLogs = pLogs.ResourceLogs().AppendEmpty()
			resourceFromRedpanda(log.Resource, curResourceLogs.Resource())
			curResourceLogs.SetSchemaUrl(log.ResourceSchemaUrl)
			curResHash = resHash
			curScopeHash = "" // Reset scope hash
		}
		if scopeHash != curScopeHash {
			curScopeLogs = curResourceLogs.ScopeLogs().AppendEmpty()
			scopeFromRedpanda(log.Scope, curScopeLogs.Scope())
			curScopeLogs.SetSchemaUrl(log.ScopeSchemaUrl)
			curScopeHash = scopeHash
		}

		// Add log record to current scope
		lr := curScopeLogs.LogRecords().AppendEmpty()
		logRecordFromRedpanda(&lr, log)
	}

	return plogotlp.NewExportRequestFromLogs(pLogs)
}

// logRecordToRedpanda converts a single pdata LogRecord to Redpanda protobuf LogRecord.
// Embeds the Resource and Scope from the parent ResourceLogs/ScopeLogs.
func logRecordToRedpanda(
	dst *pb.LogRecord,
	src *plog.LogRecord,
	resource pcommon.Resource,
	resourceSchemaURL string,
	scope pcommon.InstrumentationScope,
	scopeSchemaURL string,
) {
	dst.Resource = resourceToRedpanda(resource)
	dst.ResourceSchemaUrl = resourceSchemaURL
	dst.Scope = scopeToRedpanda(scope)
	dst.ScopeSchemaUrl = scopeSchemaURL
	dst.TimeUnixNano = int64ToUint64(int64(src.Timestamp()))
	dst.ObservedTimeUnixNano = int64ToUint64(int64(src.ObservedTimestamp()))
	dst.SeverityNumber = severityNumberToRedpanda(src.SeverityNumber())
	dst.SeverityText = src.SeverityText()
	dst.Body = anyValueToRedpanda(src.Body())
	dst.Attributes = attributesToRedpanda(src.Attributes())
	dst.DroppedAttributesCount = src.DroppedAttributesCount()
	dst.Flags = uint32(src.Flags())

	// Add trace context if present
	traceID := src.TraceID()
	if !traceID.IsEmpty() {
		dst.TraceId = traceID[:]
	}

	spanID := src.SpanID()
	if !spanID.IsEmpty() {
		dst.SpanId = spanID[:]
	}
}

// logRecordFromRedpanda converts Redpanda protobuf LogRecord to pdata LogRecord.
func logRecordFromRedpanda(dst *plog.LogRecord, src *pb.LogRecord) {
	dst.SetTimestamp(pcommon.Timestamp(uint64ToInt64(src.TimeUnixNano)))
	dst.SetObservedTimestamp(pcommon.Timestamp(uint64ToInt64(src.ObservedTimeUnixNano)))
	dst.SetSeverityNumber(severityNumberFromRedpanda(src.SeverityNumber))
	dst.SetSeverityText(src.SeverityText)

	anyValueFromRedpanda(src.Body, dst.Body())
	attributesFromRedpanda(src.Attributes, dst.Attributes())
	dst.SetDroppedAttributesCount(src.DroppedAttributesCount)

	// Add trace context if present
	if len(src.TraceId) == 16 {
		var traceID [16]byte
		copy(traceID[:], src.TraceId)
		dst.SetTraceID(traceID)
	}

	if len(src.SpanId) == 8 {
		var spanID [8]byte
		copy(spanID[:], src.SpanId)
		dst.SetSpanID(spanID)
	}

	dst.SetFlags(plog.LogRecordFlags(src.Flags))
}

// severityNumberToRedpanda converts pdata SeverityNumber to Redpanda protobuf SeverityNumber.
func severityNumberToRedpanda(src plog.SeverityNumber) pb.SeverityNumber {
	return pb.SeverityNumber(src)
}

// severityNumberFromRedpanda converts Redpanda protobuf SeverityNumber to pdata SeverityNumber.
func severityNumberFromRedpanda(src pb.SeverityNumber) plog.SeverityNumber {
	return plog.SeverityNumber(src)
}
