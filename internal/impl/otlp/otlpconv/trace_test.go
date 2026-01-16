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

package otlpconv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func createTestTraces() ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()

	// Resource 1
	rs := traces.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	resource := rs.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr("service.namespace", "test-namespace")
	resource.Attributes().PutStr("service.instance.id", "instance-123")

	// Scope 1
	ss := rs.ScopeSpans().AppendEmpty()
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	scope := ss.Scope()
	scope.SetName("test-instrumentation")
	scope.SetVersion("v1.0.0")

	// Span 1
	span1 := ss.Spans().AppendEmpty()
	span1.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	span1.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	span1.SetName("test-span-1")
	span1.SetKind(ptrace.SpanKindServer)
	span1.SetStartTimestamp(pcommon.Timestamp(1609459200000000000))
	span1.SetEndTimestamp(pcommon.Timestamp(1609459201000000000))
	span1.Attributes().PutStr("http.method", "GET")
	span1.Attributes().PutInt("http.status_code", 200)
	span1.Status().SetCode(ptrace.StatusCodeOk)
	span1.Status().SetMessage("OK")

	// Add event
	event := span1.Events().AppendEmpty()
	event.SetTimestamp(pcommon.Timestamp(1609459200500000000))
	event.SetName("test-event")
	event.Attributes().PutStr("event.key", "event.value")

	// Span 2 with link
	span2 := ss.Spans().AppendEmpty()
	span2.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	span2.SetSpanID([8]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28})
	span2.SetParentSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	span2.SetName("test-span-2")
	span2.SetKind(ptrace.SpanKindClient)

	// Add link
	link := span2.Links().AppendEmpty()
	link.SetTraceID([16]byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0})
	link.SetSpanID([8]byte{0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8})
	link.Attributes().PutStr("link.key", "link.value")

	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func TestTracesRoundtrip(t *testing.T) {
	// Create original request
	original := createTestTraces()

	// Convert to Redpanda
	redpandaSpans := TracesToRedpanda(original)
	require.Len(t, redpandaSpans, 2)

	// Verify first span
	span1 := &redpandaSpans[0]
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}, span1.TraceId)
	assert.Equal(t, []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}, span1.SpanId)
	assert.Equal(t, "test-span-1", span1.Name)
	assert.NotNil(t, span1.Resource)
	assert.NotNil(t, span1.Scope)
	assert.Len(t, span1.Events, 1)

	// Verify second span
	span2 := &redpandaSpans[1]
	assert.Equal(t, []byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}, span2.SpanId)
	assert.Equal(t, []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}, span2.ParentSpanId)
	assert.Equal(t, "test-span-2", span2.Name)
	assert.Len(t, span2.Links, 1)

	// Convert back to OTLP
	reconstructed := TracesFromRedpanda(redpandaSpans)

	// Verify structure
	reconstructedTraces := reconstructed.Traces()
	assert.Equal(t, 1, reconstructedTraces.ResourceSpans().Len())

	rs := reconstructedTraces.ResourceSpans().At(0)
	v, ok := rs.Resource().Attributes().Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "test-service", v.Str())
	assert.Equal(t, 1, rs.ScopeSpans().Len())

	ss := rs.ScopeSpans().At(0)
	assert.Equal(t, "test-instrumentation", ss.Scope().Name())
	assert.Equal(t, 2, ss.Spans().Len())

	// Verify span details
	recSpan1 := ss.Spans().At(0)
	assert.Equal(t, "test-span-1", recSpan1.Name())
	assert.Equal(t, ptrace.SpanKindServer, recSpan1.Kind())
	assert.Equal(t, 1, recSpan1.Events().Len())
	assert.Equal(t, ptrace.StatusCodeOk, recSpan1.Status().Code())

	recSpan2 := ss.Spans().At(1)
	assert.Equal(t, "test-span-2", recSpan2.Name())
	assert.Equal(t, 1, recSpan2.Links().Len())
}

func TestSpanKindConversion(t *testing.T) {
	tests := []struct {
		name         string
		pdataKind    ptrace.SpanKind
		redpandaKind interface{}
	}{
		{"unspecified", ptrace.SpanKindUnspecified, 0},
		{"internal", ptrace.SpanKindInternal, 1},
		{"server", ptrace.SpanKindServer, 2},
		{"client", ptrace.SpanKindClient, 3},
		{"producer", ptrace.SpanKindProducer, 4},
		{"consumer", ptrace.SpanKindConsumer, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// pdata -> Redpanda
			redpanda := spanKindToRedpanda(tt.pdataKind)
			assert.Equal(t, tt.redpandaKind, int(redpanda))

			// Redpanda -> pdata
			pdata := spanKindFromRedpanda(redpanda)
			assert.Equal(t, tt.pdataKind, pdata)
		})
	}
}

func TestSpanStatusConversion(t *testing.T) {
	tests := []struct {
		name    string
		code    ptrace.StatusCode
		message string
	}{
		{"unset", ptrace.StatusCodeUnset, ""},
		{"ok", ptrace.StatusCodeOk, "Success"},
		{"error", ptrace.StatusCodeError, "Internal error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create pdata status
			original := ptrace.NewStatus()
			original.SetCode(tt.code)
			original.SetMessage(tt.message)

			// Convert to Redpanda
			redpanda := spanStatusToRedpanda(original)
			if tt.message == "" {
				assert.Nil(t, redpanda)
			} else {
				require.NotNil(t, redpanda)
				assert.Equal(t, tt.message, redpanda.Message)
			}

			// Convert back
			reconstructed := ptrace.NewStatus()
			spanStatusFromRedpanda(redpanda, reconstructed)

			assert.Equal(t, tt.code, reconstructed.Code())
			assert.Equal(t, tt.message, reconstructed.Message())
		})
	}
}

func TestEmptyTracesRequest(t *testing.T) {
	// Create empty request
	traces := ptrace.NewTraces()
	req := ptraceotlp.NewExportRequestFromTraces(traces)

	// Convert to Redpanda
	spans := TracesToRedpanda(req)
	assert.Empty(t, spans)

	// Convert back
	reconstructed := TracesFromRedpanda(spans)
	assert.Equal(t, 0, reconstructed.Traces().ResourceSpans().Len())
}

func TestSpanWithAllFields(t *testing.T) {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")

	resource := rs.Resource()
	resource.Attributes().PutStr("service.name", "full-test")
	resource.SetDroppedAttributesCount(5)

	ss := rs.ScopeSpans().AppendEmpty()
	ss.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")

	scope := ss.Scope()
	scope.SetName("full-scope")
	scope.SetVersion("v2.0.0")
	scope.Attributes().PutStr("scope.attr", "value")
	scope.SetDroppedAttributesCount(3)

	span := ss.Spans().AppendEmpty()
	span.SetTraceID([16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	span.SetSpanID([8]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
	span.SetParentSpanID([8]byte{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28})
	span.SetName("full-span")
	span.SetKind(ptrace.SpanKindProducer)
	span.TraceState().FromRaw("key1=value1,key2=value2")
	span.SetFlags(0x01)

	span.SetStartTimestamp(pcommon.Timestamp(1000000000))
	span.SetEndTimestamp(pcommon.Timestamp(2000000000))

	span.Attributes().PutStr("attr1", "value1")
	span.Attributes().PutInt("attr2", 42)
	span.SetDroppedAttributesCount(2)

	// Add multiple events
	for i := 0; i < 3; i++ {
		event := span.Events().AppendEmpty()
		event.SetName("event")
		event.SetTimestamp(pcommon.Timestamp(1500000000 + int64(i)*1000))
		event.Attributes().PutInt("event.num", int64(i))
		event.SetDroppedAttributesCount(1)
	}
	span.SetDroppedEventsCount(7)

	// Add multiple links
	for i := 0; i < 2; i++ {
		link := span.Links().AppendEmpty()
		link.SetTraceID([16]byte{byte(i), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
		link.SetSpanID([8]byte{byte(i), 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18})
		link.Attributes().PutInt("link.num", int64(i))
		link.SetDroppedAttributesCount(1)
		link.SetFlags(0x02)
	}
	span.SetDroppedLinksCount(4)

	span.Status().SetCode(ptrace.StatusCodeError)
	span.Status().SetMessage("Something went wrong")

	req := ptraceotlp.NewExportRequestFromTraces(traces)

	// Convert to Redpanda
	redpandaSpans := TracesToRedpanda(req)
	require.Len(t, redpandaSpans, 1)

	pbSpan := &redpandaSpans[0]

	// Verify all fields
	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", pbSpan.ResourceSchemaUrl)
	assert.Equal(t, uint32(5), pbSpan.Resource.DroppedAttributesCount)
	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", pbSpan.ScopeSchemaUrl)
	assert.Equal(t, "full-scope", pbSpan.Scope.Name)
	assert.Equal(t, "v2.0.0", pbSpan.Scope.Version)
	assert.Equal(t, uint32(3), pbSpan.Scope.DroppedAttributesCount)

	assert.Equal(t, "full-span", pbSpan.Name)
	assert.NotEmpty(t, pbSpan.ParentSpanId)
	assert.Equal(t, "key1=value1,key2=value2", pbSpan.TraceState)
	assert.Equal(t, uint32(0x01), pbSpan.Flags)
	assert.Equal(t, uint32(2), pbSpan.DroppedAttributesCount)
	assert.Len(t, pbSpan.Events, 3)
	assert.Equal(t, uint32(7), pbSpan.DroppedEventsCount)
	assert.Len(t, pbSpan.Links, 2)
	assert.Equal(t, uint32(4), pbSpan.DroppedLinksCount)

	// Convert back
	reconstructed := TracesFromRedpanda(redpandaSpans)

	recTraces := reconstructed.Traces()
	recSpan := recTraces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	// Verify roundtrip
	assert.Equal(t, "full-span", recSpan.Name())
	assert.Equal(t, ptrace.SpanKindProducer, recSpan.Kind())
	assert.Equal(t, uint32(2), recSpan.DroppedAttributesCount())
	assert.Equal(t, 3, recSpan.Events().Len())
	assert.Equal(t, uint32(7), recSpan.DroppedEventsCount())
	assert.Equal(t, 2, recSpan.Links().Len())
	assert.Equal(t, uint32(4), recSpan.DroppedLinksCount())
	assert.Equal(t, ptrace.StatusCodeError, recSpan.Status().Code())
	assert.Equal(t, "Something went wrong", recSpan.Status().Message())
}

func TestMultipleResourcesAndScopes(t *testing.T) {
	traces := ptrace.NewTraces()

	// Resource 1, Scope 1
	rs1 := traces.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "service-1")
	ss1 := rs1.ScopeSpans().AppendEmpty()
	ss1.Scope().SetName("scope-1")
	span1 := ss1.Spans().AppendEmpty()
	span1.SetTraceID([16]byte{0x01})
	span1.SetSpanID([8]byte{0x01})
	span1.SetName("span-1-1")

	// Resource 1, Scope 2
	ss2 := rs1.ScopeSpans().AppendEmpty()
	ss2.Scope().SetName("scope-2")
	span2 := ss2.Spans().AppendEmpty()
	span2.SetTraceID([16]byte{0x02})
	span2.SetSpanID([8]byte{0x02})
	span2.SetName("span-1-2")

	// Resource 2, Scope 1
	rs2 := traces.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "service-2")
	ss3 := rs2.ScopeSpans().AppendEmpty()
	ss3.Scope().SetName("scope-1")
	span3 := ss3.Spans().AppendEmpty()
	span3.SetTraceID([16]byte{0x03})
	span3.SetSpanID([8]byte{0x03})
	span3.SetName("span-2-1")

	req := ptraceotlp.NewExportRequestFromTraces(traces)

	// Convert to Redpanda
	redpandaSpans := TracesToRedpanda(req)
	assert.Len(t, redpandaSpans, 3)

	// Convert back
	reconstructed := TracesFromRedpanda(redpandaSpans)

	// Should have 2 resource spans
	recTraces := reconstructed.Traces()
	assert.Equal(t, 2, recTraces.ResourceSpans().Len())

	// Count total spans
	totalSpans := 0
	for i := 0; i < recTraces.ResourceSpans().Len(); i++ {
		rs := recTraces.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			totalSpans += rs.ScopeSpans().At(j).Spans().Len()
		}
	}
	assert.Equal(t, 3, totalSpans)
}
