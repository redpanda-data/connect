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
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

// SpansCount counts the total number of spans in the request.
func SpansCount(req ptraceotlp.ExportRequest) int {
	traces := req.Traces()
	resourceSpans := traces.ResourceSpans()

	n := 0
	for i := range resourceSpans.Len() {
		scopeSpans := resourceSpans.At(i).ScopeSpans()
		for j := range scopeSpans.Len() {
			n += scopeSpans.At(j).Spans().Len()
		}
	}
	return n
}

// TracesToRedpandaFunc converts OTLP trace export request to individual Redpanda
// span records via callback. Each span from the batch becomes a self-contained
// message with embedded Resource/Scope. The callback receives a pointer to the
// span and can process or store it. The callback returns true to continue
// processing or false to stop early.
func TracesToRedpandaFunc(req ptraceotlp.ExportRequest, cb func(*pb.Span) bool) {
	traces := req.Traces()
	resourceSpans := traces.ResourceSpans()

	for i := range resourceSpans.Len() {
		rs := resourceSpans.At(i)
		resource := rs.Resource()
		resourceSchemaURL := rs.SchemaUrl()

		scopeSpans := rs.ScopeSpans()
		for j := range scopeSpans.Len() {
			ss := scopeSpans.At(j)
			scope := ss.Scope()
			scopeSchemaURL := ss.SchemaUrl()

			spans := ss.Spans()
			for k := range spans.Len() {
				var s pb.Span
				span := spans.At(k)
				spanToRedpanda(&s, &span,
					resource, resourceSchemaURL, scope, scopeSchemaURL)
				if !cb(&s) {
					return
				}
			}
		}
	}
}

// TracesFromRedpanda converts individual Redpanda span records to OTLP trace
// export request. Groups spans by Resource and Scope to create efficient batch
// structure. Since spans are already ordered by resource and scope from
// TracesToRedpanda, we detect changes sequentially.
func TracesFromRedpanda(spans []pb.Span) ptraceotlp.ExportRequest {
	traces := ptrace.NewTraces()

	if len(spans) == 0 {
		return ptraceotlp.NewExportRequestFromTraces(traces)
	}

	var (
		curResourceSpans ptrace.ResourceSpans
		curScopeSpans    ptrace.ScopeSpans

		curResHash   = "-"
		curScopeHash = "-"
	)
	for i := range spans {
		span := &spans[i]
		resHash := ResourceHash(span.Resource)
		scopeHash := ScopeHash(span.Scope)

		// Check if resource changed
		if resHash != curResHash {
			curResourceSpans = traces.ResourceSpans().AppendEmpty()
			resourceFromRedpanda(span.Resource, curResourceSpans.Resource())
			curResourceSpans.SetSchemaUrl(span.ResourceSchemaUrl)
			curResHash = resHash
			curScopeHash = "" // Reset scope hash
		}
		if scopeHash != curScopeHash {
			curScopeSpans = curResourceSpans.ScopeSpans().AppendEmpty()
			scopeFromRedpanda(span.Scope, curScopeSpans.Scope())
			curScopeSpans.SetSchemaUrl(span.ScopeSchemaUrl)
			curScopeHash = scopeHash
		}

		// Add span to current scope
		s := curScopeSpans.Spans().AppendEmpty()
		spanFromRedpanda(&s, span)
	}

	return ptraceotlp.NewExportRequestFromTraces(traces)
}

// spanToRedpanda converts a single pdata Span to Redpanda protobuf Span.
// Embeds the Resource and Scope from the parent ResourceSpans/ScopeSpans.
func spanToRedpanda(
	dst *pb.Span,
	src *ptrace.Span,
	resource pcommon.Resource,
	resourceSchemaURL string,
	scope pcommon.InstrumentationScope,
	scopeSchemaURL string,
) {
	traceID := src.TraceID()
	spanID := src.SpanID()

	dst.Resource = resourceToRedpanda(resource)
	dst.ResourceSchemaUrl = resourceSchemaURL
	dst.Scope = scopeToRedpanda(scope)
	dst.ScopeSchemaUrl = scopeSchemaURL
	dst.TraceId = traceID[:]
	dst.SpanId = spanID[:]
	dst.TraceState = src.TraceState().AsRaw()
	dst.Name = src.Name()
	dst.Kind = spanKindToRedpanda(src.Kind())
	dst.StartTimeUnixNano = int64ToUint64(int64(src.StartTimestamp()))
	dst.EndTimeUnixNano = int64ToUint64(int64(src.EndTimestamp()))
	dst.Attributes = attributesToRedpanda(src.Attributes())
	dst.DroppedAttributesCount = src.DroppedAttributesCount()
	dst.Events = spanEventsToRedpanda(src.Events())
	dst.DroppedEventsCount = src.DroppedEventsCount()
	dst.Links = spanLinksToRedpanda(src.Links())
	dst.DroppedLinksCount = src.DroppedLinksCount()
	dst.Status = spanStatusToRedpanda(src.Status())
	dst.Flags = src.Flags()

	// Add parent span ID if present
	parentSpanID := src.ParentSpanID()
	if !parentSpanID.IsEmpty() {
		dst.ParentSpanId = parentSpanID[:]
	}
}

// spanFromRedpanda converts Redpanda protobuf Span to pdata Span.
func spanFromRedpanda(dst *ptrace.Span, src *pb.Span) {
	var traceID [16]byte
	copy(traceID[:], src.TraceId)
	dst.SetTraceID(traceID)

	var spanID [8]byte
	copy(spanID[:], src.SpanId)
	dst.SetSpanID(spanID)

	if len(src.ParentSpanId) == 8 {
		var parentSpanID [8]byte
		copy(parentSpanID[:], src.ParentSpanId)
		dst.SetParentSpanID(parentSpanID)
	}

	dst.TraceState().FromRaw(src.TraceState)
	dst.SetName(src.Name)
	dst.SetKind(spanKindFromRedpanda(src.Kind))
	dst.SetStartTimestamp(pcommon.Timestamp(uint64ToInt64(src.StartTimeUnixNano)))
	dst.SetEndTimestamp(pcommon.Timestamp(uint64ToInt64(src.EndTimeUnixNano)))

	attributesFromRedpanda(src.Attributes, dst.Attributes())
	dst.SetDroppedAttributesCount(src.DroppedAttributesCount)

	spanEventsFromRedpanda(src.Events, dst.Events())
	dst.SetDroppedEventsCount(src.DroppedEventsCount)

	spanLinksFromRedpanda(src.Links, dst.Links())
	dst.SetDroppedLinksCount(src.DroppedLinksCount)

	spanStatusFromRedpanda(src.Status, dst.Status())
	dst.SetFlags(src.Flags)
}

// spanKindToRedpanda converts pdata SpanKind to Redpanda protobuf SpanKind.
func spanKindToRedpanda(src ptrace.SpanKind) pb.Span_SpanKind {
	switch src {
	case ptrace.SpanKindInternal:
		return pb.Span_SPAN_KIND_INTERNAL
	case ptrace.SpanKindServer:
		return pb.Span_SPAN_KIND_SERVER
	case ptrace.SpanKindClient:
		return pb.Span_SPAN_KIND_CLIENT
	case ptrace.SpanKindProducer:
		return pb.Span_SPAN_KIND_PRODUCER
	case ptrace.SpanKindConsumer:
		return pb.Span_SPAN_KIND_CONSUMER
	default:
		return pb.Span_SPAN_KIND_UNSPECIFIED
	}
}

// spanKindFromRedpanda converts Redpanda protobuf SpanKind to pdata SpanKind.
func spanKindFromRedpanda(src pb.Span_SpanKind) ptrace.SpanKind {
	switch src {
	case pb.Span_SPAN_KIND_INTERNAL:
		return ptrace.SpanKindInternal
	case pb.Span_SPAN_KIND_SERVER:
		return ptrace.SpanKindServer
	case pb.Span_SPAN_KIND_CLIENT:
		return ptrace.SpanKindClient
	case pb.Span_SPAN_KIND_PRODUCER:
		return ptrace.SpanKindProducer
	case pb.Span_SPAN_KIND_CONSUMER:
		return ptrace.SpanKindConsumer
	default:
		return ptrace.SpanKindUnspecified
	}
}

// spanStatusToRedpanda converts pdata Status to Redpanda protobuf Status.
func spanStatusToRedpanda(src ptrace.Status) *pb.Status {
	pbStatus := &pb.Status{
		Message: src.Message(),
	}

	switch src.Code() {
	case ptrace.StatusCodeOk:
		pbStatus.Code = pb.Status_STATUS_CODE_OK
	case ptrace.StatusCodeError:
		pbStatus.Code = pb.Status_STATUS_CODE_ERROR
	default:
		pbStatus.Code = pb.Status_STATUS_CODE_UNSET
	}

	return pbStatus
}

// spanStatusFromRedpanda converts Redpanda protobuf Status to pdata Status.
func spanStatusFromRedpanda(src *pb.Status, dest ptrace.Status) {
	if src == nil {
		return
	}

	dest.SetMessage(src.Message)

	switch src.Code {
	case pb.Status_STATUS_CODE_OK:
		dest.SetCode(ptrace.StatusCodeOk)
	case pb.Status_STATUS_CODE_ERROR:
		dest.SetCode(ptrace.StatusCodeError)
	default:
		dest.SetCode(ptrace.StatusCodeUnset)
	}
}

// spanEventsToRedpanda converts pdata SpanEventSlice to Redpanda protobuf Event slice.
func spanEventsToRedpanda(src ptrace.SpanEventSlice) []*pb.Span_Event {
	if src.Len() == 0 {
		return nil
	}

	events := make([]*pb.Span_Event, 0, src.Len())
	for i := range src.Len() {
		event := src.At(i)
		events = append(events, &pb.Span_Event{
			TimeUnixNano:           int64ToUint64(int64(event.Timestamp())),
			Name:                   event.Name(),
			Attributes:             attributesToRedpanda(event.Attributes()),
			DroppedAttributesCount: event.DroppedAttributesCount(),
		})
	}
	return events
}

// spanEventsFromRedpanda converts Redpanda protobuf Event slice to pdata SpanEventSlice.
func spanEventsFromRedpanda(src []*pb.Span_Event, dest ptrace.SpanEventSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbEvent := range src {
		event := dest.AppendEmpty()
		event.SetTimestamp(pcommon.Timestamp(uint64ToInt64(pbEvent.TimeUnixNano)))
		event.SetName(pbEvent.Name)
		attributesFromRedpanda(pbEvent.Attributes, event.Attributes())
		event.SetDroppedAttributesCount(pbEvent.DroppedAttributesCount)
	}
}

// spanLinksToRedpanda converts pdata SpanLinkSlice to Redpanda protobuf Link slice.
func spanLinksToRedpanda(src ptrace.SpanLinkSlice) []*pb.Span_Link {
	if src.Len() == 0 {
		return nil
	}

	links := make([]*pb.Span_Link, 0, src.Len())
	for i := range src.Len() {
		link := src.At(i)
		traceID := link.TraceID()
		spanID := link.SpanID()

		links = append(links, &pb.Span_Link{
			TraceId:                traceID[:],
			SpanId:                 spanID[:],
			TraceState:             link.TraceState().AsRaw(),
			Attributes:             attributesToRedpanda(link.Attributes()),
			DroppedAttributesCount: link.DroppedAttributesCount(),
			Flags:                  link.Flags(),
		})
	}
	return links
}

// spanLinksFromRedpanda converts Redpanda protobuf Link slice to pdata SpanLinkSlice.
func spanLinksFromRedpanda(src []*pb.Span_Link, dest ptrace.SpanLinkSlice) {
	if len(src) == 0 {
		return
	}

	dest.EnsureCapacity(len(src))
	for _, pbLink := range src {
		link := dest.AppendEmpty()

		if len(pbLink.TraceId) == 16 {
			var traceID [16]byte
			copy(traceID[:], pbLink.TraceId)
			link.SetTraceID(traceID)
		}

		if len(pbLink.SpanId) == 8 {
			var spanID [8]byte
			copy(spanID[:], pbLink.SpanId)
			link.SetSpanID(spanID)
		}

		link.TraceState().FromRaw(pbLink.TraceState)
		attributesFromRedpanda(pbLink.Attributes, link.Attributes())
		link.SetDroppedAttributesCount(pbLink.DroppedAttributesCount)
		link.SetFlags(pbLink.Flags)
	}
}
