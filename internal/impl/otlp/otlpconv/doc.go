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

// Package otlpconv provides bidirectional conversion between OpenTelemetry Collector
// OTLP format and Redpanda OTEL v1 protobuf format.
//
// # Format Differences
//
// OTLP Format (OpenTelemetry Collector):
//   - Batched structure: ResourceSpans → ScopeSpans → []Span
//   - Resource and Scope metadata shared at batch level
//   - Efficient for network transmission (reduced redundancy)
//   - Types: ptraceotlp.ExportRequest, plogotlp.ExportRequest, pmetricotlp.ExportRequest
//   - Package: go.opentelemetry.io/collector/pdata
//
// Redpanda OTEL Format:
//   - Individual records: Each signal is self-contained
//   - Resource and Scope embedded in every message
//   - Optimized for Kafka partitioning (one signal per record)
//   - Types: pb.Span, pb.LogRecord, pb.Metric
//   - Package: buf.build/gen/go/redpandadata/otel/protocolbuffers/go/redpanda/otel/v1
//
// # Conversion Directions
//
// Direction 1: OTLP → Redpanda (Unbatching)
//   - Extracts individual signals from batched OTLP format
//   - Embeds Resource and Scope metadata into each signal
//   - Use cases: OTLP input → Kafka output, pipeline processing
//
// Direction 2: Redpanda → OTLP (Batching)
//   - Groups individual signals by Resource and Scope
//   - Creates efficient batched OTLP structure
//   - Use cases: Kafka input → OTLP output, aggregation
//
// # Data Preservation
//
// All conversions preserve complete telemetry data:
//   - Trace IDs, Span IDs (16-byte and 8-byte arrays)
//   - Timestamps (nanosecond precision)
//   - Attributes (all AnyValue types including nested structures)
//   - Metadata (schema URLs, dropped counts, flags)
//   - Span events, links, status
//   - Metric data points, exemplars, aggregation types
//   - Log severity, body, trace context
package otlpconv
