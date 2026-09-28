// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

// Package bench holds micro-benchmarks that mirror the end-to-end profiling
// workload (profile_config.yaml) so per-record costs can be attributed
// without standing up infrastructure. Part of the iceberg sink per-record CPU
// profiling effort.
package bench

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/redpanda-data/benthos/v4/public/schema"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

// discardSink mirrors the shredder package's benchmark sink: no work, no
// allocation, so the benchmark isolates the shredder's own per-record cost.
type discardSink struct{}

func (discardSink) EmitValue(shredder.ShreddedValue) error { return nil }
func (discardSink) OnNewField(icebergx.Path, string, any)  {}

// wideSchema mirrors the 24-column table created by profile_config.yaml
// (~1.2KB high-entropy JSON events).
func wideSchema() *iceberg.Schema {
	names := wideFieldNames()
	fields := make([]iceberg.NestedField, 0, len(names))
	for i, n := range names {
		var typ iceberg.Type
		switch n {
		case "id", "user_id", "value", "latency_ms":
			typ = iceberg.PrimitiveTypes.Int64
		case "amount", "score":
			typ = iceberg.PrimitiveTypes.Float64
		case "is_mobile":
			typ = iceberg.PrimitiveTypes.Bool
		default:
			typ = iceberg.PrimitiveTypes.String
		}
		fields = append(fields, iceberg.NestedField{ID: i + 1, Name: n, Type: typ})
	}
	return iceberg.NewSchema(1, fields...)
}

func wideFieldNames() []string {
	return []string{
		"id", "user_id", "session_id", "trace_id", "span_id", "request_id",
		"device_id", "correlation_id", "event_type", "country", "value",
		"amount", "score", "latency_ms", "is_mobile", "user_agent", "url",
		"referrer", "payload_a", "payload_b", "payload_c", "payload_d",
		"description", "ts",
	}
}

func wideRecord() map[string]any {
	return map[string]any{
		"id":             int64(123456),
		"user_id":        int64(4212),
		"session_id":     "0d9c9c3e-9df6-4c4f-8a91-2e6f1a9f7f10",
		"trace_id":       "5c1a67aa-30e7-4a83-9b0e-fd3f9a3f6c1b",
		"span_id":        "e3b7c5d2-8f14-4a6e-b291-7c8e9d0f1a2b",
		"request_id":     "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
		"device_id":      "1f2e3d4c-5b6a-4978-8695-a4b3c2d1e0f9",
		"correlation_id": "abcdef01-2345-4678-9abc-def012345678",
		"event_type":     "purchase",
		"country":        "GB",
		"value":          int64(778123),
		"amount":         42421.42,
		"score":          73.113,
		"latency_ms":     int64(2231),
		"is_mobile":      false,
		"user_agent":     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
		"url":            "https://shop.example.com/products/0d9c9c3e-9df6-4c4f-8a91-2e6f1a9f7f10?ref=5c1a67aa-30e7-4a83-9b0e-fd3f9a3f6c1b",
		"referrer":       "https://www.google.com/search?q=e3b7c5d2-8f14-4a6e-b291-7c8e9d0f1a2b",
		"payload_a":      "9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d:1f2e3d4c-5b6a-4978-8695-a4b3c2d1e0f9",
		"payload_b":      "0d9c9c3e-9df6-4c4f-8a91-2e6f1a9f7f10:5c1a67aa-30e7-4a83-9b0e-fd3f9a3f6c1b",
		"payload_c":      "e3b7c5d2-8f14-4a6e-b291-7c8e9d0f1a2b:9a8b7c6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d",
		"payload_d":      "abcdef01-2345-4678-9abc-def012345678:1f2e3d4c-5b6a-4978-8695-a4b3c2d1e0f9",
		"description":    "synthetic high entropy event record number 123456 for per-record cpu profiling",
		"ts":             "2026-08-03T16:10:00.000000000+01:00",
	}
}

// wideFieldCommons builds the per-field schema metadata that
// writer.messagesToParquet installs on the shredder when the output's
// schema_evolution.schema_metadata is configured, declaring the same types
// inference would produce.
func wideFieldCommons(s *iceberg.Schema) map[int]*schema.Common {
	byID := make(map[int]*schema.Common)
	for _, f := range s.Fields() {
		var t schema.CommonType
		switch f.Type {
		case iceberg.PrimitiveTypes.Int64:
			t = schema.Int64
		case iceberg.PrimitiveTypes.Float64:
			t = schema.Float64
		case iceberg.PrimitiveTypes.Bool:
			t = schema.Boolean
		default:
			t = schema.String
		}
		byID[f.ID] = &schema.Common{Name: f.Name, Type: t, Optional: true}
	}
	return byID
}

// BenchmarkShredWide measures per-record shred cost for the 24-column
// profiling payload, with and without declared field schema metadata
// (the shredder-side effect of the output's declared-schema path). Run:
//
//	GOMAXPROCS=1 go test -bench BenchmarkShredWide -benchmem -run '^$' ./internal/impl/iceberg/bench/
func BenchmarkShredWide(b *testing.B) {
	for _, declared := range []bool{false, true} {
		b.Run(fmt.Sprintf("declared_schema=%v", declared), func(b *testing.B) {
			rs := shredder.NewRecordShredder(wideSchema(), true)
			if declared {
				rs.SetFieldSchemaMetadata(wideFieldCommons(wideSchema()))
			}
			record := wideRecord()
			sink := discardSink{}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rs.Shred(record, sink); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
