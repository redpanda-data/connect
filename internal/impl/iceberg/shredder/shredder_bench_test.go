// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// discardSink is a Sink that does no work and no allocation, so a benchmark
// measures the shredder's own per-record cost rather than the sink's.
type discardSink struct{}

func (discardSink) EmitValue(ShreddedValue) error         { return nil }
func (discardSink) OnNewField(icebergx.Path, string, any) {}

// benchSchema is a representative flat event schema (mixed primitive types),
// mirroring the shape produced by the iceberg benchmark harness.
func benchSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "event_id", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "value_usd", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 5, Name: "value_tier", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 6, Name: "ts", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 7, Name: "ts_ms", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 8, Name: "is_high_value", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	)
}

func benchRecord() map[string]any {
	return map[string]any{
		"event_id":      "1234567890-purchase",
		"category":      "purchase",
		"value":         int64(4200),
		"value_usd":     42.0,
		"value_tier":    "mid",
		"ts":            int64(1_700_000_000_000),
		"ts_ms":         int64(1_700_000_000),
		"is_high_value": false,
	}
}

// BenchmarkShred measures the per-record shred cost. Run with -benchmem; the
// allocs/op figure is the point of interest — caching Schema.Fields() removes
// a per-record clone of the field slice.
func BenchmarkShred(b *testing.B) {
	rs := NewRecordShredder(benchSchema(), true)
	record := benchRecord()
	sink := discardSink{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rs.Shred(record, sink); err != nil {
			b.Fatal(err)
		}
	}
}
