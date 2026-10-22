// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

const messageFormatBenchColumns = 40

// formatBenchSink does no work and no allocation, so the benchmark isolates
// the per-record cost of getting a message into the shredder.
type formatBenchSink struct{}

func (formatBenchSink) EmitValue(shredder.ShreddedValue) error { return nil }
func (formatBenchSink) OnNewField(icebergx.Path, string, any)  {}

// BenchmarkMessageFormat compares the per-record cost of an array message
// converted by arrayToRow against the object it replaces, and against the
// Bloblang mapping users otherwise need to turn an array into an object. Run:
//
//	GOMAXPROCS=1 go test ./internal/impl/iceberg/ -bench BenchmarkMessageFormat -benchmem -run '^$'
func BenchmarkMessageFormat(b *testing.B) {
	fields := make([]iceberg.NestedField, messageFormatBenchColumns)
	object := make(map[string]any, messageFormatBenchColumns)
	array := make([]any, messageFormatBenchColumns)
	mappingLines := make([]string, messageFormatBenchColumns)
	for i := range fields {
		name := fmt.Sprintf("c%d", i)
		var typ iceberg.Type = iceberg.PrimitiveTypes.String
		if i%2 == 0 {
			typ = iceberg.PrimitiveTypes.Int64
		}
		value := fmt.Sprintf("%d", 1000+i)
		if i%2 == 1 {
			value = "value-" + value
		}
		fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: typ}
		object[name] = value
		array[i] = value
		mappingLines[i] = fmt.Sprintf("root.%s = this.index(%d)", name, i)
	}
	tableSchema := iceberg.NewSchema(1, fields...)
	columns := topLevelColumnNames(tableSchema)
	sink := formatBenchSink{}

	newShredder := func() *shredder.RecordShredder {
		return shredder.NewRecordShredder(tableSchema, true)
	}

	b.Run("object", func(b *testing.B) {
		rs := newShredder()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := rs.Shred(object, sink); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("array", func(b *testing.B) {
		rs := newShredder()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			row, err := arrayToRow(array, columns)
			if err != nil {
				b.Fatal(err)
			}
			if err := rs.Shred(row, sink); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("object_via_bloblang", func(b *testing.B) {
		exec, err := bloblang.Parse(strings.Join(mappingLines, "\n"))
		if err != nil {
			b.Fatal(err)
		}
		rs := newShredder()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg := service.NewMessage(nil)
			msg.SetStructured(array)
			out, err := msg.BloblangQuery(exec)
			if err != nil {
				b.Fatal(err)
			}
			structured, err := out.AsStructured()
			if err != nil {
				b.Fatal(err)
			}
			if err := rs.Shred(structured.(map[string]any), sink); err != nil {
				b.Fatal(err)
			}
		}
	})
}
