// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
)

// writer handles writing batches of messages to a single Iceberg table.
type writer struct {
	table     *table.Table
	committer *committer
	logger    *service.Logger
}

// NewWriter creates a new writer for a specific table.
// The table and committer should use separate table references since they
// operate in different goroutines and the table object is mutable.
func NewWriter(tbl *table.Table, comm *committer, logger *service.Logger) *writer {
	return &writer{
		table:     tbl,
		committer: comm,
		logger:    logger,
	}
}

// Write writes a batch of messages to the table.
func (w *writer) Write(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Convert messages to parquet
	parquetData, rowCount, err := w.messagesToParquet(batch)
	if err != nil {
		return fmt.Errorf("failed to convert messages to parquet: %w", err)
	}

	// Generate unique file name
	fileName := fmt.Sprintf("%s-%d.parquet", uuid.New().String(), time.Now().UnixNano())

	// Get location provider for the table
	locProvider, err := w.table.LocationProvider()
	if err != nil {
		return fmt.Errorf("failed to get location provider: %w", err)
	}

	// Generate data file path
	filePath := locProvider.NewDataLocation(fileName)

	// Write file using table's IO
	tableIO, err := w.table.FS(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table IO: %w", err)
	}
	writeIO, ok := tableIO.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("table IO does not support writing (got %T)", tableIO)
	}

	if err := writeIO.WriteFile(filePath, parquetData); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	w.logger.Debugf("Wrote parquet file: %s (%d bytes, %d rows)", filePath, len(parquetData), rowCount)

	// Submit to committer
	file := dataFile{
		path:     filePath,
		rowCount: int64(rowCount),
		fileSize: int64(len(parquetData)),
	}

	if err := w.committer.Commit(ctx, file); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// messagesToParquet converts messages to parquet format using the shredder.
func (w *writer) messagesToParquet(batch service.MessageBatch) ([]byte, int, error) {
	schema := w.table.Schema()

	// Build parquet schema and field ID to column index mapping
	pqSchema, fieldToCol, err := buildParquetSchema(schema)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build parquet schema: %w", err)
	}

	// Create parquet writer and get column writers
	buf := bytes.NewBuffer(nil)
	pw := parquet.NewWriter(buf, pqSchema)
	colWriters := pw.ColumnWriters()

	// Create shredder for the schema
	rs := shredder.NewRecordShredder(schema)

	// Create sink that writes directly to column writers
	sink := newParquetSink(fieldToCol, colWriters)

	// Shred each record
	rowCount := 0
	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse message as structured: %w", err)
		}

		row, ok := structured.(map[string]any)
		if !ok {
			return nil, 0, fmt.Errorf("message is not an object, got %T", structured)
		}

		// Start new row (flushes previous row to column writers)
		if err := sink.startRow(); err != nil {
			return nil, 0, fmt.Errorf("failed to start row: %w", err)
		}

		if err := rs.Shred(row, sink); err != nil {
			return nil, 0, fmt.Errorf("failed to shred record: %w", err)
		}

		rowCount++
	}

	// Flush the last row
	if err := sink.flush(); err != nil {
		return nil, 0, fmt.Errorf("failed to flush final row: %w", err)
	}

	if err := pw.Close(); err != nil {
		return nil, 0, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	return buf.Bytes(), rowCount, nil
}

// Close closes the writer and its committer.
func (w *writer) Close() {
	w.committer.Close()
}

// parquetSink implements shredder.ShredderSink and writes values directly to column writers.
type parquetSink struct {
	fieldToCol map[int]int             // field ID -> column index
	colWriters []*parquet.ColumnWriter // column writers to write to
	currentRow [][]parquet.Value       // current row being built (multiple values per column for lists)
}

func newParquetSink(fieldToCol map[int]int, colWriters []*parquet.ColumnWriter) *parquetSink {
	return &parquetSink{
		fieldToCol: fieldToCol,
		colWriters: colWriters,
		currentRow: make([][]parquet.Value, len(colWriters)),
	}
}

// startRow flushes the previous row to column writers and resets for a new row.
func (s *parquetSink) startRow() error {
	if err := s.flush(); err != nil {
		return err
	}
	return nil
}

// flush writes the current row to column writers.
func (s *parquetSink) flush() error {
	// Write each column's values to its column writer
	for i, vals := range s.currentRow {
		if len(vals) == 0 {
			continue
		}
		if _, err := s.colWriters[i].WriteRowValues(vals); err != nil {
			return fmt.Errorf("failed to write to column %d: %w", i, err)
		}
		// Reset slice but keep capacity
		s.currentRow[i] = vals[:0]
	}
	return nil
}

func (s *parquetSink) EmitValue(sv shredder.ShreddedValue) error {
	colIdx, ok := s.fieldToCol[sv.FieldID]
	if !ok {
		// Unknown field ID - skip (shouldn't happen with correct schema)
		return nil
	}

	// Append the value with rep/def levels set
	val := sv.Value.Level(sv.RepLevel, sv.DefLevel, colIdx)
	s.currentRow[colIdx] = append(s.currentRow[colIdx], val)

	return nil
}

func (s *parquetSink) OnNewField(_ icebergx.Path, _ string, _ any) {
	// For now, ignore unknown fields
	// TODO: Could be used for schema evolution
}

// buildParquetSchema builds a parquet schema from an iceberg schema and returns
// a mapping from field ID to column index.
func buildParquetSchema(schema *iceberg.Schema) (*parquet.Schema, map[int]int, error) {
	group := make(parquet.Group)

	for _, field := range schema.Fields() {
		node, err := icebergFieldToParquet(field)
		if err != nil {
			return nil, nil, fmt.Errorf("field %s: %w", field.Name, err)
		}
		group[field.Name] = node
	}
	pqSchema := parquet.NewSchema("root", group)

	fieldToCol := make(map[int]int)
	st := schema.AsStruct()
	for leaf := range schemaLeaves(&st, -1, nil) {
		col, ok := pqSchema.Lookup(leaf.Path...)
		if !ok {
			return nil, nil, fmt.Errorf("invalid schema mapping for %s", strings.Join(leaf.Path, "."))
		}
		fieldToCol[leaf.FieldID] = col.ColumnIndex
	}

	return pqSchema, fieldToCol, nil
}

type schemaLeaf struct {
	FieldID int
	Type    iceberg.Type
	Path    []string
}

func schemaLeaves(root iceberg.Type, fieldID int, path []string) iter.Seq[schemaLeaf] {
	walkStruct := func(st *iceberg.StructType, yield func(schemaLeaf) bool) bool {
		for _, field := range st.Fields() {
			for leaf := range schemaLeaves(field.Type, field.ID, append(path, field.Name)) {
				if !yield(leaf) {
					return false
				}
			}
		}
		return true
	}
	walkList := func(lt *iceberg.ListType, yield func(schemaLeaf) bool) bool {
		for leaf := range schemaLeaves(lt.Element, lt.ElementID, append(path, "list", "element")) {
			if !yield(leaf) {
				return false
			}
		}
		return true
	}
	walkMap := func(mt *iceberg.MapType, yield func(schemaLeaf) bool) bool {
		for leaf := range schemaLeaves(mt.KeyType, mt.KeyID, append(path, "key_value", "key")) {
			if !yield(leaf) {
				return false
			}
		}
		for leaf := range schemaLeaves(mt.ValueType, mt.ValueID, append(path, "key_value", "value")) {
			if !yield(leaf) {
				return false
			}
		}
		return true
	}
	return func(yield func(schemaLeaf) bool) {
		switch t := root.(type) {
		case *iceberg.StructType:
			walkStruct(t, yield)
		case *iceberg.ListType:
			walkList(t, yield)
		case *iceberg.MapType:
			walkMap(t, yield)
		default:
			yield(schemaLeaf{
				FieldID: fieldID,
				Type:    t,
				Path:    path,
			})
		}
	}
}

// icebergFieldToParquet converts an iceberg field to a parquet node.
func icebergFieldToParquet(field iceberg.NestedField) (parquet.Node, error) {
	node, err := icebergTypeToParquet(field.Type)
	if err != nil {
		return nil, err
	}

	// Add optional wrapper if not required
	if !field.Required {
		node = parquet.Optional(node)
	}

	// Note: We don't add field IDs because iceberg-go's AddFiles method doesn't support them.
	// The AddFiles method resolves field IDs by matching column names to the schema.

	return node, nil
}

// icebergTypeToParquet converts an iceberg type to a parquet node.
func icebergTypeToParquet(t iceberg.Type) (parquet.Node, error) {
	switch t := t.(type) {
	case iceberg.BooleanType:
		return parquet.Leaf(parquet.BooleanType), nil
	case iceberg.Int32Type:
		return parquet.Int(32), nil
	case iceberg.Int64Type:
		return parquet.Int(64), nil
	case iceberg.Float32Type:
		return parquet.Leaf(parquet.FloatType), nil
	case iceberg.Float64Type:
		return parquet.Leaf(parquet.DoubleType), nil
	case iceberg.StringType:
		return parquet.String(), nil
	case iceberg.BinaryType:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case iceberg.DateType:
		return parquet.Date(), nil
	case iceberg.TimeType:
		return parquet.Time(parquet.Microsecond), nil
	case iceberg.TimestampType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.TimestampTzType:
		return parquet.Timestamp(parquet.Microsecond), nil
	case iceberg.UUIDType:
		return parquet.UUID(), nil
	case *iceberg.StructType:
		group := make(parquet.Group)
		for _, f := range t.Fields() {
			node, err := icebergFieldToParquet(f)
			if err != nil {
				return nil, err
			}
			group[f.Name] = node
		}
		return group, nil
	case *iceberg.ListType:
		elem, err := icebergTypeToParquet(t.Element)
		if err != nil {
			return nil, err
		}
		return parquet.List(elem), nil
	case *iceberg.MapType:
		key, err := icebergTypeToParquet(t.KeyType)
		if err != nil {
			return nil, err
		}
		val, err := icebergTypeToParquet(t.ValueType)
		if err != nil {
			return nil, err
		}
		return parquet.Map(key, val), nil
	default:
		return nil, fmt.Errorf("unsupported iceberg type: %T", t)
	}
}
