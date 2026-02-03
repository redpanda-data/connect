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
	"slices"

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

	// Convert messages to parquet (grouped by partition)
	parquetFiles, err := w.messagesToParquet(batch)
	if err != nil {
		return fmt.Errorf("failed to convert messages to parquet: %w", err)
	}

	// Get location provider for the table
	locProvider, err := w.table.LocationProvider()
	if err != nil {
		return fmt.Errorf("failed to get location provider: %w", err)
	}

	// Write file using table's IO
	tableIO, err := w.table.FS(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table IO: %w", err)
	}
	writeIO, ok := tableIO.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("table IO does not support writing (got %T)", tableIO)
	}

	// Write each partition file and submit to committer
	var files []dataFile
	for _, pf := range parquetFiles {
		// Generate unique file name
		fileName := uuid.New().String() + ".parquet"

		// Generate data file path (partition path is empty for unpartitioned tables)
		var filePath string
		if pf.path == "" {
			filePath = locProvider.NewDataLocation(fileName)
		} else {
			filePath = locProvider.NewDataLocation(pf.path + "/" + fileName)
		}

		if err := writeIO.WriteFile(filePath, pf.result.data); err != nil {
			return fmt.Errorf("failed to write parquet file %q: %w", filePath, err)
		}

		w.logger.Debugf("Wrote parquet file: %s (%d bytes, %d rows)", filePath, len(pf.result.data), pf.result.rowCount)

		files = append(files, dataFile{
			path:     filePath,
			rowCount: int64(pf.result.rowCount),
			fileSize: int64(len(pf.result.data)),
		})
	}

	// Submit all files to committer
	for _, file := range files {
		if err := w.committer.Commit(ctx, file); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
	}

	return nil
}

// parquetResult holds the output of parquet conversion for a partition.
type parquetResult struct {
	data     []byte
	rowCount int
}

// partitionFile pairs a partition path with its parquet data.
type partitionFile struct {
	path   string // partition path (may be truncated), empty for unpartitioned
	result parquetResult
}

// messagesToParquet converts messages to parquet format using the shredder.
// Returns a slice of partition files. For unpartitioned tables, returns a single
// file with an empty path.
func (w *writer) messagesToParquet(batch service.MessageBatch) ([]partitionFile, error) {
	schema := w.table.Schema()
	spec := w.table.Spec()

	// Build parquet schema and field ID to column index mapping
	pqSchema, fieldToCol, err := icebergx.BuildParquetSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build parquet schema: %w", err)
	}

	// Build sourceID -> partition index map
	partitionSourceIDs := make(map[int]int)
	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		partitionSourceIDs[field.SourceID] = i
	}
	numPartitionFields := spec.NumFields()

	// Create shredder for the schema
	rs := shredder.NewRecordShredder(schema)

	// For unpartitioned tables, use a single writer
	if spec.IsUnpartitioned() {
		sink := newParquetSink(pqSchema, fieldToCol)

		for _, msg := range batch {
			structured, err := msg.AsStructured()
			if err != nil {
				return nil, fmt.Errorf("failed to parse message as structured: %w", err)
			}

			row, ok := structured.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("message is not an object, got %T", structured)
			}

			if err := rs.Shred(row, sink); err != nil {
				return nil, fmt.Errorf("failed to shred record: %w", err)
			}

			if err := sink.flush(); err != nil {
				return nil, fmt.Errorf("failed to flush row: %w", err)
			}
		}

		result, err := sink.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close parquet writer: %w", err)
		}

		return []partitionFile{{path: "", result: result}}, nil
	}

	// For partitioned tables, route rows to different writers
	// Use sorted slice with binary search (keyed by full partition key, not truncated path)
	type partitionEntry struct {
		key  icebergx.PartitionKey
		sink *parquetSink
	}
	var partitions []*partitionEntry

	// Create a buffering sink to capture values and partition key
	bufferSink := newBufferingSink(partitionSourceIDs, numPartitionFields)

	for _, msg := range batch {
		structured, err := msg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("failed to parse message as structured: %w", err)
		}

		row, ok := structured.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message is not an object, got %T", structured)
		}

		// Shred to buffer (captures values and partition key in one pass)
		bufferSink.reset()
		if err := rs.Shred(row, bufferSink); err != nil {
			return nil, fmt.Errorf("failed to shred record: %w", err)
		}

		// Compute partition key
		partitionKey, err := icebergx.NewPartitionKey(spec, schema, bufferSink.partitionValues)
		if err != nil {
			return nil, fmt.Errorf("failed to compute partition key: %w", err)
		}

		// Binary search for existing partition entry
		idx, found := slices.BinarySearchFunc(partitions, partitionKey, func(e *partitionEntry, k icebergx.PartitionKey) int {
			return e.key.Compare(k)
		})

		var entry *partitionEntry
		if found {
			entry = partitions[idx]
		} else {
			entry = &partitionEntry{
				key:  partitionKey,
				sink: newParquetSink(pqSchema, fieldToCol),
			}
			// Insert at sorted position
			partitions = slices.Insert(partitions, idx, entry)
		}

		// Write buffered values to the correct partition
		if err := bufferSink.writeTo(entry.sink); err != nil {
			return nil, fmt.Errorf("failed to write row to partition: %w", err)
		}
	}

	// Close all partition sinks and collect results (compute paths now)
	results := make([]partitionFile, 0, len(partitions))
	for _, entry := range partitions {
		result, err := entry.sink.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close parquet writer: %w", err)
		}
		path, err := icebergx.PartitionKeyToPath(spec, entry.key)
		if err != nil {
			return nil, fmt.Errorf("failed to compute partition path: %w", err)
		}
		results = append(results, partitionFile{path: path, result: result})
	}

	return results, nil
}

// Close closes the writer and its committer.
func (w *writer) Close() {
	w.committer.Close()
}

// parquetColumn holds state for writing to a single parquet column.
type parquetColumn struct {
	writer *parquet.ColumnWriter
	colIdx int             // column index for parquet.Value.Level()
	values []parquet.Value // accumulated values for current row
}

// parquetSink implements shredder.ShredderSink and writes values directly to column writers.
type parquetSink struct {
	buffer   *bytes.Buffer
	writer   *parquet.GenericWriter[any]
	columns  map[int]*parquetColumn // field ID -> column state
	rowCount int
}

func newParquetSink(pqSchema *parquet.Schema, fieldToCol map[int]int) *parquetSink {
	buf := bytes.NewBuffer(nil)
	pw := parquet.NewGenericWriter[any](buf, pqSchema)
	colWriters := pw.ColumnWriters()

	columns := make(map[int]*parquetColumn, len(fieldToCol))
	for fieldID, colIdx := range fieldToCol {
		columns[fieldID] = &parquetColumn{
			writer: colWriters[colIdx],
			colIdx: colIdx,
			values: make([]parquet.Value, 0, 8),
		}
	}
	return &parquetSink{
		buffer:  buf,
		writer:  pw,
		columns: columns,
	}
}

func (s *parquetSink) EmitValue(sv shredder.ShreddedValue) error {
	col, ok := s.columns[sv.FieldID]
	if !ok {
		return fmt.Errorf("unknown field ID: %d", sv.FieldID)
	}

	// Append the value with rep/def levels set
	val := sv.Value.Level(sv.RepLevel, sv.DefLevel, col.colIdx)
	col.values = append(col.values, val)

	return nil
}

func (s *parquetSink) OnNewField(_ icebergx.Path, _ string, _ any) {
	// For now, ignore unknown fields
	// TODO: Could be used for schema evolution
}

// flush writes the current row to column writers and increments the row count.
func (s *parquetSink) flush() error {
	for _, col := range s.columns {
		if _, err := col.writer.WriteRowValues(col.values); err != nil {
			return fmt.Errorf("failed to write to column %d: %w", col.colIdx, err)
		}
		// Reset slice but keep capacity
		col.values = col.values[:0]
	}
	s.rowCount++
	return nil
}

// Close closes the parquet writer and returns the result.
func (s *parquetSink) Close() (parquetResult, error) {
	if err := s.writer.Close(); err != nil {
		return parquetResult{}, err
	}
	return parquetResult{
		data:     s.buffer.Bytes(),
		rowCount: s.rowCount,
	}, nil
}

// bufferingSink captures shredded values and partition keys for later replay.
// This allows shredding once and then routing to the correct partition writer.
type bufferingSink struct {
	values             []shredder.ShreddedValue // buffered values in emission order
	partitionSourceIDs map[int]int              // sourceFieldID -> partition field index
	partitionValues    []parquet.Value          // captured partition values
}

func newBufferingSink(partitionSourceIDs map[int]int, numPartitionFields int) *bufferingSink {
	return &bufferingSink{
		values:             make([]shredder.ShreddedValue, 0, 64),
		partitionSourceIDs: partitionSourceIDs,
		partitionValues:    make([]parquet.Value, numPartitionFields),
	}
}

func (s *bufferingSink) reset() {
	s.values = s.values[:0]
	for i := range s.partitionValues {
		s.partitionValues[i] = parquet.Value{}
	}
}

func (s *bufferingSink) EmitValue(sv shredder.ShreddedValue) error {
	// Buffer the value
	s.values = append(s.values, sv)

	// Capture partition values (only top-level fields, rep level 0)
	if idx, ok := s.partitionSourceIDs[sv.FieldID]; ok && sv.RepLevel == 0 {
		s.partitionValues[idx] = sv.Value
	}

	return nil
}

func (s *bufferingSink) OnNewField(_ icebergx.Path, _ string, _ any) {
	// Ignore unknown fields
}

// writeTo replays buffered values to the target sink and flushes.
func (s *bufferingSink) writeTo(target *parquetSink) error {
	for _, sv := range s.values {
		if err := target.EmitValue(sv); err != nil {
			return err
		}
	}
	return target.flush()
}
