/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SchemaMode specifies how to handle schema mismatches when constructing parquet files
type SchemaMode int

const (
	// SchemaModeIgnoreExtra is a mode where unknown properties in messages are ignored
	SchemaModeIgnoreExtra SchemaMode = iota
	// SchemaModeStrict is a mode where non-null unknown properties in message result in errors
	SchemaModeStrict
	// SchemaModeStrictWithNulls is a mode where all unknown properties result in errors
	SchemaModeStrictWithNulls
)

// objectMessageToRow converts a message into columnar form using the provided name to index mapping.
// We have to materialize the column into a row so that we can know if a column is null - the
// msg can be sparse, but the row must not be sparse.
func objectMessageToRow(msg *service.Message, out []any, nameToPosition map[string]int, mode SchemaMode) error {
	v, err := msg.AsStructured()
	if err != nil {
		return fmt.Errorf("error extracting object from message: %w", err)
	}
	row, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("expected object, got: %T", v)
	}
	var missingColumns []*MissingColumnError
	for k, v := range row {
		idx, ok := nameToPosition[normalizeColumnName(k)]
		if !ok {
			if mode == SchemaModeStrict && v != nil {
				missingColumns = append(missingColumns, NewMissingColumnError(msg, k, v))
			} else if mode == SchemaModeStrictWithNulls {
				missingColumns = append(missingColumns, NewMissingColumnError(msg, k, v))
			}
			continue
		}
		out[idx] = v
	}
	if len(missingColumns) > 0 {
		return &BatchSchemaMismatchError[*MissingColumnError]{missingColumns}
	}
	return nil
}

// writeRowGroupFromObject writes a batch of object messages directly to a concurrent row group's column writers,
// then flushes (compresses) the row group. Values are written directly to the column writers as they are converted.
func writeRowGroupFromObject(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
	mode SchemaMode,
	rg *parquet.ConcurrentRowGroupWriter,
) ([]*statsBuffer, error) {
	rowWidth := len(schema.Fields())
	nameToPosition := make(map[string]int, rowWidth)
	stats := make([]*statsBuffer, rowWidth)
	buffers := make([]typedBuffer, rowWidth)
	columnWriters := rg.ColumnWriters()

	for idx, t := range transformers {
		leaf, ok := schema.Lookup(t.name)
		if !ok {
			return nil, fmt.Errorf("invariant failed: unable to find column %q", t.name)
		}
		buffers[idx] = t.bufferFactory()
		buffers[idx].Reset(columnWriters[leaf.ColumnIndex], leaf.ColumnIndex)
		stats[idx] = &statsBuffer{}
		nameToPosition[t.name] = idx
	}

	// Shred records into columns - snowflake's data model is a flat list of columns,
	// so no dremel style record shredding is needed. Values are written directly
	// to column writers as they are converted.
	row := make([]any, rowWidth)
	for _, msg := range batch {
		err := objectMessageToRow(msg, row, nameToPosition, mode)
		if err != nil {
			return nil, err
		}
		for i, v := range row {
			t := transformers[i]
			s := stats[i]
			b := buffers[i]
			err = t.converter.ValidateAndConvert(s, v, b)
			if err != nil {
				if errors.Is(err, errNullValue) {
					return nil, &NonNullColumnError{msg, t.column.Name}
				}
				return nil, fmt.Errorf("invalid data for column %s: %w", t.name, err)
			}
			// reset the column as nil for the next row
			row[i] = nil
		}
	}

	// Flush compresses the row group data
	if err := rg.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush row group: %w", err)
	}

	return stats, nil
}

// arrayMessageToRow converts a message into columnar form using the provided name to index mapping.
// We have to materialize the column into a row so that we can know if a column is null - the
// msg can be sparse, but the row must not be sparse.
func arrayMessageToRow(msg *service.Message, out []any, mode SchemaMode) error {
	v, err := msg.AsStructured()
	if err != nil {
		return fmt.Errorf("error extracting object from message: %w", err)
	}
	row, ok := v.([]any)
	if !ok {
		return fmt.Errorf("expected array, got: %T", v)
	}
	copy(out, row)
	if len(row) > len(out) && mode != SchemaModeIgnoreExtra {
		// We have extra columns here folks
		var missingColumns []*MissingColumnError
		for i, v := range row[len(out):] {
			if mode == SchemaModeStrict && v != nil {
				k := fmt.Sprintf("COLUMN_%d", len(out)+i)
				missingColumns = append(missingColumns, NewMissingColumnError(msg, k, v))
			} else if mode == SchemaModeStrictWithNulls {
				k := fmt.Sprintf("COLUMN_%d", len(out)+i)
				missingColumns = append(missingColumns, NewMissingColumnError(msg, k, v))
			}
		}
		if len(missingColumns) > 0 {
			return &BatchSchemaMismatchError[*MissingColumnError]{missingColumns}
		}
	}
	return nil
}

// writeRowGroupFromArray writes a batch of array messages directly to a concurrent row group's column writers,
// then flushes (compresses) the row group. Values are written directly to the column writers as they are converted.
func writeRowGroupFromArray(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
	mode SchemaMode,
	rg *parquet.ConcurrentRowGroupWriter,
) ([]*statsBuffer, error) {
	rowWidth := len(schema.Fields())
	stats := make([]*statsBuffer, rowWidth)
	buffers := make([]typedBuffer, rowWidth)
	columnWriters := rg.ColumnWriters()

	for idx, t := range transformers {
		leaf, ok := schema.Lookup(t.name)
		if !ok {
			return nil, fmt.Errorf("invariant failed: unable to find column %q", t.name)
		}
		buffers[idx] = t.bufferFactory()
		buffers[idx].Reset(columnWriters[leaf.ColumnIndex], leaf.ColumnIndex)
		stats[idx] = &statsBuffer{}
	}

	row := make([]any, rowWidth)
	for _, msg := range batch {
		err := arrayMessageToRow(msg, row, mode)
		if err != nil {
			return nil, err
		}
		for i, v := range row {
			t := transformers[i]
			s := stats[i]
			b := buffers[i]
			err = t.converter.ValidateAndConvert(s, v, b)
			if err != nil {
				if errors.Is(err, errNullValue) {
					return nil, &NonNullColumnError{msg, t.column.Name}
				}
				return nil, fmt.Errorf("invalid data for column %s: %w", t.name, err)
			}
			// reset the column as nil for the next row
			row[i] = nil
		}
	}

	// Flush compresses the row group data
	if err := rg.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush row group: %w", err)
	}

	return stats, nil
}

type parquetWriter struct {
	b      *bytes.Buffer
	w      *parquet.GenericWriter[any]
	schema *parquet.Schema
}

func newParquetWriter(rpcnVersion string, schema *parquet.Schema) *parquetWriter {
	b := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[any](
		b,
		schema,
		parquet.CreatedBy("RedpandaConnect", rpcnVersion, "unknown"),
		// Recommended by the Snowflake team to enable data page stats
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Zstd),
		parquet.WriteBufferSize(0),
	)
	return &parquetWriter{b, w, schema}
}

// BeginRowGroup creates a new concurrent row group for parallel construction.
func (w *parquetWriter) BeginRowGroup() *parquet.ConcurrentRowGroupWriter {
	return w.w.BeginRowGroup()
}

// Reset prepares the writer for a new file with the given metadata.
func (w *parquetWriter) Reset(metadata map[string]string) {
	for k, v := range metadata {
		w.w.SetKeyValueMetadata(k, v)
	}
	w.b.Reset()
	w.w.Reset(w.b)
}

// Close finalizes the parquet file and returns the bytes.
func (w *parquetWriter) Close() ([]byte, *format.FileMetaData, error) {
	if err := w.w.Close(); err != nil {
		return nil, nil, err
	}
	return w.b.Bytes(), w.w.File().Metadata(), nil
}

func totalUncompressedSize(metadata *format.FileMetaData) int32 {
	var size int64
	for _, rowGroup := range metadata.RowGroups {
		size += rowGroup.TotalByteSize
	}
	return int32(size)
}
