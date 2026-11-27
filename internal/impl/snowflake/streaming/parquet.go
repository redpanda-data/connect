// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streaming

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"golang.org/x/sync/errgroup"

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

// convertedChunk holds the rows of a slice of a batch in columnar form, ready
// to be written to a parquet row group.
type convertedChunk struct {
	// numRows is the number of rows in the chunk.
	numRows int
	// columns holds the values of each column, indexed by parquet column index.
	columns [][]parquet.Value
	// stats holds the statistics of each column, indexed like the transformers.
	stats []*statsBuffer
}

// convertObjectChunk validates and converts a batch of object messages into
// columnar form. It does not touch any parquet writer, so several chunks can be
// converted in parallel.
func convertObjectChunk(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
	mode SchemaMode,
) (*convertedChunk, error) {
	nameToPosition := make(map[string]int, len(transformers))
	for idx, t := range transformers {
		nameToPosition[t.name] = idx
	}
	return convertChunk(batch, schema, transformers, func(msg *service.Message, row []any) error {
		return objectMessageToRow(msg, row, nameToPosition, mode)
	})
}

// convertArrayChunk validates and converts a batch of array messages into
// columnar form. It does not touch any parquet writer, so several chunks can be
// converted in parallel.
func convertArrayChunk(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
	mode SchemaMode,
) (*convertedChunk, error) {
	return convertChunk(batch, schema, transformers, func(msg *service.Message, row []any) error {
		return arrayMessageToRow(msg, row, mode)
	})
}

func convertChunk(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
	toRow func(msg *service.Message, row []any) error,
) (*convertedChunk, error) {
	rowWidth := len(schema.Fields())
	stats := make([]*statsBuffer, rowWidth)
	buffers := make([]typedBuffer, rowWidth)
	columnIndexes := make([]int, rowWidth)

	for idx, t := range transformers {
		leaf, ok := schema.Lookup(t.name)
		if !ok {
			return nil, fmt.Errorf("invariant failed: unable to find column %q", t.name)
		}
		buffers[idx] = t.bufferFactory()
		buffers[idx].Reset(leaf.ColumnIndex, len(batch))
		columnIndexes[idx] = leaf.ColumnIndex
		stats[idx] = &statsBuffer{}
	}

	// Shred records into columns - snowflake's data model is a flat list of columns,
	// so no dremel style record shredding is needed.
	row := make([]any, rowWidth)
	for _, msg := range batch {
		if err := toRow(msg, row); err != nil {
			return nil, err
		}
		for i, v := range row {
			t := transformers[i]
			if err := t.converter.ValidateAndConvert(stats[i], v, buffers[i]); err != nil {
				if errors.Is(err, errNullValue) {
					return nil, &NonNullColumnError{msg, t.column.Name}
				}
				return nil, fmt.Errorf("invalid data for column %s: %w", t.name, err)
			}
			// reset the column as nil for the next row
			row[i] = nil
		}
	}

	columns := make([][]parquet.Value, rowWidth)
	for idx, b := range buffers {
		columns[columnIndexes[idx]] = b.Values()
	}
	return &convertedChunk{numRows: len(batch), columns: columns, stats: stats}, nil
}

// writeRowGroup writes the chunks, in order, into a single row group.
// Snowflake only reads the first row group of a BDEC file, so all the rows of a
// file must be written through one call to this function. Each column has its
// own writer and buffers, so columns are encoded and compressed in parallel,
// up to parallelism at a time.
func writeRowGroup(rg *parquet.ConcurrentRowGroupWriter, chunks []*convertedChunk, parallelism int) error {
	var g errgroup.Group
	g.SetLimit(parallelism)
	for columnIndex, cw := range rg.ColumnWriters() {
		g.Go(func() error {
			for _, chunk := range chunks {
				values := chunk.columns[columnIndex]
				if len(values) != chunk.numRows {
					return fmt.Errorf("invariant failed: column %d has %d values for %d rows", columnIndex, len(values), chunk.numRows)
				}
				if _, err := cw.WriteRowValues(values); err != nil {
					return fmt.Errorf("writing column %d: %w", columnIndex, err)
				}
			}
			if err := cw.Flush(); err != nil {
				return fmt.Errorf("flushing column %d: %w", columnIndex, err)
			}
			return nil
		})
	}
	return g.Wait()
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

// BeginRowGroup creates a new row group that is written through its column
// writers. A BDEC file must only ever contain one.
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

// verifyRowCounts checks that the parquet footer describes a single row group
// holding exactly the rows that were serialized. It guards against two classes
// of bad files being uploaded to Snowflake:
//
//   - Files with more than one row group. Snowflake's scanner only reads one
//     row group per BDEC file, so the rows of any other row group would be
//     silently dropped. The Java SDK always writes one row group per file,
//     and checks it in Iceberg mode.
//   - Internally inconsistent files, mirroring the Java SDK's verifyRowCounts
//     check (SNOW-1465503).
//
// The BDEC schema is flat, so every column chunk must record exactly one value
// (nulls included) per row.
func verifyRowCounts(expectedRows int64, md *format.FileMetaData) error {
	if len(md.RowGroups) != 1 {
		return fmt.Errorf("parquet row count verification failed: footer reports %d row groups, expected exactly 1 row group; refusing to upload a file Snowflake cannot fully read", len(md.RowGroups))
	}
	if md.NumRows != expectedRows {
		return fmt.Errorf("parquet row count verification failed: footer reports %d rows, expected %d rows; refusing to upload corrupted file", md.NumRows, expectedRows)
	}
	rg := md.RowGroups[0]
	if rg.NumRows != expectedRows {
		return fmt.Errorf("parquet row count verification failed: row group reports %d rows, expected %d rows; refusing to upload corrupted file", rg.NumRows, expectedRows)
	}
	for _, col := range rg.Columns {
		if col.MetaData.NumValues != rg.NumRows {
			return fmt.Errorf("parquet row count verification failed: column %q reports %d values, expected %d; refusing to upload corrupted file", strings.Join(col.MetaData.PathInSchema, "."), col.MetaData.NumValues, rg.NumRows)
		}
	}
	return nil
}

func totalUncompressedSize(metadata *format.FileMetaData) int32 {
	var size int64
	for _, rowGroup := range metadata.RowGroups {
		size += rowGroup.TotalByteSize
	}
	return int32(size)
}
