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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/encoding/thrift"
)

// messageToRow converts a message into columnar form using the provided name to index mapping.
// We have to materialize the column into a row so that we can know if a column is null - the
// msg can be sparse, but the row must not be sparse.
func messageToRow(msg *service.Message, out []any, nameToPosition map[string]int) error {
	v, err := msg.AsStructured()
	if err != nil {
		return fmt.Errorf("error extracting object from message: %w", err)
	}
	row, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("expected object, got: %T", v)
	}
	for k, v := range row {
		idx, ok := nameToPosition[normalizeColumnName(k)]
		if !ok {
			// TODO(schema): Unknown column, we just skip it.
			// In the future we may evolve the schema based on the new data.
			continue
		}
		out[idx] = v
	}
	return nil
}

// TODO: If the memory pressure is too great from writing all
// records buffered as a single row group, then consider
// return some kind of iterator of chunks of rows that we can
// then feed into the actual parquet construction process.
//
// If a single parquet file is too much, we can consider having multiple
// parquet files in a single bdec file.
func constructRowGroup(
	batch service.MessageBatch,
	schema *parquet.Schema,
	transformers []*dataTransformer,
) ([]parquet.Row, []*statsBuffer, error) {
	// We write all of our data in a columnar fashion, but need to pivot that data so that we can feed it into
	// out parquet library (which sadly will redo the pivot - maybe we need a lower level abstraction...).
	// So create a massive matrix that we will write stuff in columnar form, but then we don't need to move any
	// data to create rows of the data via an in-place transpose operation.
	//
	// TODO: Consider caching/pooling this matrix as I expect many are similarily sized.
	rowWidth := len(schema.Fields())
	matrix := make([]parquet.Value, len(batch)*rowWidth)
	nameToPosition := make(map[string]int, rowWidth)
	stats := make([]*statsBuffer, rowWidth)
	for idx, t := range transformers {
		leaf, ok := schema.Lookup(t.name)
		if !ok {
			return nil, nil, fmt.Errorf("invariant failed: unable to find column %q", t.name)
		}
		t.buf.Prepare(matrix, leaf.ColumnIndex, rowWidth)
		stats[idx] = &statsBuffer{}
		nameToPosition[t.name] = idx
	}
	// First we need to shred our record into columns, snowflake's data model
	// is thankfully a flat list of columns, so no dremel style record shredding
	// is needed
	row := make([]any, rowWidth)
	for _, msg := range batch {
		err := messageToRow(msg, row, nameToPosition)
		if err != nil {
			return nil, nil, err
		}
		for i, v := range row {
			t := transformers[i]
			s := stats[i]
			err = t.converter.ValidateAndConvert(s, v, t.buf)
			if err != nil {
				// TODO(schema): if this is a null value err then we can evolve the schema to mark it null.
				return nil, nil, fmt.Errorf("invalid data for column %s: %w", t.name, err)
			}
			// reset the column as nil for the next row
			row[i] = nil
		}
	}
	// Now all our values have been written to each buffer - here is where we do our matrix
	// transpose mentioned above
	rows := make([]parquet.Row, len(batch))
	for i := range rows {
		rowStart := i * rowWidth
		rows[i] = matrix[rowStart : rowStart+rowWidth]
	}
	return rows, stats, nil
}

type parquetFileData struct {
	schema   *parquet.Schema
	rows     []parquet.Row
	metadata map[string]string
}

func writeParquetFile(writer io.Writer, rpcnVersion string, data parquetFileData) (err error) {
	pw := parquet.NewGenericWriter[map[string]any](
		writer,
		data.schema,
		parquet.CreatedBy("RedpandaConnect", rpcnVersion, "unknown"),
		// Recommended by the Snowflake team to enable data page stats
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Zstd),
	)
	for k, v := range data.metadata {
		pw.SetKeyValueMetadata(k, v)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()
	_, err = pw.WriteRows(data.rows)
	if err != nil {
		return
	}
	err = pw.Close()
	return
}

func readParquetMetadata(parquetFile []byte) (metadata format.FileMetaData, err error) {
	if len(parquetFile) < 8 {
		return format.FileMetaData{}, fmt.Errorf("too small of parquet file: %d", len(parquetFile))
	}
	trailingBytes := parquetFile[len(parquetFile)-8:]
	if string(trailingBytes[4:]) != "PAR1" {
		return metadata, fmt.Errorf("missing magic bytes, got: %q", trailingBytes[4:])
	}
	footerSize := int(binary.LittleEndian.Uint32(trailingBytes))
	if len(parquetFile) < footerSize+8 {
		return metadata, fmt.Errorf("too small of parquet file: %d, footer size: %d", len(parquetFile), footerSize)
	}
	footerBytes := parquetFile[len(parquetFile)-(footerSize+8) : len(parquetFile)-8]
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), footerBytes, &metadata); err != nil {
		return metadata, fmt.Errorf("unable to extract parquet metadata: %w", err)
	}
	return
}

func totalUncompressedSize(metadata format.FileMetaData) int32 {
	var size int64
	for _, rowGroup := range metadata.RowGroups {
		size += rowGroup.TotalByteSize
	}
	return int32(size)
}
