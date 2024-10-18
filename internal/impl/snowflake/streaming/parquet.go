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

func messageToRow(msg *service.Message) (map[string]any, error) {
	v, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("error extracting object from message: %w", err)
	}
	row, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object, got: %T", v)
	}
	mapped := make(map[string]any, len(row))
	for k, v := range row {
		mapped[normalizeColumnName(k)] = v
	}
	return mapped, nil
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
	transformers map[string]*dataTransformer,
) ([]parquet.Row, error) {
	// We write all of our data in a columnar fashion, but need to pivot that data so that we can feed it into
	// out parquet library (which sadly will redo the pivot - maybe we need a lower level abstraction...).
	// So create a massive matrix that we will write stuff in columnar form, but then we don't need to move any
	// data to create rows of the data via an in-place transpose operation.
	//
	// TODO: Consider caching/pooling this matrix as I expect many are similarily sized.
	matrix := make([]parquet.Value, len(batch)*len(schema.Fields()))
	rowWidth := len(schema.Fields())
	for idx, field := range schema.Fields() {
		// The column index is consistent between two schemas that are the same because the schema fields are always
		// in sorted order.
		columnIndex := idx
		t := transformers[field.Name()]
		t.buf.Prepare(matrix, columnIndex, rowWidth)
		t.stats.Reset()
	}
	// First we need to shred our record into columns, snowflake's data model
	// is thankfully a flat list of columns, so no dremel style record shredding
	// is needed
	for _, msg := range batch {
		row, err := messageToRow(msg)
		if err != nil {
			return nil, err
		}
		// We **must** write a null, so iterate over the schema not the record,
		// which might be sparse
		for name, t := range transformers {
			v := row[name]
			err = t.converter.ValidateAndConvert(t.stats, v, t.buf)
			if err != nil {
				return nil, fmt.Errorf("invalid data for column %s: %w", name, err)
			}
		}
	}
	// Now all our values have been written to each buffer - here is where we do our matrix
	// transpose mentioned above
	rows := make([]parquet.Row, len(batch))
	for i := range rows {
		rowStart := i * rowWidth
		rows[i] = matrix[rowStart : rowStart+rowWidth]
	}
	return rows, nil
}

type parquetFileData struct {
	schema   *parquet.Schema
	rows     []parquet.Row
	metadata map[string]string
}

func writeParquetFile(writer io.Writer, data parquetFileData) (err error) {
	pw := parquet.NewGenericWriter[map[string]any](
		writer,
		data.schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
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
