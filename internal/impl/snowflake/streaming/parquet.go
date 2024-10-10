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
	"encoding/binary"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
)

func writeParquetFile(schema *parquet.Schema, rows []map[string]any, metadata map[string]string) ([]byte, error) {
	buf := &bytes.Buffer{}
	pw := parquet.NewGenericWriter[map[string]any](
		buf,
		schema,
		parquet.CreatedBy("RedpandaConnect", version, "main"),
		// Recommended by the Snowflake team to enable data page stats
		parquet.DataPageStatistics(true),
		parquet.Compression(&parquet.Zstd),
	)
	for k, v := range metadata {
		pw.SetKeyValueMetadata(k, v)
	}
	err := writeWithoutPanic(pw, rows)
	if err != nil {
		return nil, err
	}
	err = closeWithoutPanic(pw)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeWithoutPanic[T any](pWtr *parquet.GenericWriter[T], rows []T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	_, err = pWtr.Write(rows)
	return
}

func closeWithoutPanic[T any](pWtr *parquet.GenericWriter[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	err = pWtr.Close()
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
