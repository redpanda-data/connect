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

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
)

type parquetColumnData struct {
	values           any // some slice of values
	definitionLevels []int16
}

func writeParquetFile(writer io.Writer, schema parquetSchema, data map[int32]parquetColumnData, md map[string]string) (err error) {
	kvMeta := metadata.KeyValueMetadata{}
	for k, v := range md {
		if err := kvMeta.Append(k, v); err != nil {
			return err
		}
	}
	props := parquet.NewWriterProperties(
		parquet.WithCreatedBy("RedpandaConnect latest (build main)"),
		parquet.WithStats(true),
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithEncoding(parquet.Encodings.Plain),
		parquet.WithVersion(parquet.V1_0),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithDictionaryDefault(false),
	)
	pw := file.NewParquetWriter(
		writer,
		schema,
		file.WithWriteMetadata(kvMeta),
		file.WithWriterProps(props),
	)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()
	rg := pw.AppendBufferedRowGroup()
	var cw file.ColumnChunkWriter
	for i := 0; i < rg.NumColumns(); i++ {
		cw, err = rg.Column(i)
		if err != nil {
			return err
		}
		column := data[cw.Descr().SchemaNode().FieldID()]
		switch vals := column.values.(type) {
		case []parquet.FixedLenByteArray:
			w := cw.(*file.FixedLenByteArrayColumnChunkWriter)
			_, err = w.WriteBatch(vals, column.definitionLevels, nil)
			if err != nil {
				return
			}
		case []parquet.ByteArray:
			w := cw.(*file.ByteArrayColumnChunkWriter)
			_, err = w.WriteBatch(vals, column.definitionLevels, nil)
			if err != nil {
				return
			}
		}
		err = cw.Close()
		if err != nil {
			return
		}
	}
	err = rg.Close()
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
