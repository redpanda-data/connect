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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/parquet-go/parquet-go"
)

type dataTransformer struct {
	converter     dataConverter
	column        *columnMetadata
	bufferFactory typedBufferFactory
	name          string
}

func convertFixedType(column columnMetadata) (parquet.Node, dataConverter, typedBufferFactory, error) {
	var scale int32
	var precision int32
	if column.Scale != nil {
		scale = *column.Scale
	}
	if column.Precision != nil {
		precision = *column.Precision
	}
	isDecimal := column.Scale != nil && column.Precision != nil
	if (column.Scale != nil && *column.Scale != 0) || strings.ToUpper(column.PhysicalType) == "SB16" {
		c := numberConverter{nullable: column.Nullable, scale: scale, precision: precision}
		b := defaultTypedBufferFactory
		t := parquet.FixedLenByteArrayType(16)
		if isDecimal {
			return parquet.Decimal(int(scale), int(precision), t), c, b, nil
		}
		return parquet.Leaf(t), c, b, nil
	}
	var ptype parquet.Type
	var defaultPrecision int32
	var bufferFactory typedBufferFactory
	switch strings.ToUpper(column.PhysicalType) {
	case "SB1":
		ptype = parquet.Int32Type
		defaultPrecision = maxPrecisionForByteWidth(1)
		bufferFactory = int32TypedBufferFactory
	case "SB2":
		ptype = parquet.Int32Type
		defaultPrecision = maxPrecisionForByteWidth(2)
		bufferFactory = int32TypedBufferFactory
	case "SB4":
		ptype = parquet.Int32Type
		defaultPrecision = maxPrecisionForByteWidth(4)
		bufferFactory = int32TypedBufferFactory
	case "SB8":
		ptype = parquet.Int64Type
		defaultPrecision = maxPrecisionForByteWidth(8)
		bufferFactory = int64TypedBufferFactory
	default:
		return nil, nil, nil, fmt.Errorf("unsupported physical column type: %s", column.PhysicalType)
	}
	validationPrecision := precision
	if column.Precision == nil {
		validationPrecision = defaultPrecision
	}
	c := numberConverter{nullable: column.Nullable, scale: scale, precision: validationPrecision}
	if isDecimal {
		return parquet.Decimal(int(scale), int(precision), ptype), c, bufferFactory, nil
	}
	return parquet.Leaf(ptype), c, bufferFactory, nil
}

// maxJSONSize is the size that any kind of semi-structured data can be, which is 16MiB minus a small overhead
const maxJSONSize = 16*humanize.MiByte - 64

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (*parquet.Schema, []*dataTransformer, map[string]string, error) {
	groupNode := parquet.Group{}
	transformers := make([]*dataTransformer, len(columns))
	// Don't write the sfVer key as it allows us to not have to narrow the numeric types in parquet.
	typeMetadata := map[string]string{ /*"sfVer": "1,1"*/ }
	var err error
	for idx, column := range columns {
		id := int(column.Ordinal)
		var n parquet.Node
		var converter dataConverter
		var bufferFactory typedBufferFactory = defaultTypedBufferFactory
		logicalType := strings.ToLower(column.LogicalType)
		switch logicalType {
		case "fixed":
			n, converter, bufferFactory, err = convertFixedType(column)
			if err != nil {
				return nil, nil, nil, err
			}
		case "array":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonArrayConverter{jsonConverter{column.Nullable, maxJSONSize}}
		case "object":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonObjectConverter{jsonConverter{column.Nullable, maxJSONSize}}
		case "variant":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonConverter{column.Nullable, maxJSONSize}
		case "any", "text", "char":
			n = parquet.String()
			byteLength := 16 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = binaryConverter{nullable: column.Nullable, maxLength: byteLength, utf8: true}
		case "binary":
			n = parquet.Leaf(parquet.ByteArrayType)
			// Why binary data defaults to 8MiB instead of the 16MiB for strings... ¯\_(ツ)_/¯
			byteLength := 8 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = binaryConverter{nullable: column.Nullable, maxLength: byteLength}
		case "boolean":
			n = parquet.Leaf(parquet.BooleanType)
			converter = boolConverter{column.Nullable}
		case "real":
			n = parquet.Leaf(parquet.DoubleType)
			converter = doubleConverter{column.Nullable}
		case "timestamp_tz", "timestamp_ltz", "timestamp_ntz":
			var scale, precision int32
			var pt parquet.Type
			if column.PhysicalType == "SB8" {
				pt = parquet.Int64Type
				precision = maxPrecisionForByteWidth(8)
				bufferFactory = int64TypedBufferFactory
			} else {
				pt = parquet.FixedLenByteArrayType(16)
				precision = maxPrecisionForByteWidth(16)
			}
			if column.Scale != nil {
				scale = *column.Scale
			}
			// The server always returns 0 precision for timestamp columns,
			// the Java SDK also seems to not validate precision of timestamps
			// so ignore it and use the default precision for the column type
			n = parquet.Decimal(int(scale), int(precision), pt)
			converter = timestampConverter{
				nullable:  column.Nullable,
				scale:     scale,
				precision: precision,
				includeTZ: logicalType == "timestamp_tz",
				trimTZ:    logicalType == "timestamp_ntz",
				defaultTZ: time.UTC,
			}
		case "time":
			t := parquet.Int32Type
			precision := 9
			bufferFactory = int32TypedBufferFactory
			if column.PhysicalType == "SB8" {
				t = parquet.Int64Type
				precision = 18
				bufferFactory = int64TypedBufferFactory
			}
			scale := int32(9)
			if column.Scale != nil {
				scale = *column.Scale
			}
			n = parquet.Decimal(int(scale), precision, t)
			converter = timeConverter{column.Nullable, scale}
		case "date":
			n = parquet.Leaf(parquet.Int32Type)
			converter = dateConverter{column.Nullable}
			bufferFactory = int32TypedBufferFactory
		default:
			return nil, nil, nil, fmt.Errorf("unsupported logical column type: %s", column.LogicalType)
		}
		if column.Nullable {
			n = parquet.Optional(n)
		}
		n = parquet.FieldID(n, id)
		// Use plain encoding for now as there seems to be compatibility issues with the default settings
		// we might be able to tune this more.
		n = parquet.Encoded(n, &parquet.Plain)
		typeMetadata[strconv.Itoa(id)] = fmt.Sprintf(
			"%d,%d",
			logicalTypeOrdinal(column.LogicalType),
			physicalTypeOrdinal(column.PhysicalType),
		)
		name := normalizeColumnName(column.Name)
		groupNode[name] = n
		transformers[idx] = &dataTransformer{
			name:          name,
			converter:     converter,
			column:        &column,
			bufferFactory: bufferFactory,
		}
	}
	return parquet.NewSchema("bdec", groupNode), transformers, typeMetadata, nil
}

func physicalTypeOrdinal(str string) int {
	switch strings.ToUpper(str) {
	case "ROWINDEX":
		return 9
	case "DOUBLE":
		return 7
	case "SB1":
		return 1
	case "SB2":
		return 2
	case "SB4":
		return 3
	case "SB8":
		return 4
	case "SB16":
		return 5
	case "LOB":
		return 8
	case "ROW":
		return 10
	}
	return -1
}

func logicalTypeOrdinal(str string) int {
	switch strings.ToUpper(str) {
	case "BOOLEAN":
		return 1
	case "NULL":
		return 15
	case "REAL":
		return 8
	case "FIXED":
		return 2
	case "TEXT":
		return 9
	case "BINARY":
		return 10
	case "DATE":
		return 7
	case "TIME":
		return 6
	case "TIMESTAMP_LTZ":
		return 3
	case "TIMESTAMP_NTZ":
		return 4
	case "TIMESTAMP_TZ":
		return 5
	case "ARRAY":
		return 13
	case "OBJECT":
		return 12
	case "VARIANT":
		return 11
	}
	return -1
}

func maxPrecisionForByteWidth(byteWidth int) int32 {
	switch byteWidth {
	case 1:
		return 3
	case 2:
		return 5
	case 4:
		return 9
	case 8:
		return 18
	}
	return 38
}
