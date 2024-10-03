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
	"fmt"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

type dataTransformerFn func(buf *statsBuffer, val any) (any, error)

type dataTransformer struct {
	converter dataTransformerFn
	stats     *statsBuffer
}

func convertFixedType(column columnMetadata) (parquet.Node, dataTransformerFn, error) {
	isDecimal := column.Scale != nil && column.Precision != nil
	if (column.Scale != nil && *column.Scale != 0) || strings.ToUpper(column.PhysicalType) == "SB16" {
		if isDecimal {
			return parquet.Decimal(int(*column.Scale), int(*column.Precision), parquet.FixedLenByteArrayType(16)), incrementIntAsFixedArray16Stat, nil
		}
		return parquet.Leaf(parquet.FixedLenByteArrayType(16)), incrementBinaryStat, nil
	}
	var ptype parquet.Type
	switch strings.ToUpper(column.PhysicalType) {
	case "SB1":
	case "SB2":
	case "SB4":
		ptype = parquet.Int32Type
	case "SB8":
		ptype = parquet.Int64Type
	default:
		return nil, nil, fmt.Errorf("unsupported physical column type: %s", column.PhysicalType)
	}
	if isDecimal {
		return parquet.Decimal(int(*column.Scale), int(*column.Precision), ptype), incrementIntStat, nil
	}
	return parquet.Leaf(ptype), incrementIntStat, nil
}

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (*parquet.Schema, map[string]*dataTransformer, map[string]string, error) {
	groupNode := parquet.Group{}
	transformers := map[string]*dataTransformer{}
	typeMetadata := map[string]string{"sfVer": "1,1"}
	var err error
	for _, column := range columns {
		id := int(column.Ordinal)
		var n parquet.Node
		var converter dataTransformerFn
		switch strings.ToLower(column.LogicalType) {
		case "fixed":
			n, converter, err = convertFixedType(column)
			if err != nil {
				return nil, nil, nil, err
			}
		case "text":
			fallthrough
		case "char":
			fallthrough
		case "any":
			fallthrough
		case "binary":
			n = parquet.String()
			converter = incrementBinaryStat
		case "boolean":
			n = parquet.Leaf(parquet.BooleanType)
			converter = incrementBoolStat
		case "real":
			n = parquet.Leaf(parquet.DoubleType)
			converter = incrementDoubleStat
		default:
			return nil, nil, nil, fmt.Errorf("unsupported logical column type: %s", column.LogicalType)
		}
		if column.Nullable {
			n = parquet.Optional(n)
		}
		n = parquet.FieldID(n, id)
		n = parquet.Encoded(n, &parquet.Plain)
		n = parquet.Compressed(n, &parquet.Gzip)
		typeMetadata[strconv.Itoa(id)] = fmt.Sprintf("%d,%d", logicalTypeOrdinal(column.LogicalType), physicalTypeOrdinal(column.PhysicalType))
		if map[string]bool{"ARRAY": true, "OBJECT": true, "VARIANT": true}[strings.ToUpper(column.LogicalType)] {
			// mark the column metadata as being an object json for the server side scanner
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
		}
		// TODO: Use the unquoted name
		groupNode[column.Name] = n
		transformers[column.Name] = &dataTransformer{converter: converter, stats: &statsBuffer{columnId: id}}
	}
	return parquet.NewSchema("bdec", groupNode), transformers, typeMetadata, nil
}

type statsBuffer struct {
	columnId               int
	minIntVal, maxIntVal   int64
	minRealVal, maxRealVal float64
	minStrVal, maxStrVal   []byte
	maxStrLen              int
	nullCount              int64
	first                  bool
}

func (s *statsBuffer) Reset() {
	s.first = true
	s.minIntVal = 0
	s.maxIntVal = 0
	s.minRealVal = 0
	s.maxRealVal = 0
	s.minStrVal = nil
	s.maxStrVal = nil
	s.maxStrLen = 0
	s.nullCount = 0
}

func incrementBoolStat(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsBool(val)
	if err != nil {
		return val, err
	}
	var i int64 = 0
	if v {
		i = 1
	}
	if buf.first {
		buf.minIntVal = i
		buf.maxIntVal = i
		buf.first = false
		return v, nil
	}
	buf.minIntVal = min(buf.minIntVal, i)
	buf.maxIntVal = max(buf.maxIntVal, i)
	return v, nil
}

func incrementIntStat(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsInt64(val)
	if err != nil {
		return val, err
	}
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
		return v, nil
	}
	buf.minIntVal = min(buf.minIntVal, v)
	buf.maxIntVal = max(buf.maxIntVal, v)
	return v, nil
}

func incrementIntAsFixedArray16Stat(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsInt64(val)
	if err != nil {
		return val, err
	}
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
		return int64ToInt128Binary(v), nil
	}
	buf.minIntVal = min(buf.minIntVal, v)
	buf.maxIntVal = max(buf.maxIntVal, v)
	return int64ToInt128Binary(v), nil
}

func incrementDoubleStat(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsFloat64(val)
	if err != nil {
		return val, err
	}
	if buf.first {
		buf.minRealVal = v
		buf.maxRealVal = v
		buf.first = false
		return v, nil
	}
	buf.minRealVal = min(buf.minRealVal, v)
	buf.maxRealVal = max(buf.maxRealVal, v)
	return v, nil
}

func incrementBinaryStat(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsBytes(val)
	if err != nil {
		return val, err
	}
	if buf.first {
		buf.minStrVal = v
		buf.maxStrVal = v
		buf.maxStrLen = len(v)
		buf.first = false
		return v, nil
	}
	if bytes.Compare(v, buf.minStrVal) < 0 {
		buf.minStrVal = v
	}
	if bytes.Compare(v, buf.maxStrVal) > 0 {
		buf.maxStrVal = v
	}
	buf.maxStrLen = max(buf.maxStrLen, len(v))
	return v, nil
}

func computeColumnEpInfo(stats map[string]*dataTransformer) map[string]fileColumnProperties {
	info := map[string]fileColumnProperties{}
	for colName, transformer := range stats {
		stat := transformer.stats
		var minStrVal *string = nil
		if stat.minStrVal != nil {
			s := truncateBytesAsHex(stat.minStrVal, false)
			minStrVal = &s
		}
		var maxStrVal *string = nil
		if stat.maxStrVal != nil {
			s := truncateBytesAsHex(stat.maxStrVal, true)
			maxStrVal = &s
		}
		info[colName] = fileColumnProperties{
			ColumnOrdinal:  int32(stat.columnId),
			NullCount:      stat.nullCount,
			MinStrValue:    minStrVal,
			MaxStrValue:    maxStrVal,
			MaxLength:      int64(stat.maxStrLen),
			MinIntValue:    stat.minIntVal,
			MaxIntValue:    stat.maxIntVal,
			MinRealValue:   stat.minRealVal,
			MaxRealValue:   stat.maxRealVal,
			DistinctValues: -1,
		}
	}
	return info
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
