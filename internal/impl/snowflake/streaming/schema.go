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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

type dataConverter interface {
	ValidateAndConvert(buf *statsBuffer, val any) (any, error)
}

type dataTransformer struct {
	name      string // raw name from API
	converter dataConverter
	stats     *statsBuffer
}

func convertFixedType(column columnMetadata) (parquet.Node, dataConverter, error) {
	isDecimal := column.Scale != nil && column.Precision != nil
	if (column.Scale != nil && *column.Scale != 0) || strings.ToUpper(column.PhysicalType) == "SB16" {
		if isDecimal {
			return parquet.Decimal(int(*column.Scale), int(*column.Precision), parquet.FixedLenByteArrayType(16)), sb16Converter{column.Nullable}, nil
		}
		return parquet.Leaf(parquet.FixedLenByteArrayType(16)), sb16Converter{column.Nullable}, nil
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
		return parquet.Decimal(int(*column.Scale), int(*column.Precision), ptype), intConverter{column.Nullable}, nil
	}
	return parquet.Leaf(ptype), intConverter{column.Nullable}, nil
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
		var converter dataConverter
		switch strings.ToLower(column.LogicalType) {
		case "fixed":
			n, converter, err = convertFixedType(column)
			if err != nil {
				return nil, nil, nil, err
			}
		case "array":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonArrayConverter{jsonConverter{binaryConverter{column.Nullable}}}
		case "object":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonObjectConverter{jsonConverter{binaryConverter{column.Nullable}}}
		case "variant":
			typeMetadata[fmt.Sprintf("%d:obj_enc", id)] = "1"
			n = parquet.String()
			converter = jsonConverter{binaryConverter{column.Nullable}}
		case "text":
			fallthrough
		case "char":
			fallthrough
		case "any":
			n = parquet.String()
			converter = binaryConverter{column.Nullable}
		case "binary":
			n = parquet.Leaf(parquet.ByteArrayType)
			converter = binaryConverter{column.Nullable}
		case "boolean":
			n = parquet.Leaf(parquet.BooleanType)
			converter = boolConverter{column.Nullable}
		case "real":
			n = parquet.Leaf(parquet.DoubleType)
			converter = doubleConverter{column.Nullable}
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
		typeMetadata[strconv.Itoa(id)] = fmt.Sprintf("%d,%d", logicalTypeOrdinal(column.LogicalType), physicalTypeOrdinal(column.PhysicalType))
		name := normalizeColumnName(column.Name)
		groupNode[name] = n
		transformers[name] = &dataTransformer{
			name:      column.Name,
			converter: converter,
			stats:     &statsBuffer{columnID: id},
		}
	}
	return parquet.NewSchema("bdec", groupNode), transformers, typeMetadata, nil
}

type statsBuffer struct {
	columnID               int
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

var errNullValue = errors.New("unexpected null value")

type boolConverter struct {
	nullable bool
}

func (c boolConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
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

type intConverter struct {
	nullable bool
}

func (c intConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
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

type sb16Converter struct {
	nullable bool
}

func (c sb16Converter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
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

type doubleConverter struct {
	nullable bool
}

func (c doubleConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
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

type binaryConverter struct {
	nullable bool
}

func (c binaryConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
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

type stringConverter struct {
	binaryConverter
}

func (c stringConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	v, err := c.binaryConverter.ValidateAndConvert(buf, val)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(v.([]byte)) {
		return v, errors.New("invalid UTF8")
	}
	return v, err
}

type jsonConverter struct {
	binaryConverter
}

var errInvalidJSON = errors.New("invalid json")

func (c jsonConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	v, err := c.binaryConverter.ValidateAndConvert(buf, val)
	if err != nil {
		return nil, err
	}
	if !json.Valid(v.([]byte)) {
		return nil, errInvalidJSON
	}
	return v, nil
}

type jsonArrayConverter struct {
	jsonConverter
}

func (c jsonArrayConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	v, err := c.jsonConverter.ValidateAndConvert(buf, val)
	if err != nil {
		return nil, err
	}
	// Already valid JSON, so now we're just checking the first byte is a `[` so it's an array
	if !bytes.HasPrefix(bytes.TrimSpace(v.([]byte)), []byte{'['}) {
		return nil, errors.New("not a JSON array")
	}
	return v, nil
}

type jsonObjectConverter struct {
	jsonConverter
}

func (c jsonObjectConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	v, err := c.jsonConverter.ValidateAndConvert(buf, val)
	if err != nil {
		return nil, err
	}
	// Already valid JSON, so now we're just checking the first byte is a `{` so it's an object
	if !bytes.HasPrefix(bytes.TrimSpace(v.([]byte)), []byte{'{'}) {
		return nil, errors.New("not a JSON object")
	}
	return v, nil
}

type timestampConverter struct {
	nullable bool
	scale    int
	tz       bool
}

func (c timestampConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
		buf.nullCount++
		return val, nil
	}
	t, err := bloblang.ValueAsTimestamp(val)
	if err != nil {
		return val, err
	}
	v := snowflakeTimestampInt(t, c.scale, c.tz)
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

func computeColumnEpInfo(stats map[string]*dataTransformer) map[string]fileColumnProperties {
	info := map[string]fileColumnProperties{}
	for _, transformer := range stats {
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
		info[transformer.name] = fileColumnProperties{
			ColumnOrdinal:  int32(stat.columnID),
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
