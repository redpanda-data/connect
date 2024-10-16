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
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dustin/go-humanize"
	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

var pow10TableInt64 []int64

func init() {
	pow10TableInt64 = make([]int64, 19)
	n := int64(1)
	pow10TableInt64[0] = n
	for i := range pow10TableInt64[1:] {
		n = 10 * n
		pow10TableInt64[i+1] = n
	}
}

type dataConverter interface {
	ValidateAndConvert(buf *statsBuffer, val any) (any, error)
}

type dataTransformer struct {
	name      string // raw name from API
	converter dataConverter
	stats     *statsBuffer
}

func convertFixedType(column columnMetadata) (parquet.Node, dataConverter, error) {
	scale := 0
	precision := 0
	if column.Scale != nil {
		scale = int(*column.Scale)
	}
	if column.Precision != nil {
		precision = int(*column.Precision)
	}
	isDecimal := column.Scale != nil && column.Precision != nil
	if (column.Scale != nil && *column.Scale != 0) || strings.ToUpper(column.PhysicalType) == "SB16" {
		c := sb16Converter{nullable: column.Nullable, scale: scale, precision: precision}
		if isDecimal {
			return parquet.Decimal(scale, precision, parquet.FixedLenByteArrayType(16)), c, nil
		}
		return parquet.Leaf(parquet.FixedLenByteArrayType(16)), c, nil
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
	c := intConverter{nullable: column.Nullable, scale: scale, precision: precision}
	if isDecimal {
		return parquet.Decimal(scale, precision, ptype), c, nil
	}
	return parquet.Leaf(ptype), c, nil
}

type parquetSchema = *parquet.Schema

// maxJSONSize is the size that any kind of semi-structured data can be, which is 16MiB minus a small overhead
const maxJSONSize = 16*humanize.MiByte - 64

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (parquetSchema, map[string]*dataTransformer, map[string]string, error) {
	groupNode := parquet.Group{}
	transformers := map[string]*dataTransformer{}
	typeMetadata := map[string]string{"sfVer": "1,1"}
	var err error
	for _, column := range columns {
		id := int(column.Ordinal)
		var n parquet.Node
		var converter dataConverter
		logicalType := strings.ToLower(column.LogicalType)
		switch logicalType {
		case "fixed":
			n, converter, err = convertFixedType(column)
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
		case "text":
			fallthrough
		case "char":
			fallthrough
		case "any":
			n = parquet.String()
			byteLength := 16 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = stringConverter{binaryConverter{column.Nullable, byteLength}}
		case "binary":
			n = parquet.Leaf(parquet.ByteArrayType)
			// Why binary data defaults to 8MiB instead of the 16MiB for strings... ¯\_(ツ)_/¯
			byteLength := 8 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = binaryConverter{column.Nullable, byteLength}
		case "boolean":
			n = parquet.Leaf(parquet.BooleanType)
			converter = boolConverter{column.Nullable}
		case "real":
			n = parquet.Leaf(parquet.DoubleType)
			converter = doubleConverter{column.Nullable}
		case "timestamp_ltz":
			fallthrough
		case "timestamp_ntz":
			fallthrough
		case "timestamp_tz":
			if column.PhysicalType == "SB8" {
				n = parquet.Leaf(parquet.Int64Type)
			} else {
				n = parquet.Leaf(parquet.FixedLenByteArrayType(16))
			}
			scale := 0
			if column.Scale != nil {
				scale = int(*column.Scale)
			}
			tz := logicalType != "timestamp_ntz"
			converter = timestampConverter{column.Nullable, scale, tz}
		case "time":
			if column.PhysicalType == "SB8" {
				n = parquet.Leaf(parquet.Int64Type)
			} else {
				n = parquet.Leaf(parquet.Int32Type)
			}
			scale := 9
			if column.Scale != nil {
				scale = int(*column.Scale)
			}
			converter = timeConverter{column.Nullable, scale}
		case "date":
			n = parquet.Leaf(parquet.Int32Type)
			converter = dateConverter{column.Nullable}
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
	nullable  bool
	scale     int
	precision int
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
	nullable  bool
	scale     int
	precision int
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
	} else {
		buf.minIntVal = min(buf.minIntVal, v)
		buf.maxIntVal = max(buf.maxIntVal, v)
	}
	return packInteger(v), nil
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
	nullable  bool
	maxLength int
}

func (c binaryConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return val, errNullValue
		}
		buf.nullCount++
		return val, nil
	}
	v, err := bloblang.ValueAsBytes(val)
	if err != nil {
		return val, err
	}
	if len(v) > c.maxLength {
		return val, fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
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
	nullable  bool
	maxLength int
}

func (c jsonConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
		buf.nullCount++
		return val, nil
	}
	v := []byte(bloblang.ValueToString(val))
	if len(v) > c.maxLength {
		return val, fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
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

type jsonArrayConverter struct {
	jsonConverter
}

func (c jsonArrayConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val != nil {
		if _, ok := val.([]any); !ok {
			return nil, errors.New("not a JSON array")
		}
	}
	return c.jsonConverter.ValidateAndConvert(buf, val)
}

type jsonObjectConverter struct {
	jsonConverter
}

func (c jsonObjectConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val != nil {
		if _, ok := val.(map[string]any); !ok {
			return nil, errors.New("not a JSON object")
		}
	}
	return c.jsonConverter.ValidateAndConvert(buf, val)
}

type timestampConverter struct {
	nullable bool
	scale    int
	tz       bool
}

var timestampFormats = []string{
	time.DateTime,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05.000",
	"2006-01-02 15:04:05.000-0700",
	"2006-01-02 15:04:05.000-07:00",
}

func (c timestampConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
		buf.nullCount++
		return val, nil
	}
	var s string
	switch v := val.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	}
	var t time.Time
	var err error
	if s != "" {
		for _, format := range timestampFormats {
			t, err = time.Parse(format, s)
			if err == nil {
				break
			}
		}
	} else {
		err = errors.ErrUnsupported
	}
	if err != nil {
		t, err = bloblang.ValueAsTimestamp(val)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to coerse TIMESTAMP value from %v", val)
	}
	v := snowflakeTimestampInt(t, c.scale, c.tz)
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
	} else {
		buf.minIntVal = min(buf.minIntVal, v)
		buf.maxIntVal = max(buf.maxIntVal, v)
	}
	return v, nil
}

type timeConverter struct {
	nullable bool
	scale    int
}

func (c timeConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
		buf.nullCount++
		return val, nil
	}

	var s string
	switch v := val.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	}
	s = strings.TrimSpace(s)
	var t time.Time
	var err error
	switch len(s) {
	case len("15:04"):
		t, err = time.Parse("15:04", s)
	case len("15:04:05"):
		t, err = time.Parse("15:04:05", s)
	default:
		// Allow up to 9 decimal places
		padding := len(s) - len("15:04:05.")
		if padding >= 0 {
			t, err = time.Parse("15:04:05."+strings.Repeat("0", min(padding, 9)), s)
		} else {
			err = errors.ErrUnsupported
		}
	}
	if err != nil {
		t, err = bloblang.ValueAsTimestamp(val)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to coerse TIME value from %v", val)
	}
	// 24 hours in nanoseconds fits within uint64, so we can't overflow
	nanos := t.Hour()*int(time.Hour.Nanoseconds()) +
		t.Minute()*int(time.Minute.Nanoseconds()) +
		t.Second()*int(time.Second.Nanoseconds()) +
		t.Nanosecond()
	return int64(nanos) / pow10TableInt64[9-c.scale], nil
}

type dateConverter struct {
	nullable bool
}

// TODO(perf): have some way of sorting these by when they are used
// as the format is likely the same for a given pipeline
var dateFormats = []string{
	"2006-01-02",
	"2006-1-02",
	"2006-01-2",
	"2006-1-2",
	"01-02-2006",
	"01-2-2006",
	"1-02-2006",
	"1-2-2006",
	"2006/01/02",
	"2006/01/2",
	"2006/1/02",
	"2006/1/2",
	"01/02/2006",
	"1/02/2006",
	"01/2/2006",
	"1/2/2006",
}

func (c dateConverter) ValidateAndConvert(buf *statsBuffer, val any) (any, error) {
	if val == nil {
		if !c.nullable {
			return nil, errNullValue
		}
		buf.nullCount++
		return val, nil
	}
	var s string
	switch v := val.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	}
	var t time.Time
	var err error
	if s != "" {
		for _, format := range dateFormats {
			t, err = time.Parse(format, s)
			if err == nil {
				break
			}
		}
	} else {
		err = errors.ErrUnsupported
	}
	if err != nil {
		t, err = bloblang.ValueAsTimestamp(val)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to coerse DATE value from %v", val)
	}
	t = t.UTC()
	if t.Year() < -9999 || t.Year() > 9999 {
		return nil, fmt.Errorf("DATE columns out of range, year: %d", t.Year())
	}
	return t.Unix() / int64(24*60*60), nil
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
			MinIntValue:    int128.Int64(stat.minIntVal),
			MaxIntValue:    int128.Int64(stat.maxIntVal),
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

func byteWidth(v int64) int {
	if v < 0 {
		switch {
		case v >= math.MinInt8:
			return 1
		case v >= math.MinInt16:
			return 2
		case v >= math.MinInt32:
			return 4
		}
		return 8
	}
	switch {
	case v <= math.MaxInt8:
		return 1
	case v <= math.MaxInt16:
		return 2
	case v <= math.MaxInt32:
		return 4
	}
	return 8
}

func packInteger(v int64) any {
	if v < 0 {
		switch {
		case v >= math.MinInt8:
			return int8(v)
		case v >= math.MinInt16:
			return int16(v)
		case v >= math.MinInt32:
			return int32(v)
		}
		return v
	}
	switch {
	case v <= math.MaxInt8:
		return int8(v)
	case v <= math.MaxInt16:
		return int16(v)
	case v <= math.MaxInt32:
		return int32(v)
	}
	return v
}
