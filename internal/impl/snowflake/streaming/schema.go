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
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/dustin/go-humanize"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

type parquetSchema = *schema.GroupNode

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
	ValidateAndConvert(buf *statsBuffer, val any) error
	Finish() parquetColumnData
}

type dataTransformer struct {
	name      string // raw name from API
	converter dataConverter
	stats     *statsBuffer
}

func convertFixedType(normalizedName string, repetition parquet.Repetition, column columnMetadata) (schema.Node, dataConverter, error) {
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
		var l schema.LogicalType
		if isDecimal {
			l = schema.NewDecimalLogicalType(int32(precision), int32(scale))
		}
		n, err := schema.NewPrimitiveNodeLogical(
			normalizedName,
			repetition,
			l,
			parquet.Types.FixedLenByteArray,
			16,
			column.Ordinal,
		)
		return n, &sb16Converter{nullable: column.Nullable, scale: scale, precision: precision}, err
	}
	var ptype parquet.Type
	switch strings.ToUpper(column.PhysicalType) {
	case "SB1":
	case "SB2":
	case "SB4":
		ptype = parquet.Types.Int32
	case "SB8":
		ptype = parquet.Types.Int64
	default:
		return nil, nil, fmt.Errorf("unsupported physical column type: %s", column.PhysicalType)
	}
	var l schema.LogicalType
	if isDecimal {
		l = schema.NewDecimalLogicalType(int32(precision), int32(scale))
	}
	n, err := schema.NewPrimitiveNodeLogical(
		normalizedName,
		repetition,
		l,
		ptype,
		16,
		column.Ordinal,
	)
	return n, &sb16Converter{nullable: column.Nullable, scale: scale, precision: precision}, err
}

// maxJSONSize is the size that any kind of semi-structured data can be, which is 16MiB plus a small overhead
const maxJSONSize = 16*humanize.MiByte - 64

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (*schema.GroupNode, map[string]*dataTransformer, map[string]string, error) {
	fields := schema.FieldList{}
	transformers := map[string]*dataTransformer{}
	typeMetadata := map[string]string{"sfVer": "1,1"}
	var err error
	for _, column := range columns {
		name := normalizeColumnName(column.Name)
		repetition := parquet.Repetitions.Required
		if column.Nullable {
			repetition = parquet.Repetitions.Optional
		}
		var n schema.Node
		var err error
		var converter dataConverter
		logicalType := strings.ToLower(column.LogicalType)
		switch logicalType {
		case "fixed":
			n, converter, err = convertFixedType(name, repetition, column)
			if err != nil {
				return nil, nil, nil, err
			}
		case "array":
			typeMetadata[fmt.Sprintf("%d:obj_enc", column.Ordinal)] = "1"
			n, err = schema.NewPrimitiveNodeLogical(
				name,
				repetition,
				schema.StringLogicalType{},
				parquet.Types.ByteArray,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &jsonArrayConverter{jsonConverter{column.Nullable, maxJSONSize}}
		case "object":
			typeMetadata[fmt.Sprintf("%d:obj_enc", column.Ordinal)] = "1"
			n, err = schema.NewPrimitiveNodeLogical(
				name,
				repetition,
				schema.StringLogicalType{},
				parquet.Types.ByteArray,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &jsonObjectConverter{jsonConverter{column.Nullable, maxJSONSize}}
		case "variant":
			typeMetadata[fmt.Sprintf("%d:obj_enc", column.Ordinal)] = "1"
			n, err = schema.NewPrimitiveNodeLogical(
				name,
				repetition,
				schema.StringLogicalType{},
				parquet.Types.ByteArray,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &jsonConverter{column.Nullable, maxJSONSize}
		case "text":
			fallthrough
		case "char":
			fallthrough
		case "any":
			n, err = schema.NewPrimitiveNodeLogical(
				name,
				repetition,
				schema.StringLogicalType{},
				parquet.Types.ByteArray,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			byteLength := 16 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = &stringConverter{binaryConverter{column.Nullable, byteLength}}
		case "binary":
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				parquet.Types.ByteArray,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			// Why binary data defaults to 8MiB instead of the 16MiB for strings... ¯\_(ツ)_/¯
			byteLength := 8 * humanize.MiByte
			if column.ByteLength != nil {
				byteLength = int(*column.ByteLength)
			}
			byteLength = min(byteLength, 16*humanize.MiByte)
			converter = &binaryConverter{column.Nullable, byteLength}
		case "boolean":
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				parquet.Types.Boolean,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &boolConverter{column.Nullable}
		case "real":
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				parquet.Types.Double,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &doubleConverter{column.Nullable}
		case "timestamp_ltz":
			fallthrough
		case "timestamp_ntz":
			fallthrough
		case "timestamp_tz":
			prim := parquet.Types.FixedLenByteArray
			typeLen := int32(16)
			if column.PhysicalType == "SB8" {
				prim = parquet.Types.Int64
				typeLen = -1
			}
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				prim,
				typeLen,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			scale := 0
			if column.Scale != nil {
				scale = int(*column.Scale)
			}
			tz := logicalType != "timestamp_ntz"
			converter = &timestampConverter{column.Nullable, scale, tz}
		case "time":
			prim := parquet.Types.Int32
			if column.PhysicalType == "SB8" {
				prim = parquet.Types.Int64
			}
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				prim,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			scale := 9
			if column.Scale != nil {
				scale = int(*column.Scale)
			}
			converter = &timeConverter{column.Nullable, scale}
		case "date":
			n, err = schema.NewPrimitiveNode(
				name,
				repetition,
				parquet.Types.Int32,
				-1,
				column.Ordinal,
			)
			if err != nil {
				return nil, nil, nil, err
			}
			converter = &dateConverter{column.Nullable}
		default:
			return nil, nil, nil, fmt.Errorf("unsupported logical column type: %s", column.LogicalType)
		}
		typeMetadata[strconv.Itoa(int(column.Ordinal))] = fmt.Sprintf(
			"%d,%d",
			logicalTypeOrdinal(column.LogicalType),
			physicalTypeOrdinal(column.PhysicalType),
		)
		fields = append(fields, n)
		transformers[name] = &dataTransformer{
			name:      column.Name,
			converter: converter,
			stats:     &statsBuffer{columnID: column.Ordinal},
		}
	}
	root, err := schema.NewGroupNode("bdec", parquet.Repetitions.Required, fields, -1)
	return root, transformers, typeMetadata, err
}

type statsBuffer struct {
	columnID                   int32
	minIntVal, maxIntVal       int64
	minInt128Val, maxInt128Val int128.Int128
	minRealVal, maxRealVal     float64
	minStrVal, maxStrVal       []byte
	maxStrLen                  int
	nullCount                  int64
	first                      bool
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

func (c *boolConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
	}
	v, err := bloblang.ValueAsBool(val)
	if err != nil {
		return err
	}
	var i int64 = 0
	if v {
		i = 1
	}
	if buf.first {
		buf.minIntVal = i
		buf.maxIntVal = i
		buf.first = false
		return nil
	}
	buf.minIntVal = min(buf.minIntVal, i)
	buf.maxIntVal = max(buf.maxIntVal, i)
	return nil
}

func (c *boolConverter) Finish() parquetColumnData {
	panic(nil)
}

type intConverter struct {
	nullable         bool
	scale, precision int
}

func (c *intConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
	}
	v, err := bloblang.ValueAsInt64(val)
	if err != nil {
		return err
	}
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
		return nil
	}
	buf.minIntVal = min(buf.minIntVal, v)
	buf.maxIntVal = max(buf.maxIntVal, v)
	return nil
}

func (c *intConverter) Finish() parquetColumnData {
	panic(nil)
}

type sb16Converter struct {
	nullable         bool
	scale, precision int

	data []parquet.FixedLenByteArray
	defs []int16
}

func (c *sb16Converter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		c.defs = append(c.defs, 0)
		return nil
	}
	v, err := bloblang.ValueAsInt64(val)
	if err != nil {
		return err
	}
	b := int128.Int64(v).Bytes()
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
	} else {
		buf.minIntVal = min(buf.minIntVal, v)
		buf.maxIntVal = max(buf.maxIntVal, v)
	}
	c.data = append(c.data, parquet.FixedLenByteArray(b[:]))
	c.defs = append(c.defs, 1)
	return nil
}

func (c *sb16Converter) Finish() parquetColumnData {
	return parquetColumnData{
		values:           c.data,
		definitionLevels: c.defs,
	}
}

type doubleConverter struct {
	nullable bool
}

func (c *doubleConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
	}
	v, err := bloblang.ValueAsFloat64(val)
	if err != nil {
		return err
	}
	if buf.first {
		buf.minRealVal = v
		buf.maxRealVal = v
		buf.first = false
		return nil
	}
	buf.minRealVal = min(buf.minRealVal, v)
	buf.maxRealVal = max(buf.maxRealVal, v)
	return nil
}

func (c *doubleConverter) Finish() parquetColumnData {
	panic(nil)
}

type binaryConverter struct {
	nullable  bool
	maxLength int
}

func (c *binaryConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
	}
	v, err := bloblang.ValueAsBytes(val)
	if err != nil {
		return err
	}
	if len(v) > c.maxLength {
		return fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
	}
	if buf.first {
		buf.minStrVal = v
		buf.maxStrVal = v
		buf.maxStrLen = len(v)
		buf.first = false
		return nil
	}
	if bytes.Compare(v, buf.minStrVal) < 0 {
		buf.minStrVal = v
	}
	if bytes.Compare(v, buf.maxStrVal) > 0 {
		buf.maxStrVal = v
	}
	buf.maxStrLen = max(buf.maxStrLen, len(v))
	return nil
}

func (c *binaryConverter) Finish() parquetColumnData {
	panic(nil)
}

type stringConverter struct {
	binaryConverter
}

func (c *stringConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	err := c.binaryConverter.ValidateAndConvert(buf, val)
	if err != nil {
		return err
	}
	//if !utf8.Valid(v.([]byte)) {
	//return errors.New("invalid UTF8")
	//}
	return err
}

func (c *stringConverter) Finish() parquetColumnData {
	panic(nil)
}

type jsonConverter struct {
	nullable  bool
	maxLength int
}

func (c *jsonConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
	}
	v := []byte(bloblang.ValueToString(val))
	if len(v) > c.maxLength {
		return fmt.Errorf("too large of JSON value, size: %d, max: %d", len(v), c.maxLength)
	}
	if buf.first {
		buf.minStrVal = v
		buf.maxStrVal = v
		buf.maxStrLen = len(v)
		buf.first = false
		return nil
	}
	if bytes.Compare(v, buf.minStrVal) < 0 {
		buf.minStrVal = v
	}
	if bytes.Compare(v, buf.maxStrVal) > 0 {
		buf.maxStrVal = v
	}
	buf.maxStrLen = max(buf.maxStrLen, len(v))
	return nil
}

func (c *jsonConverter) Finish() parquetColumnData {
	panic(nil)
}

type jsonArrayConverter struct {
	jsonConverter
}

func (c *jsonArrayConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val != nil {
		if _, ok := val.([]any); !ok {
			return errors.New("not a JSON array")
		}
	}
	return c.jsonConverter.ValidateAndConvert(buf, val)
}

type jsonObjectConverter struct {
	jsonConverter
}

func (c *jsonObjectConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val != nil {
		if _, ok := val.(map[string]any); !ok {
			return errors.New("not a JSON object")
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

func (c *timestampConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
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
		return err
	}
	v := snowflakeTimestampInt(t, c.scale, c.tz)
	if buf.first {
		buf.minIntVal = v
		buf.maxIntVal = v
		buf.first = false
		return nil
	}
	buf.minIntVal = min(buf.minIntVal, v)
	buf.maxIntVal = max(buf.maxIntVal, v)
	return nil
}

func (c *timestampConverter) Finish() parquetColumnData {
	panic(nil)
}

type timeConverter struct {
	nullable bool
	scale    int
}

func (c *timeConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
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
		return fmt.Errorf("unable to coerse TIME value from %v", val)
	}
	// 24 hours in nanoseconds fits within uint64, so we can't overflow
	nanos := t.Hour()*int(time.Hour.Nanoseconds()) +
		t.Minute()*int(time.Minute.Nanoseconds()) +
		t.Second()*int(time.Second.Nanoseconds()) +
		t.Nanosecond()
	_ = int64(nanos) / pow10TableInt64[9-c.scale]
	return nil
}

func (c *timeConverter) Finish() parquetColumnData {
	panic(nil)
}

type dateConverter struct {
	nullable bool
}

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

func (c *dateConverter) ValidateAndConvert(buf *statsBuffer, val any) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		buf.nullCount++
		return nil
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
		return fmt.Errorf("unable to coerse DATE value from %v", val)
	}
	t = t.UTC()
	if t.Year() < -9999 || t.Year() > 9999 {
		return fmt.Errorf("DATE columns out of range, year: %d", t.Year())
	}
	_ = t.Unix() / int64(24*60*60)
	return nil
}

func (c *dateConverter) Finish() parquetColumnData {
	panic(nil)
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
			MinIntValue:    stat.minInt128Val,
			MaxIntValue:    stat.maxInt128Val,
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
