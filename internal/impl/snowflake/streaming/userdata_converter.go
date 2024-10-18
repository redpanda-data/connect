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
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

// typedBuffer is the buffer that holds columnar data before we write to the parquet file
type typedBuffer interface {
	WriteNull()
	WriteInt128(int128.Int128)
	WriteBool(bool)
	WriteFloat64(float64)
	WriteBytes([]byte) // should never be nil

	// Prepare for writing values to the following matrix.
	// Must be called before writing, and Flush must be
	// called after.
	// The matrix size must be pre-allocated to be the size of
	// the data that will be written - this buffer will not modify
	// the size of the data.
	Prepare(matrix []parquet.Value, columnIndex, rowWidth int)
	// Flush the values using the specific type to matrix
	// most types of buffers don't support changing the type at
	// flush time, except for integer types which are narrowed
	// to use the minimum amount of storage.
	Flush(parquet.Type) error
}

type typedBufferImpl struct {
	matrix      []parquet.Value
	columnIndex int
	rowWidth    int
	currentRow  int
}

func (b *typedBufferImpl) WriteValue(v parquet.Value) {
	b.matrix[(b.currentRow*b.rowWidth)+b.columnIndex] = v
	b.currentRow++
}
func (b *typedBufferImpl) WriteNull() {
	b.WriteValue(parquet.NullValue())
}
func (b *typedBufferImpl) WriteInt128(v int128.Int128) {
	b.WriteValue(parquet.FixedLenByteArrayValue(v.Bytes()))
}
func (b *typedBufferImpl) WriteBool(v bool) {
	b.WriteValue(parquet.BooleanValue(v))
}
func (b *typedBufferImpl) WriteFloat64(v float64) {
	b.WriteValue(parquet.DoubleValue(v))
}
func (b *typedBufferImpl) WriteBytes(v []byte) {
	b.WriteValue(parquet.ByteArrayValue(v))
}
func (b *typedBufferImpl) Prepare(matrix []parquet.Value, columnIndex, rowWidth int) {
	b.currentRow = 0
	b.matrix = matrix
	b.columnIndex = columnIndex
	b.rowWidth = rowWidth
}
func (b *typedBufferImpl) Flush(parquet.Type) error {
	return nil
}

// int128Buffer is special in that it holds values out then
// writes them in flush using a specific type.
// We hold null values as int128.MaxInt128 because that's
// outside the valid range for what is representable in
// Snowflake (which supports up to 38 bits of precision and
// max int128 is greater than that).
type int128Buffer struct {
	matrix      []parquet.Value
	columnIndex int
	rowWidth    int
	ints        []int128.Int128
}

func (b *int128Buffer) WriteInt128(v int128.Int128) {
	b.ints = append(b.ints, v)
}
func (b *int128Buffer) WriteNull() {
	b.ints = append(b.ints, int128.MaxInt128)
}
func (b *int128Buffer) WriteBool(bool) {
	panic("invalid value")
}
func (b *int128Buffer) WriteFloat64(float64) {
	panic("invalid value")
}
func (b *int128Buffer) WriteBytes(v []byte) {
	panic("invalid value")
}
func (b *int128Buffer) Prepare(matrix []parquet.Value, columnIndex, rowWidth int) {
	b.matrix = matrix
	b.columnIndex = columnIndex
	b.rowWidth = rowWidth
	if b.ints != nil {
		b.ints = b.ints[:0]
	}
}
func (b *int128Buffer) Flush(t parquet.Type) error {
	switch t.Kind() {
	case parquet.Int32Type.Kind():
		for i, n := range b.ints {
			var v parquet.Value // zero value is null
			if n != int128.MaxInt128 {
				v = parquet.Int32Value(int32(n.Int64()))
			}
			b.matrix[(i*b.rowWidth)+b.columnIndex] = v
		}
	case parquet.Int64Type.Kind():
		for i, n := range b.ints {
			var v parquet.Value
			if n != int128.MaxInt128 {
				v = parquet.Int64Value(n.Int64())
			}
			b.matrix[(i*b.rowWidth)+b.columnIndex] = v
		}
	case parquet.FixedLenByteArrayType(16).Kind():
		for i, n := range b.ints {
			var v parquet.Value
			if n != int128.MaxInt128 {
				v = parquet.FixedLenByteArrayValue(n.Bytes())
			}
			b.matrix[(i*b.rowWidth)+b.columnIndex] = v
		}
	default:
		return fmt.Errorf("unexpected narrowed integer type: %s", t.String())
	}
	return nil
}

type dataConverter interface {
	ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error
}

var errNullValue = errors.New("unexpected null value")

type boolConverter struct {
	nullable bool
}

func (c boolConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	v, err := bloblang.ValueAsBool(val)
	if err != nil {
		return err
	}
	i := int128.Uint64(0)
	if v {
		i = int128.Uint64(1)
	}
	if stats.first {
		stats.minIntVal = i
		stats.maxIntVal = i
		stats.first = false
	} else {
		stats.minIntVal = int128.Min(stats.minIntVal, i)
		stats.maxIntVal = int128.Max(stats.maxIntVal, i)
	}
	buf.WriteBool(v)
	return nil
}

type numberConverter struct {
	nullable  bool
	scale     int32
	precision int32
}

func (c numberConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	var v int128.Int128
	var err error
	switch t := val.(type) {
	case int:
		v = int128.Int64(int64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case int8:
		v = int128.Int64(int64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case int16:
		v = int128.Int64(int64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case int32:
		v = int128.Int64(int64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case int64:
		v = int128.Int64(int64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case uint:
		v = int128.Uint64(uint64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case uint8:
		v = int128.Uint64(uint64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case uint16:
		v = int128.Uint64(uint64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case uint32:
		v = int128.Uint64(uint64(t))
		v, err = int128.Rescale(v, 0, c.scale)
	case uint64:
		v = int128.Uint64(t)
		v, err = int128.Rescale(v, 0, c.scale)
	case float32:
		v, err = int128.Float32(t, c.precision, c.scale)
	case float64:
		v, err = int128.Float64(t, c.precision, c.scale)
	case json.Number:
		v, err = int128.String(t.String(), c.precision, c.scale)
	default:
		// fallback to the good error message that bloblang provides
		var i int64
		i, err = bloblang.ValueAsInt64(val)
		v = int128.Int64(i)
		v, err = int128.Rescale(v, 0, c.scale)
	}
	if err != nil {
		return err
	}
	if !v.FitsInPrecision(c.precision) {
		return fmt.Errorf(
			"number (%s) does not fit within specified precision: %d",
			v.String(),
			c.precision,
		)
	}
	if stats.first {
		stats.minIntVal = v
		stats.maxIntVal = v
		stats.first = false
	} else {
		stats.minIntVal = int128.Min(stats.minIntVal, v)
		stats.maxIntVal = int128.Max(stats.maxIntVal, v)
	}
	buf.WriteInt128(v)
	return nil
}

type doubleConverter struct {
	nullable bool
}

func (c doubleConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	v, err := bloblang.ValueAsFloat64(val)
	if err != nil {
		return err
	}
	if stats.first {
		stats.minRealVal = v
		stats.maxRealVal = v
		stats.first = false
	} else {
		stats.minRealVal = min(stats.minRealVal, v)
		stats.maxRealVal = max(stats.maxRealVal, v)
	}
	buf.WriteFloat64(v)
	return nil
}

type binaryConverter struct {
	nullable  bool
	maxLength int
	utf8      bool
}

func (c binaryConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	v, err := bloblang.ValueAsBytes(val)
	if err != nil {
		return err
	}
	if len(v) > c.maxLength {
		return fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
	}
	if c.utf8 && !utf8.Valid(v) {
		return errors.New("invalid UTF8")
	}
	if stats.first {
		stats.minStrVal = v
		stats.maxStrVal = v
		stats.maxStrLen = len(v)
		stats.first = false
	} else {
		if bytes.Compare(v, stats.minStrVal) < 0 {
			stats.minStrVal = v
		}
		if bytes.Compare(v, stats.maxStrVal) > 0 {
			stats.maxStrVal = v
		}
		stats.maxStrLen = max(stats.maxStrLen, len(v))
	}
	buf.WriteBytes(v)
	return nil
}

type jsonConverter struct {
	nullable  bool
	maxLength int
}

func (c jsonConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	v := []byte(bloblang.ValueToString(val))
	if len(v) > c.maxLength {
		return fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
	}
	if stats.first {
		stats.minStrVal = v
		stats.maxStrVal = v
		stats.maxStrLen = len(v)
		stats.first = false
	} else {
		if bytes.Compare(v, stats.minStrVal) < 0 {
			stats.minStrVal = v
		}
		if bytes.Compare(v, stats.maxStrVal) > 0 {
			stats.maxStrVal = v
		}
		stats.maxStrLen = max(stats.maxStrLen, len(v))
	}
	buf.WriteBytes(v)
	return nil
}

type jsonArrayConverter struct {
	jsonConverter
}

func (c jsonArrayConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val != nil {
		if _, ok := val.([]any); !ok {
			return errors.New("not a JSON array")
		}
	}
	return c.jsonConverter.ValidateAndConvert(stats, val, buf)
}

type jsonObjectConverter struct {
	jsonConverter
}

func (c jsonObjectConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val != nil {
		if _, ok := val.(map[string]any); !ok {
			return errors.New("not a JSON object")
		}
	}
	return c.jsonConverter.ValidateAndConvert(stats, val, buf)
}

type timestampConverter struct {
	nullable         bool
	scale, precision int32
	includeTZ        bool
	trimTZ           bool
	defaultTZ        *time.Location
}

var timestampFormats = []string{
	time.DateTime,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.000",
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05.000",
	"2006-01-02T15:04:05.000-0700",
	"2006-01-02T15:04:05.000-07:00",
	"2006-01-02 15:04:05.000-0700",
	"2006-01-02 15:04:05.000-07:00",
	"2006-01-02 15:04:05.000000000-07:00",
	"2006-01-02T15:04:05.000000000-07:00",
}

func (c timestampConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
		return nil
	}
	var s string
	var t time.Time
	var err error
	switch v := val.(type) {
	case []byte:
		s = string(v)
	case string:
		s = v
	default:
		t, err = bloblang.ValueAsTimestamp(val)
	}
	if s != "" {
		location := c.defaultTZ
		if c.trimTZ {
			location = time.UTC
		}
		for _, format := range timestampFormats {
			t, err = time.ParseInLocation(format, s, location)
			if err == nil {
				break
			}
		}
	}
	if err != nil {
		return fmt.Errorf("unable to coerse TIMESTAMP value from %v", val)
	}
	v := snowflakeTimestampInt(t, c.scale, c.includeTZ)
	if !v.FitsInPrecision(c.precision) {
		return fmt.Errorf(
			"unable to fit timestamp (%s -> %s) within required precision: %v",
			t.Format(time.RFC3339Nano),
			v.String(),
			c.precision,
		)
	}
	if stats.first {
		stats.minIntVal = v
		stats.maxIntVal = v
		stats.first = false
	} else {
		stats.minIntVal = int128.Min(stats.minIntVal, v)
		stats.maxIntVal = int128.Max(stats.maxIntVal, v)
	}
	buf.WriteInt128(v)
	return nil
}

type timeConverter struct {
	nullable bool
	scale    int32
}

func (c timeConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
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
	v := int128.Int64(int64(nanos) / pow10TableInt64[9-c.scale])
	if stats.first {
		stats.minIntVal = v
		stats.maxIntVal = v
		stats.first = false
	} else {
		stats.minIntVal = int128.Min(stats.minIntVal, v)
		stats.maxIntVal = int128.Max(stats.maxIntVal, v)
	}
	// TODO(perf): consider switching to int64 buffers so more stuff can fit in cache
	buf.WriteInt128(v)
	return nil
}

type dateConverter struct {
	nullable bool
}

// TODO(perf): have some way of sorting these by when they are used
// as the format is likely the same for a given pipeline
// Or punt to a user having to configure a format
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

func (c dateConverter) ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error {
	if val == nil {
		if !c.nullable {
			return errNullValue
		}
		stats.nullCount++
		buf.WriteNull()
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
	v := int128.Int64(t.Unix() / int64(24*60*60))
	if stats.first {
		stats.minIntVal = v
		stats.maxIntVal = v
		stats.first = false
	} else {
		stats.minIntVal = int128.Min(stats.minIntVal, v)
		stats.maxIntVal = int128.Max(stats.maxIntVal, v)
	}
	// TODO(perf): consider switching to int64 buffers so more stuff can fit in cache
	buf.WriteInt128(v)
	return nil
}
