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
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/Jeffail/gabs/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

type typedBufferFactory func() typedBuffer

// typedBuffer is the buffer that holds columnar data before we write to the parquet file
type typedBuffer interface {
	WriteNull()
	WriteInt128(int128.Num)
	WriteBool(bool)
	WriteFloat64(float64)
	WriteBytes([]byte) // should never be nil

	// Prepare for writing values to the following matrix.
	// Must be called before writing
	// The matrix size must be pre-allocated to be the size of
	// the data that will be written - this buffer will not modify
	// the size of the data.
	Prepare(matrix []parquet.Value, columnIndex, rowWidth int)
}

type typedBufferImpl struct {
	matrix      []parquet.Value
	columnIndex int
	rowWidth    int
	currentRow  int

	// For int128 we don't make a bunch of small allocs,
	// but append to this existing buffer a bunch, this
	// saves GC pressure. We could optimize copies and
	// reallocations, but this is simpler and seems to
	// be effective for now.
	scratch []byte
}

func (b *typedBufferImpl) WriteValue(v parquet.Value) {
	b.matrix[(b.currentRow*b.rowWidth)+b.columnIndex] = v
	b.currentRow++
}
func (b *typedBufferImpl) WriteNull() {
	b.WriteValue(parquet.NullValue())
}
func (b *typedBufferImpl) WriteInt128(v int128.Num) {
	b.scratch = v.AppendBigEndian(b.scratch)
	b.WriteValue(parquet.FixedLenByteArrayValue(b.scratch[len(b.scratch)-16:]).Level(0, 1, b.columnIndex))
}
func (b *typedBufferImpl) WriteBool(v bool) {
	b.WriteValue(parquet.BooleanValue(v).Level(0, 1, b.columnIndex))
}
func (b *typedBufferImpl) WriteFloat64(v float64) {
	b.WriteValue(parquet.DoubleValue(v).Level(0, 1, b.columnIndex))
}
func (b *typedBufferImpl) WriteBytes(v []byte) {
	b.WriteValue(parquet.ByteArrayValue(v).Level(0, 1, b.columnIndex))
}
func (b *typedBufferImpl) Prepare(matrix []parquet.Value, columnIndex, rowWidth int) {
	b.currentRow = 0
	b.matrix = matrix
	b.columnIndex = columnIndex
	b.rowWidth = rowWidth
	if b.scratch != nil {
		b.scratch = b.scratch[:0]
	}
}

var defaultTypedBufferFactory = typedBufferFactory(func() typedBuffer { return &typedBufferImpl{} })

type int64Buffer struct {
	typedBufferImpl
}

func (b *int64Buffer) WriteInt128(v int128.Num) {
	b.WriteValue(parquet.Int64Value(v.ToInt64()).Level(0, 1, b.columnIndex))
}

var int64TypedBufferFactory = typedBufferFactory(func() typedBuffer { return &int64Buffer{} })

type int32Buffer struct {
	typedBufferImpl
}

func (b *int32Buffer) WriteInt128(v int128.Num) {
	b.WriteValue(parquet.Int32Value(int32(v.ToInt64())).Level(0, 1, b.columnIndex))
}

type dataConverter interface {
	ValidateAndConvert(stats *statsBuffer, val any, buf typedBuffer) error
}

var int32TypedBufferFactory = typedBufferFactory(func() typedBuffer { return &int32Buffer{} })

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
	i := int128.FromUint64(0)
	if v {
		i = int128.FromUint64(1)
	}
	stats.UpdateIntStats(i)
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
	var v int128.Num
	var err error
	switch t := val.(type) {
	case int:
		v = int128.FromInt64(int64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case int8:
		v = int128.FromInt64(int64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case int16:
		v = int128.FromInt64(int64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case int32:
		v = int128.FromInt64(int64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case int64:
		v = int128.FromInt64(t)
		v, err = int128.Rescale(v, c.precision, c.scale)
	case uint:
		v = int128.FromUint64(uint64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case uint8:
		v = int128.FromUint64(uint64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case uint16:
		v = int128.FromUint64(uint64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case uint32:
		v = int128.FromUint64(uint64(t))
		v, err = int128.Rescale(v, c.precision, c.scale)
	case uint64:
		v = int128.FromUint64(t)
		v, err = int128.Rescale(v, c.precision, c.scale)
	case float32:
		v, err = int128.FromFloat32(t, c.precision, c.scale)
	case float64:
		v, err = int128.FromFloat64(t, c.precision, c.scale)
	case string:
		v, err = int128.FromString(t, c.precision, c.scale)
	case json.Number:
		v, err = int128.FromString(t.String(), c.precision, c.scale)
	default:
		// fallback to the good error message that bloblang provides
		var i int64
		i, err = bloblang.ValueAsInt64(val)
		if err != nil {
			return err
		}
		v = int128.FromInt64(i)
		v, err = int128.Rescale(v, c.precision, c.scale)
	}
	if err != nil {
		return err
	}
	stats.UpdateIntStats(v)
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
	stats.UpdateFloat64Stats(v)
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
	var v []byte
	switch t := val.(type) {
	case string:
		if t != "" {
			// We don't modify this byte slice at all, so this is safe to grab the bytes
			// without making a copy.
			// Also make sure this isn't an empty string because it's undefined what the
			// value is.
			v = unsafe.Slice(unsafe.StringData(t), len(t))
		} else {
			v = []byte{}
		}
	case []byte:
		v = t
	default:
		b, err := bloblang.ValueAsBytes(val)
		if err != nil {
			return err
		}
		v = b
	}
	if len(v) > c.maxLength {
		return fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
	}
	if c.utf8 && !utf8.Valid(v) {
		return errors.New("invalid UTF8")
	}
	stats.UpdateBytesStats(v)
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
	v := gabs.Wrap(val).Bytes()
	if len(v) > c.maxLength {
		return fmt.Errorf("value too long, length: %d, max: %d", len(v), c.maxLength)
	}
	stats.UpdateBytesStats(v)
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
		if err != nil {
			return err
		}
	}
	if s != "" {
		location := c.defaultTZ
		t, err = time.ParseInLocation(time.RFC3339Nano, s, location)
		if err != nil {
			return &InvalidTimestampFormatError{"timestamp", s}
		}
	}
	if c.trimTZ {
		t = t.UTC()
	}
	y := t.Year()
	if y < 1 || y > 9999 {
		return fmt.Errorf(
			"timestamp out of representable inclusive range of years between 1 and 9999: %d",
			y,
		)
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
	stats.UpdateIntStats(v)
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
	t, err := bloblang.ValueAsTimestamp(val)
	if err != nil {
		if s, ok := val.(string); ok {
			return &InvalidTimestampFormatError{"time", s}
		}
		return err
	}
	t = t.In(time.UTC)
	// 24 hours in nanoseconds fits within uint64, so we can't overflow
	nanos := t.Hour()*int(time.Hour.Nanoseconds()) +
		t.Minute()*int(time.Minute.Nanoseconds()) +
		t.Second()*int(time.Second.Nanoseconds()) +
		t.Nanosecond()
	v := int128.FromInt64(int64(nanos) / pow10TableInt64[9-c.scale])
	stats.UpdateIntStats(v)
	buf.WriteInt128(v)
	return nil
}

type dateConverter struct {
	nullable bool
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
	t, err := bloblang.ValueAsTimestamp(val)
	if err != nil {
		if s, ok := val.(string); ok {
			return &InvalidTimestampFormatError{"date", s}
		}
		return err
	}
	t = t.UTC()
	if t.Year() < -9999 || t.Year() > 9999 {
		return fmt.Errorf("DATE columns out of range, year: %d", t.Year())
	}
	v := int128.FromInt64(t.Unix() / int64(24*60*60))
	stats.UpdateIntStats(v)
	buf.WriteInt128(v)
	return nil
}
