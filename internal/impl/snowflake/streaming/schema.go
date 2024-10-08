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
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

type dataTransformerFn func(buf *statsBuffer, val any) (any, error)

type dataTransformer struct {
	converter dataTransformerFn
	stats     *statsBuffer
}

// See ParquetTypeGenerator
func constructParquetSchema(columns []columnMetadata) (*parquet.Schema, map[string]*dataTransformer, error) {
	groupNode := parquet.Group{}
	transformers := map[string]*dataTransformer{}
	for _, column := range columns {
		id := int(column.Ordinal)
		var n parquet.Node
		var converter dataTransformerFn
		switch strings.ToLower(column.LogicalType) {
		case "fixed":
			// TODO: It's not this simple :)
			n = parquet.Leaf(parquet.Int64Type)
			converter = incrementIntStat
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
			return nil, nil, fmt.Errorf("unsupported logical column type: %s", column.LogicalType)
		}
		if column.Nullable {
			n = parquet.Optional(n)
		}
		n = parquet.FieldID(n, id)
		// TODO: Use the unquoted name
		groupNode[column.Name] = n
		transformers[column.Name] = &dataTransformer{converter: converter, stats: &statsBuffer{columnId: id}}
	}
	// parquet.PrintSchema(os.Stderr, "bdec", groupNode)
	return parquet.NewSchema("bdec", groupNode), transformers, nil
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
