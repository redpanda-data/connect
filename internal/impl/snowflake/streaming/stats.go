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
	"math"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

type statsBuffer struct {
	minIntVal, maxIntVal   int128.Num
	minRealVal, maxRealVal float64
	minStrVal, maxStrVal   []byte
	maxStrLen              int
	nullCount              int64
	hasData                bool
}

func (s *statsBuffer) UpdateIntStats(v int128.Num) {
	if !s.hasData {
		s.minIntVal = v
		s.maxIntVal = v
		s.hasData = true
	} else {
		s.minIntVal = int128.Min(s.minIntVal, v)
		s.maxIntVal = int128.Max(s.maxIntVal, v)
	}
}

func (s *statsBuffer) UpdateFloat64Stats(v float64) {
	if !s.hasData {
		s.minRealVal = v
		s.maxRealVal = v
		s.hasData = true
	} else {
		if compareDouble(v, s.minRealVal) < 0 {
			s.minRealVal = v
		}
		if compareDouble(v, s.maxRealVal) > 0 {
			s.maxRealVal = v
		}
	}
}

func (s *statsBuffer) UpdateBytesStats(v []byte) {
	if !s.hasData {
		s.minStrVal = v
		s.maxStrVal = v
		s.maxStrLen = len(v)
		s.hasData = true
	} else {
		if bytes.Compare(v, s.minStrVal) < 0 {
			s.minStrVal = v
		}
		if bytes.Compare(v, s.maxStrVal) > 0 {
			s.maxStrVal = v
		}
		s.maxStrLen = max(s.maxStrLen, len(v))
	}
}

func mergeStats(a, b *statsBuffer) *statsBuffer {
	c := &statsBuffer{hasData: true}
	switch {
	case a.hasData && b.hasData:
		c.minIntVal = int128.Min(a.minIntVal, b.minIntVal)
		c.maxIntVal = int128.Max(a.maxIntVal, b.maxIntVal)
		c.minRealVal = a.minRealVal
		if compareDouble(b.minRealVal, c.minRealVal) < 0 {
			c.minRealVal = b.minRealVal
		}
		c.maxRealVal = a.maxRealVal
		if compareDouble(b.maxRealVal, c.maxRealVal) > 0 {
			c.maxRealVal = b.maxRealVal
		}
		c.maxStrLen = max(a.maxStrLen, b.maxStrLen)
		c.minStrVal = a.minStrVal
		if bytes.Compare(b.minStrVal, a.minStrVal) < 0 {
			c.minStrVal = b.minStrVal
		}
		c.maxStrVal = a.maxStrVal
		if bytes.Compare(b.maxStrVal, a.maxStrVal) > 0 {
			c.maxStrVal = b.maxStrVal
		}
	case a.hasData:
		*c = *a
	case b.hasData:
		*c = *b
	default:
		c.hasData = false
	}
	c.nullCount = a.nullCount + b.nullCount
	return c
}

func computeColumnEpInfo(transformers []*dataTransformer, stats []*statsBuffer) map[string]fileColumnProperties {
	info := map[string]fileColumnProperties{}
	for idx, transformer := range transformers {
		stat := stats[idx]
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
		info[transformer.column.Name] = fileColumnProperties{
			ColumnOrdinal:  transformer.column.Ordinal,
			NullCount:      stat.nullCount,
			MinStrValue:    minStrVal,
			MaxStrValue:    maxStrVal,
			MaxLength:      int64(stat.maxStrLen),
			MinIntValue:    stat.minIntVal,
			MaxIntValue:    stat.maxIntVal,
			MinRealValue:   asJSONNumber(stat.minRealVal),
			MaxRealValue:   asJSONNumber(stat.maxRealVal),
			DistinctValues: -1,
		}
	}
	return info
}

func asJSONNumber(f float64) json.RawMessage {
	if math.IsNaN(f) {
		return json.RawMessage(`"NaN"`)
	}
	if math.IsInf(f, -1) {
		return json.RawMessage(`"-Infinity"`)
	}
	if math.IsInf(f, 1) {
		return json.RawMessage(`"Infinity"`)
	}
	b, _ := json.Marshal(f) // this cannot fail, we handle the cases above
	return json.RawMessage(b)
}

// with similar semantics to Java's Double.compare
func compareDouble(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	aBits := rawDoubleBits(a)
	bBits := rawDoubleBits(b)
	if aBits == bBits {
		return 0
	}
	if aBits < bBits {
		// (-0, 0) or (!NaN, NaN)
		return -1
	}
	// (0, -0) or (NaN, !NaN)
	return 1
}

// rawDoubleBits to Double.doubleToLongBits in Java.
func rawDoubleBits(a float64) int64 {
	if math.IsNaN(a) {
		a = math.NaN() // Use a canonical NaN (yes there are many different kinds)
	}
	return int64(math.Float64bits(a))
}
