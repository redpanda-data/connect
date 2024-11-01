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

func mergeStats(a, b *statsBuffer) *statsBuffer {
	c := &statsBuffer{hasData: true}
	switch {
	case a.hasData && b.hasData:
		c.minIntVal = int128.Min(a.minIntVal, b.minIntVal)
		c.maxIntVal = int128.Max(a.maxIntVal, b.maxIntVal)
		c.minRealVal = min(a.minRealVal, b.minRealVal)
		c.maxRealVal = max(a.maxRealVal, b.maxRealVal)
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
			ColumnOrdinal:  int32(transformer.column.Ordinal),
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
