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
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
	"github.com/stretchr/testify/require"
)

func TestMergeInt(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minIntVal: int128.FromInt64(-1),
		maxIntVal: int128.FromInt64(4),
		hasData:   true,
	}, &statsBuffer{
		minIntVal: int128.FromInt64(3),
		maxIntVal: int128.FromInt64(5),
		hasData:   true,
	})
	require.Equal(t, &statsBuffer{
		minIntVal: int128.FromInt64(-1),
		maxIntVal: int128.FromInt64(5),
		hasData:   true,
	}, s)
}

func TestMergeReal(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minRealVal: -1.2,
		maxRealVal: 4.5,
		nullCount:  4,
		hasData:    true,
	}, &statsBuffer{
		minRealVal: 3.4,
		maxRealVal: 5.9,
		nullCount:  2,
		hasData:    true,
	})
	require.Equal(t, &statsBuffer{
		minRealVal: -1.2,
		maxRealVal: 5.9,
		nullCount:  6,
		hasData:    true,
	}, s)
}

func TestMergeStr(t *testing.T) {
	s := mergeStats(&statsBuffer{
		minStrVal: []byte("aa"),
		maxStrVal: []byte("bbbb"),
		maxStrLen: 6,
		nullCount: 1,
		hasData:   true,
	}, &statsBuffer{
		minStrVal: []byte("aaaa"),
		maxStrVal: []byte("cccccc"),
		maxStrLen: 24,
		nullCount: 1,
		hasData:   true,
	})
	require.Equal(t, &statsBuffer{
		minStrVal: []byte("aa"),
		maxStrVal: []byte("cccccc"),
		maxStrLen: 24,
		nullCount: 2,
		hasData:   true,
	}, s)
}
