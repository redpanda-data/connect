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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type validateTestCase struct {
	input     any
	output    any
	err       bool
	scale     int
	precision int
}

func TestTimeConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  "13:02",
			output: int64(46920),
			scale:  0,
		},
		{
			input:  "13:02   ",
			output: int64(46920),
			scale:  0,
		},
		{
			input:  "13:02:06",
			output: int64(46926),
			scale:  0,
		},
		{
			input:  "13:02:06",
			output: int64(469260),
			scale:  1,
		},
		{
			input:  "13:02:06",
			output: int64(46926000000000),
			scale:  9,
		},
		{
			input:  "13:02:06.1234",
			output: int64(46926),
			scale:  0,
		},
		{
			input:  "13:02:06.1234",
			output: int64(469261),
			scale:  1,
		},
		{
			input:  "13:02:06.1234",
			output: int64(46926123400000),
			scale:  9,
		},
		{
			input:  "13:02:06.123456789",
			output: int64(46926),
			scale:  0,
		},
		{
			input:  "13:02:06.123456789",
			output: int64(469261),
			scale:  1,
		},
		{
			input:  "13:02:06.123456789",
			output: int64(46926123456789),
			scale:  9,
		},
		{
			input:  46926,
			output: int64(46926),
			scale:  0,
		},
		{
			input:  1728680106,
			output: int64(75306000000000),
			scale:  9,
		},
		{
			input: "2023-01-19T14:23:55.878137",
			scale: 9,
			err:   true,
		},
		{
			input:  nil,
			output: nil,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &timeConverter{nullable: true, scale: tc.scale}
			runTestcase(t, c, tc)
		})
	}
}

func runTestcase(t *testing.T, dc dataConverter, tc validateTestCase) {
	t.Helper()
	s := statsBuffer{}
	v, err := dc.ValidateAndConvert(&s, tc.input)
	if tc.err {
		require.Errorf(t, err, "instead got: %#v", v)
	} else {
		require.NoError(t, err)
		require.Equal(t, tc.output, v)
	}
}

func TestIntConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     12,
			output:    int64(12),
			precision: 2,
		},
		{
			input:     1234,
			output:    int64(1234),
			precision: 4,
		},
		{
			input:     123456789,
			output:    int64(123456789),
			precision: 9,
		},
		{
			input:     123456789987654321,
			output:    int64(123456789987654321),
			precision: 18,
		},
		{
			// TODO: Support really big ints
			input:     "91234567899876543219876543211234567891",
			err:       true,
			precision: 38,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &intConverter{nullable: true, scale: tc.scale, precision: tc.precision}
			runTestcase(t, c, tc)
		})
	}
}

func TestRealConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  12345.54321,
			output: float64(12345.54321),
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &doubleConverter{nullable: true}
			runTestcase(t, c, tc)
		})
	}
}

func TestFixedConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     12,
			output:    int8(12),
			precision: 2,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &sb16Converter{nullable: true, scale: tc.scale, precision: tc.precision}
			runTestcase(t, c, tc)
		})
	}
}

func TestBoolConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  true,
			output: true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &boolConverter{nullable: true}
			runTestcase(t, c, tc)
		})
	}
}

func TestBinaryConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  []byte("1234abcd"),
			output: []byte("1234abcd"),
		},
		{
			input: []byte(strings.Repeat("a", 57)),
			err:   true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &binaryConverter{nullable: true, maxLength: 56}
			runTestcase(t, c, tc)
		})
	}
}

func TestStringConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  "1234abcd",
			output: []byte("1234abcd"),
		},
		{
			input: strings.Repeat("a", 57),
			err:   true,
		},
		{
			input: "a\xc5z",
			err:   true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &stringConverter{binaryConverter{nullable: true, maxLength: 56}}
			runTestcase(t, c, tc)
		})
	}
}

func TestTimestampNTZConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     "2013-04-28 20:57:00",
			err:       true,
			scale:     0,
			precision: 9,
		},
		{
			input:     "2013-04-28T20:57:01.000",
			output:    1367182621000,
			scale:     9,
			precision: 4,
		},
		{
			input:     "2022-09-18T22:05:07.123456789",
			output:    1663538707123456789,
			scale:     9,
			precision: 4,
		},
		{
			input: "2021-01-01T01:00:00.123+01:00",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &timestampConverter{nullable: true, scale: tc.scale, tz: false}
			runTestcase(t, c, tc)
		})
	}
}

func TestTimestampTZConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     "2013-04-28 20:57:00",
			err:       true,
			scale:     0,
			precision: 9,
		},
		{
			input:     "2013-04-28T20:57:01.000",
			output:    1367182621000,
			scale:     3,
			precision: 18,
		},
		{
			input: "2021-01-01T01:00:00.123",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &timestampConverter{nullable: true, scale: tc.scale, tz: true}
			runTestcase(t, c, tc)
		})
	}
}

func TestDateConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  "1970-1-10",
			output: int8(9),
		},
		{
			input:  1674478926,
			output: int16(19380),
		},
		{
			input:  "1967-06-23",
			output: int16(-923),
		},
		{
			input:  "2020-07-21",
			output: int16(18464),
		},
		{
			input: time.Time{}.AddDate(10_000, 0, 0),
			err:   true,
		},
		{
			input: time.Time{}.AddDate(-10_001, 0, 0),
			err:   true,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			c := &dateConverter{nullable: true}
			runTestcase(t, c, tc)
		})
	}
}

func TestByteWidth(t *testing.T) {
	tests := [][2]int64{
		{0, 1},
		{1, 1},
		{-1, 1},
		{-16, 1},
		{16, 1},
		{math.MaxInt8 - 1, 1},
		{math.MaxInt8, 1},
		{math.MaxInt8 + 1, 2},
		{math.MinInt8 - 1, 2},
		{math.MinInt8, 1},
		{math.MinInt8 + 1, 1},
		{math.MaxInt16 - 1, 2},
		{math.MaxInt16, 2},
		{math.MaxInt16 + 1, 4},
		{math.MinInt16 - 1, 4},
		{math.MinInt16, 2},
		{math.MinInt16 + 1, 2},
		{math.MaxInt32 - 1, 4},
		{math.MaxInt32, 4},
		{math.MaxInt32 + 1, 8},
		{math.MinInt32 - 1, 8},
		{math.MinInt32, 4},
		{math.MinInt32 + 1, 4},
		{math.MaxInt64 - 1, 8},
		{math.MaxInt64, 8},
		// {math.MaxInt64 + 1, 8},
		// {math.MinInt64 - 1, 8},
		{math.MinInt64, 8},
		{math.MinInt64 + 1, 8},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("byteWidth(%d)", tc[0]), func(t *testing.T) {
			require.Equal(t, int(tc[1]), byteWidth(tc[0]))
		})
	}
}
