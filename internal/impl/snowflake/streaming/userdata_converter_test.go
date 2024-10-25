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
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/int128"
)

type validateTestCase struct {
	name      string
	input     any
	output    any
	err       bool
	scale     int32
	precision int32
}

func TestTimeConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  "2020-01-01T13:02:00.0Z",
			output: 46920,
			scale:  0,
		},
		{
			input:  "2020-01-01T13:02:06.0Z",
			output: 46926,
			scale:  0,
		},
		{
			input:  "2020-01-01T13:02:06Z",
			output: 469260,
			scale:  1,
		},
		{
			input:  "2020-01-01T13:02:06Z",
			output: 46926000000000,
			scale:  9,
		},
		{
			input:  "2020-01-01T13:02:06.1234Z",
			output: 46926,
			scale:  0,
		},
		{
			input:  "2020-01-01T13:02:06.1234Z",
			output: 469261,
			scale:  1,
		},
		{
			input:  "2020-01-01T13:02:06.1234Z",
			output: 46926123400000,
			scale:  9,
		},
		{
			input:  "2020-01-01T13:02:06.123456789Z",
			output: 46926,
			scale:  0,
		},
		{
			input:  "2020-01-01T13:02:06.123456789Z",
			output: 469261,
			scale:  1,
		},
		{
			input:  "2020-01-01T13:02:06.123456789Z",
			output: 46926123456789,
			scale:  9,
		},
		{
			input:  46926,
			output: 46926,
			scale:  0,
		},
		{
			input:  1728680106,
			output: 75306000000000,
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

func TestNumberConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			name:      "Number(2, 0)",
			input:     12,
			output:    12,
			precision: 2,
		},
		{
			name:      "Number(4, 0)",
			input:     1234,
			output:    1234,
			precision: 4,
		},
		{
			name:      "Number(9, 0)",
			input:     123456789,
			output:    123456789,
			precision: 9,
		},
		{
			name:      "Number(18, 0)",
			input:     123456789987654321,
			output:    123456789987654321,
			precision: 18,
		},
		{
			name:      "Number(38, 0)",
			input:     json.Number("91234567899876543219876543211234567891"),
			output:    int128.MustParse("91234567899876543219876543211234567891"),
			precision: 38,
		},
		{
			name:      "Number(38, 37)",
			input:     json.Number("9.1234567899876543219876543211234567891"),
			output:    int128.MustParse("91234567899876543219876543211234567891"),
			precision: 38,
			scale:     37,
		},
		{
			name:      "Number(38, 28)",
			input:     json.Number("9123456789.9876543219876543211234567891"),
			output:    int128.MustParse("91234567899876543219876543211234567891"),
			precision: 38,
			scale:     28,
		},
		{
			name:      "Number(19, 0) Error",
			input:     json.Number("91234567899876543219876543211234567891"),
			err:       true,
			precision: 19, // too small
		},
		{
			name:      "Number(19, 4)",
			input:     json.Number("123.4321"),
			output:    1234321,
			scale:     4,
			precision: 19,
		},
		{
			name:      "Number(19, 10)",
			input:     json.Number("123.4321"),
			output:    1234321000000,
			scale:     10,
			precision: 19,
		},
		{
			name:      "Number(26, 4)",
			input:     123456789987654321,
			output:    int128.MustParse("1234567899876543210000"),
			scale:     4,
			precision: 26,
		},
		{
			name:      "Number(19, 4) Error",
			input:     123456789987654321,
			err:       true,
			scale:     4,
			precision: 19,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := &numberConverter{
				nullable:  true,
				scale:     tc.scale,
				precision: tc.precision,
			}
			runTestcase(t, c, tc)
		})
	}
}

func TestRealConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  12345.54321,
			output: 12345.54321,
		},
		{
			input:  3.415,
			output: 3.415,
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

func TestBoolConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  true,
			output: true,
		},
		{
			input:  false,
			output: false,
		},
		{
			input:  nil,
			output: nil,
		},
		{
			input:  "false",
			output: false,
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
			c := &binaryConverter{nullable: true, maxLength: 56, utf8: true}
			runTestcase(t, c, tc)
		})
	}
}

func TestTimestampNTZConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     "2013-04-28T20:57:00.0Z",
			output:    1367182620,
			scale:     0,
			precision: 18,
		},
		{
			input:     "2013-04-28T20:57:01.000Z",
			output:    1367182621000,
			scale:     3,
			precision: 18,
		},
		{
			input:     "2013-04-28T20:57:01.000Z",
			output:    1367182621,
			scale:     0,
			precision: 18,
		},
		{
			input:     "2013-04-28T20:57:01.000+01:00",
			output:    1367179021000,
			scale:     3,
			precision: 18,
		},
		{
			input:     "2022-09-18T22:05:07.123456789Z",
			output:    1663538707123456789,
			scale:     9,
			precision: 38,
		},
		{
			input:     "2022-09-18T22:05:07.123456789+01:00",
			output:    1663535107123456789,
			scale:     9,
			precision: 38,
		},
		{
			input:     "2013-04-28T20:57:01.000Z",
			output:    1367182621000,
			scale:     3,
			precision: 18,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			loc, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			c := &timestampConverter{
				nullable:  true,
				scale:     tc.scale,
				precision: tc.precision,
				includeTZ: false,
				trimTZ:    true,
				defaultTZ: loc,
			}
			runTestcase(t, c, tc)
		})
	}
}

func TestTimestampTZConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     "2013-04-28T20:57:01.000Z",
			output:    22399920062465440,
			scale:     3,
			precision: 18,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			loc, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			c := &timestampConverter{
				nullable:  true,
				scale:     tc.scale,
				precision: tc.precision,
				includeTZ: true,
				trimTZ:    false,
				defaultTZ: loc,
			}
			runTestcase(t, c, tc)
		})
	}
}

func TestTimestampLTZConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:     "2013-04-28T20:57:00Z",
			output:    1367182620,
			scale:     0,
			precision: 18,
		},
		{
			input:     "2013-04-28T20:57:00Z",
			output:    136718262000,
			scale:     2,
			precision: 18,
		},
		{
			input:     "2013-04-28T20:57:00Z",
			err:       true,
			scale:     0,
			precision: 9, // Mor precision needed
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run("", func(t *testing.T) {
			loc, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			c := &timestampConverter{
				nullable:  true,
				scale:     tc.scale,
				precision: tc.precision,
				includeTZ: false,
				trimTZ:    false,
				defaultTZ: loc,
			}
			runTestcase(t, c, tc)
		})
	}
}

func TestDateConverter(t *testing.T) {
	tests := []validateTestCase{
		{
			input:  "1970-01-10T00:00:00Z",
			output: 9,
		},
		{
			input:  1674478926,
			output: 19380,
		},
		{
			input:  "1967-06-23T00:00:00Z",
			output: -923,
		},
		{
			input:  "2020-07-21T00:00:00Z",
			output: 18464,
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

type testTypedBuffer struct {
	output any
}

func (b *testTypedBuffer) WriteNull() {
	b.output = nil
}
func (b *testTypedBuffer) WriteInt128(v int128.Num) {
	switch {
	case int128.Less(v, int128.MinInt64):
		b.output = v
	case int128.Greater(v, int128.MaxInt64):
		b.output = v
	default:
		b.output = int(v.ToInt64())
	}
}

func (b *testTypedBuffer) WriteBool(v bool) {
	b.output = v
}
func (b *testTypedBuffer) WriteFloat64(v float64) {
	b.output = v
}
func (b *testTypedBuffer) WriteBytes(v []byte) {
	b.output = v
}
func (b *testTypedBuffer) Prepare([]parquet.Value, int, int) {
	b.output = nil
}
func (b *testTypedBuffer) Reset() {}

func runTestcase(t *testing.T, dc dataConverter, tc validateTestCase) {
	t.Helper()
	s := statsBuffer{}
	b := testTypedBuffer{}
	err := dc.ValidateAndConvert(&s, tc.input, &b)
	if tc.err {
		require.Errorf(t, err, "instead got: %#v", b.output)
	} else {
		require.NoError(t, err)
		require.Equal(t, tc.output, b.output)
	}
}
