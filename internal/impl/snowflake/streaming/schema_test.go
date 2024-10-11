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

	"github.com/stretchr/testify/require"
)

type validateTestCase struct {
	input  any
	output any
	err    bool
	scale  int
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
			c := timeConverter{nullable: true, scale: tc.scale}
			s := statsBuffer{}
			actual, err := c.ValidateAndConvert(&s, tc.input)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.output, actual)
			}
		})
	}
}
