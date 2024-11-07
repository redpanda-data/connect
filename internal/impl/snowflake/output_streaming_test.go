/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidColumnTypeRegex(t *testing.T) {
	matches := []string{
		"INT",
		"NUMBER",
		"NUMBER ( 38, 0 )",
		"  NUMBER ( 38, 0 )  ",
		"DOUBLE PRECISION",
		"DOUBLE   PRECISION",
		"  varchar ( 99 )  ",
		"  varchar ( 0 )  ",
	}
	for _, m := range matches {
		m := m
		t.Run(m, func(t *testing.T) {
			require.Regexp(t, validColumnTypeRegex, m)
		})
	}
	nonMatches := []string{
		"VAR",
		"N",
		"VAR(1, 3)",
		"VAR(1)",
		"VARCHAR()",
		"VARCHAR(  )",
		"GARBAGE VARCHAR(2)",
		"VARCHAR(2) GARBAGE",
	}
	for _, m := range nonMatches {
		m := m
		t.Run(m, func(t *testing.T) {
			require.NotRegexp(t, validColumnTypeRegex, m)
		})
	}
}
