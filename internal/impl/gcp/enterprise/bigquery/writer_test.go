// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkRowsByBytes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		got := chunkRowsByBytes(nil, 1024)
		assert.Empty(t, got)
	})

	t.Run("single row fits", func(t *testing.T) {
		rows := [][]byte{[]byte("hello")}
		got := chunkRowsByBytes(rows, 1024)
		require.Len(t, got, 1)
		assert.Equal(t, rows, got[0])
	})

	t.Run("splits at byte threshold", func(t *testing.T) {
		// 5 rows of 100 bytes each, max=250 → 3 chunks of [2, 2, 1].
		rows := make([][]byte, 5)
		for i := range rows {
			rows[i] = bytes.Repeat([]byte{'a'}, 100)
		}
		got := chunkRowsByBytes(rows, 250)
		require.Len(t, got, 3)
		assert.Len(t, got[0], 2)
		assert.Len(t, got[1], 2)
		assert.Len(t, got[2], 1)
	})

	t.Run("oversized single row still emits", func(t *testing.T) {
		// One oversized row is emitted as its own chunk; the API will reject
		// it server-side, which is the expected error path.
		rows := [][]byte{bytes.Repeat([]byte{'b'}, 1024)}
		got := chunkRowsByBytes(rows, 100)
		require.Len(t, got, 1)
		assert.Len(t, got[0], 1)
	})

	t.Run("all rows fit in one chunk", func(t *testing.T) {
		rows := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		got := chunkRowsByBytes(rows, 100)
		require.Len(t, got, 1)
		assert.Equal(t, rows, got[0])
	})
}
