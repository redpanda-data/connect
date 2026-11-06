// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTableQueueFIFOAndDedup(t *testing.T) {
	q := NewTableQueue()
	q.Push("a")
	q.Push("b")
	q.Push("a")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := []string{}
	for range 2 {
		tbl, ok := q.Pop(ctx)
		assert.True(t, ok)
		got = append(got, tbl)
	}
	assert.Equal(t, []string{"a", "b"}, got)
	assert.Equal(t, 0, q.Len())
}

func TestTableQueuePopBlocksUntilPushOrCancel(t *testing.T) {
	q := NewTableQueue()
	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(20 * time.Millisecond); q.Push("late") }()
	tbl, ok := q.Pop(ctx)
	assert.True(t, ok)
	assert.Equal(t, "late", tbl)
	cancel()
	_, ok = q.Pop(ctx)
	assert.False(t, ok)
}
