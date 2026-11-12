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
	"github.com/stretchr/testify/require"
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

func TestTableQueuePushAgainAfterPop(t *testing.T) {
	q := NewTableQueue()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	q.Push("a")
	tbl, ok := q.Pop(ctx)
	require.True(t, ok)
	require.Equal(t, "a", tbl)

	q.Push("a")
	assert.Equal(t, 1, q.Len())
	tbl, ok = q.Pop(ctx)
	require.True(t, ok)
	assert.Equal(t, "a", tbl)
}

type popResult struct {
	table string
	ok    bool
}

func popAsync(ctx context.Context, q *TableQueue) <-chan popResult {
	out := make(chan popResult, 1)
	go func() {
		tbl, ok := q.Pop(ctx)
		out <- popResult{tbl, ok}
	}()
	return out
}

func TestTableQueuePopBlocksUntilPush(t *testing.T) {
	q := NewTableQueue()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res := popAsync(ctx, q)
	select {
	case r := <-res:
		t.Fatalf("Pop returned %+v before any Push", r)
	case <-time.After(50 * time.Millisecond):
	}

	q.Push("late")
	select {
	case r := <-res:
		assert.True(t, r.ok)
		assert.Equal(t, "late", r.table)
	case <-ctx.Done():
		t.Fatal("Pop did not return after Push")
	}
}

func TestTableQueuePopReturnsOnCancelWhileBlocked(t *testing.T) {
	q := NewTableQueue()
	ctx, cancel := context.WithCancel(t.Context())
	res := popAsync(ctx, q)
	select {
	case r := <-res:
		t.Fatalf("Pop returned %+v before cancel", r)
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case r := <-res:
		assert.False(t, r.ok)
	case <-time.After(5 * time.Second):
		t.Fatal("Pop did not return after cancel")
	}
}
