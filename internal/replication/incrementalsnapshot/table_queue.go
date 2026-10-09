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
	"sync"
)

// TableQueue orders multi-table backfills so they run one table at a time.
// It is FIFO and deduplicates tables already waiting: a table popped can be
// pushed again. It is safe for concurrent use.
type TableQueue struct {
	mu      sync.Mutex
	pending []string
	queued  map[string]struct{}
	signal  chan struct{}
}

// NewTableQueue returns an empty TableQueue.
func NewTableQueue() *TableQueue {
	return &TableQueue{queued: map[string]struct{}{}, signal: make(chan struct{}, 1)}
}

// Push queues table unless it is already waiting. It never blocks.
func (q *TableQueue) Push(table string) {
	q.mu.Lock()
	if _, exists := q.queued[table]; !exists {
		q.queued[table] = struct{}{}
		q.pending = append(q.pending, table)
	}
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// Pop returns the oldest queued table, blocking until one is pushed. It
// reports false once ctx ends.
func (q *TableQueue) Pop(ctx context.Context) (string, bool) {
	for {
		q.mu.Lock()
		if len(q.pending) > 0 {
			t := q.pending[0]
			q.pending = q.pending[1:]
			delete(q.queued, t)
			q.mu.Unlock()
			return t, true
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-q.signal:
		}
	}
}

// Len returns the number of tables waiting.
func (q *TableQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}
