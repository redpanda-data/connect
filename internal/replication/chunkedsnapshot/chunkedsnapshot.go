// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package chunkedsnapshot splits snapshot table scans into primary key ranges and runs
// them on a shared worker pool. Planning the bounds and reading each range stay with the
// connector, since both are database-specific.
package chunkedsnapshot

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Range is a (Min, Max] primary key range, where a zero-value bound is unbounded.
type Range[K any] struct {
	Min K
	Max K
}

// RangesFromBounds turns sorted, distinct bounds into contiguous ranges covering the whole keyspace.
func RangesFromBounds[K any](bounds []K) []Range[K] {
	ranges := make([]Range[K], 0, len(bounds)+1)
	var prev K
	for _, bound := range bounds {
		ranges = append(ranges, Range[K]{Min: prev, Max: bound})
		prev = bound
	}
	return append(ranges, Range[K]{Min: prev})
}

// BoundsFromSamples returns every step-th of the sorted key samples as range bounds.
func BoundsFromSamples[K any](samples []K, step int) []K {
	step = max(1, step)
	var bounds []K
	for i := step; i < len(samples); i += step {
		bounds = append(bounds, samples[i])
	}
	return bounds
}

// Task reads one table or range of a table.
type Task func(context.Context) error

// Run runs every table's tasks on up to workers goroutines, starting tables with a single
// task first so their serial scans overlap the chunks of split tables.
func Run(ctx context.Context, workers int, tables [][]Task) error {
	var serial, chunks []Task
	for _, tasks := range tables {
		if len(tasks) == 1 {
			serial = append(serial, tasks...)
		} else {
			chunks = append(chunks, tasks...)
		}
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(workers)
	for _, task := range append(serial, chunks...) {
		wg.Go(func() error { return task(ctx) })
	}
	return wg.Wait()
}

// Completion calls onComplete with a table's total rows once all of its tasks succeed.
type Completion struct {
	remaining  atomic.Int32
	rows       atomic.Int64
	onComplete func(rows int64)
}

// NewCompletion returns a Completion for a table split into tasks tasks.
func NewCompletion(tasks int, onComplete func(rows int64)) *Completion {
	c := &Completion{onComplete: onComplete}
	c.remaining.Store(int32(tasks))
	return c
}

// Done records a successful task's rows, calling onComplete after the table's last task.
func (c *Completion) Done(rows int) {
	c.rows.Add(int64(rows))
	if c.remaining.Add(-1) == 0 {
		c.onComplete(c.rows.Load())
	}
}
