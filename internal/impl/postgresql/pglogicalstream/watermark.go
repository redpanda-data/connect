/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package pglogicalstream

import (
	"cmp"
	"sync"
)

// watermark is a utility that allows you to store the highest value and subscribe to when
// a specific offset is reached
type watermark[T cmp.Ordered] struct {
	val  T
	mu   sync.Mutex
	cond sync.Cond
}

// create a new watermark at the initial value
func newWatermark[T cmp.Ordered](initial T) *watermark[T] {
	w := &watermark[T]{val: initial}
	w.cond = *sync.NewCond(&w.mu)
	return w
}

// Set the watermark value if it's newer
func (w *watermark[T]) Set(v T) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if v <= w.val {
		return
	}
	w.val = v
	w.cond.Broadcast()
}

// Get the current watermark value
func (w *watermark[T]) Get() T {
	w.mu.Lock()
	cpy := w.val
	w.mu.Unlock()
	return cpy
}

// WaitFor waits until the watermark satifies some predicate.
func (w *watermark[T]) WaitFor(pred func(T) bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for !pred(w.val) {
		w.cond.Wait()
	}
}
