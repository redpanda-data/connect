/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package watermark

import (
	"cmp"
	"sync"
)

// Value is a utility that allows you to store the highest value and subscribe to when
// a specific offset is reached
type (
	Value[T cmp.Ordered] struct {
		val     T
		mu      sync.Mutex
		waiters map[chan<- any]T
	}
)

// New makes a new Value holding `initial`
func New[T cmp.Ordered](initial T) *Value[T] {
	w := &Value[T]{val: initial}
	w.waiters = map[chan<- any]T{}
	return w
}

// Set the watermark value if it's newer
func (w *Value[T]) Set(v T) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if v <= w.val {
		return
	}
	w.val = v
	for notify, val := range w.waiters {
		if val <= w.val {
			notify <- nil
			delete(w.waiters, notify)
		}
	}
}

// Get the current watermark value
func (w *Value[T]) Get() T {
	w.mu.Lock()
	cpy := w.val
	w.mu.Unlock()
	return cpy
}

// WaitFor returns a channel that recieves a value when the watermark reaches `val`.
func (w *Value[T]) WaitFor(val T) <-chan any {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := make(chan any, 1)
	if w.val >= val {
		ch <- nil
		return ch
	}
	w.waiters[ch] = val
	return ch
}
