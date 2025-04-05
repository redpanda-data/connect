// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"container/list"
)

type chunk[T any] struct {
	slice   []T
	current int
}

type chunkedBuffer[T any] struct {
	list *list.List
}

func newChunkedBuffer[T any]() *chunkedBuffer[T] {
	return &chunkedBuffer[T]{list: list.New()}
}

func (cb *chunkedBuffer[T]) PeekFront() (item T, ok bool) {
	e := cb.list.Front()
	if e == nil {
		return item, false
	}
	c := e.Value.(*chunk[T])
	return c.PeekFront()
}

func (cb *chunkedBuffer[T]) PopFront() (item T, ok bool) {
	e := cb.list.Front()
	if e == nil {
		return item, false
	}
	c := e.Value.(*chunk[T])
	item, ok = c.PopFront()
	if c.Empty() && !c.HasCapacity() {
		cb.list.Remove(e)
	}
	return item, ok
}

func (cb *chunkedBuffer[T]) PushBack(item T) {
	e := cb.list.Back()
	if e != nil {
		c := e.Value.(*chunk[T])
		if c.HasCapacity() {
			c.PushBack(item)
			return
		}
	}
	c := &chunk[T]{slice: make([]T, 0, 128), current: 0}
	c.PushBack(item)
	cb.list.PushBack(c)
}

func (cb *chunkedBuffer[T]) Empty() bool {
	switch cb.list.Len() {
	case 0:
		return true
	case 1:
		e := cb.list.Front().Value.(*chunk[T])
		return e.Empty()
	default:
		return false
	}
}

func (c *chunk[T]) PeekFront() (item T, ok bool) {
	if c.current >= len(c.slice) {
		return item, false
	}
	return c.slice[c.current], true
}

func (c *chunk[T]) PopFront() (item T, ok bool) {
	item, ok = c.PeekFront()
	if ok {
		var dft T
		c.slice[c.current] = dft // nil out a pointer type for GC
		c.current++
	}
	return item, ok
}

func (c *chunk[T]) PushBack(item T) {
	c.slice = append(c.slice, item)
}

func (c *chunk[T]) HasCapacity() bool {
	return len(c.slice) < cap(c.slice)
}

func (c *chunk[T]) Empty() bool {
	return c.current == len(c.slice)
}
