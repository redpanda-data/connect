// Copyright 2024 Redpanda Data, Inc.
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

package typed

import "sync/atomic"

// A small type safe generic wrapper over atomic.Value
//
// Who doesn't like generics?
type AtomicValue[T any] struct {
	val *atomic.Value
}

func NewAtomicValue[T any](v T) *AtomicValue[T] {
	a := &AtomicValue[T]{}
	a.val.Store(v)
	return a
}

func (a *AtomicValue[T]) Load() T {
	// This dereference is safe because we only create these with values
	return *a.val.Load().(*T)
}

func (a *AtomicValue[T]) Store(v T) {
	a.val.Store(&v)
}