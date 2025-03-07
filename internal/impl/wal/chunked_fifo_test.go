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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPushPop(t *testing.T) {
	for count := range []int{1, 3, 127, 128, 129, 255, 256, 257} {
		cb := newChunkedBuffer[int]()
		for i := range count {
			cb.PushBack(i)
			require.False(t, cb.Empty())
		}
		for i := range count {
			require.False(t, cb.Empty())
			e, ok := cb.PopFront()
			require.True(t, ok)
			require.Equal(t, i, e)
		}
		_, ok := cb.PopFront()
		require.False(t, ok)
		require.True(t, cb.Empty(), "%+v", cb)
	}
}

func TestPushPopPushPop(t *testing.T) {
	for count := range []int{4, 100, 128, 200, 256, 300} {
		cb := newChunkedBuffer[int]()
		for i := range count / 2 {
			cb.PushBack(i)
			require.False(t, cb.Empty())
		}
		for i := range count / 2 {
			require.False(t, cb.Empty())
			e, ok := cb.PopFront()
			require.True(t, ok)
			require.Equal(t, i, e)
		}
		for i := range count / 2 {
			cb.PushBack(i)
			require.False(t, cb.Empty())
		}
		for i := range count / 2 {
			require.False(t, cb.Empty())
			e, ok := cb.PopFront()
			require.True(t, ok)
			require.Equal(t, i, e)
		}
		_, ok := cb.PopFront()
		require.False(t, ok)
		require.True(t, cb.Empty(), "%+v", cb)
	}
}
