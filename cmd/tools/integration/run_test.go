// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestForEachParallelSerialKeepsOrder(t *testing.T) {
	var order []int
	forEachParallel(4, 1, func(i int) {
		order = append(order, i)
	})
	assert.Equal(t, []int{0, 1, 2, 3}, order)
}

func TestForEachParallelRunsUpToNAtOnce(t *testing.T) {
	started := make(chan int, 4)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		forEachParallel(4, 2, func(i int) {
			started <- i
			<-release
		})
		close(done)
	}()

	// The first two items start at the same time, in order of their index.
	first, second := <-started, <-started
	assert.ElementsMatch(t, []int{0, 1}, []int{first, second})
	select {
	case i := <-started:
		t.Fatalf("item %d started while two items run", i)
	default:
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("forEachParallel did not return")
	}
	assert.Len(t, started, 2)
}
