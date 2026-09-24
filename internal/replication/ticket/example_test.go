// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package ticket_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/redpanda-data/connect/v4/internal/replication/ticket"
)

// This example shows the typical use. Flushers draw a ticket under the
// batcher lock, and then do the slow work outside of it, in ticket order.
func ExampleLock() {
	var (
		batcherMu sync.Mutex
		nextBatch int // Stands in for the batcher: each flush makes a new batch.
		queue     ticket.Lock
		wg        sync.WaitGroup
	)
	for range 4 {
		wg.Go(func() {
			// Step 1: flush and draw a ticket in the same critical section.
			batcherMu.Lock()
			batch := nextBatch
			nextBatch++
			t := queue.Take()
			batcherMu.Unlock()

			// Step 2: wait for our turn. The ticket owns a batch, so an
			// abandon must seal the lock.
			if err := queue.Acquire(context.Background(), t, true); err != nil {
				return
			}

			// Step 4, deferred: release our turn, also on error paths.
			defer queue.Release()

			// Step 3: the slow work, for example track and send. The
			// goroutines start in any order, but this runs in flush order.
			fmt.Println("tracked batch", batch)
		})
	}
	wg.Wait()

	// Output:
	// tracked batch 0
	// tracked batch 1
	// tracked batch 2
	// tracked batch 3
}

// This example shows an abandoned ticket. The Acquire of t1 is cancelled, so
// Release skips t1 and gives the turn to t2.
func ExampleLock_Acquire_abandon() {
	var queue ticket.Lock
	t0, t1, t2 := queue.Take(), queue.Take(), queue.Take()

	// t0 gets the turn at once.
	fmt.Println("t0:", queue.Acquire(context.Background(), t0, false))

	// The caller of t1 gives up, for example on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fmt.Println("t1:", queue.Acquire(ctx, t1, false))

	// t0 releases. The turn skips t1 and goes to t2.
	queue.Release()
	fmt.Println("t2:", queue.Acquire(context.Background(), t2, false))
	queue.Release()

	// Output:
	// t0: <nil>
	// t1: context canceled
	// t2: <nil>
}

// This example shows a seal. The batch of t0 is lost, for example because
// Track failed, so the holder seals the lock. t1 must not track a batch past
// the lost rows.
func ExampleLock_Seal() {
	var queue ticket.Lock
	t0, t1 := queue.Take(), queue.Take()

	fmt.Println("t0:", queue.Acquire(context.Background(), t0, false))
	queue.Seal()
	queue.Release() // The holder still releases its turn.

	fmt.Println("t1:", queue.Acquire(context.Background(), t1, false))
	fmt.Println("sealed:", queue.Sealed())

	// Output:
	// t0: <nil>
	// t1: ticket lock sealed
	// sealed: true
}
