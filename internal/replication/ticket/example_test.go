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
	"errors"
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
			// The ticket holds a batch, so it comes from Take: an abandon
			// must seal the lock.
			batcherMu.Lock()
			batch := nextBatch
			nextBatch++
			t := queue.Take()
			batcherMu.Unlock()

			// Step 2: release the ticket when we are done, also on error
			// paths.
			defer t.Release()

			// Step 3: wait for our turn.
			if err := t.Wait(context.Background()); err != nil {
				return
			}

			// Step 4: the slow work, for example track and send. The
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

// This example shows an abandoned ticket. The Wait of t1 is cancelled, so
// the lock skips t1 and gives the turn to t2. t1 is from TakeSkippable, so
// the abandon does not seal the lock.
func ExampleTicket_Wait_abandon() {
	var queue ticket.Lock
	t0, t1, t2 := queue.Take(), queue.TakeSkippable(), queue.Take()

	// t0 gets the turn at once.
	fmt.Println("t0:", t0.Wait(context.Background()))

	// The caller of t1 gives up, for example on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fmt.Println("t1:", t1.Wait(ctx))

	// t0 releases. The turn skips t1 and goes to t2.
	t0.Release()
	fmt.Println("t2:", t2.Wait(context.Background()))
	t2.Release()

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

	fmt.Println("t0:", t0.Wait(context.Background()))
	queue.Seal(errors.New("track failed"))
	t0.Release() // The holder still releases its turn.

	err := t1.Wait(context.Background())
	fmt.Println("t1:", err)
	fmt.Println("sealed:", errors.Is(err, ticket.ErrSealed))

	// Output:
	// t0: <nil>
	// t1: ticket lock sealed: track failed
	// sealed: true
}
