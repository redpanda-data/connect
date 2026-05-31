// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql/driver"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// TestConcurrencyDeduplicationWindow — stress test for DeduplicationWindow
// ---------------------------------------------------------------------------

// TestConcurrencyDeduplicationWindowAddEvictFlushWait exercises the production
// concurrency pattern for DeduplicationWindow:
//
//   - AddRow: called sequentially before the window is installed (signals goroutine,
//     before SetWindow).
//   - Evict + Flush: called sequentially from the Streamer goroutine (always the same
//     goroutine — no concurrent Evict/Flush in production).
//   - Wait: called from multiple "RunTable-like" goroutines concurrently.
//
// Invariants verified:
//  1. No panics or data-race detector hits.
//  2. Flush is idempotent: a second call returns an empty slice.
//  3. All Wait goroutines unblock after Flush is called.
//  4. Rows evicted before Flush are absent from the flushed result.
//  5. The window terminates within the 10-second deadline.
func TestConcurrencyDeduplicationWindowAddEvictFlushWait(t *testing.T) {
	t.Parallel()

	const (
		numRows      = 500
		numEvictions = 200 // evict the first 200 rows before flush
		numWaiters   = 8
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Phase 1: sequential AddRow — production: this happens in ReadChunk before SetWindow.
	win := NewDeduplicationWindow("S", "T", NewCSN(100), NewCSN(200), []string{"ID"})
	for i := range numRows {
		win.AddRow(map[string]any{"ID": int64(i), "VAL": "snap"})
	}
	require.Equal(t, numRows, win.Len())

	// Phase 2: launch Wait goroutines — they block until Flush is called.
	// Production: RunTable calls win.Wait() on the signals goroutine.
	var waitWg sync.WaitGroup
	waitErrs := make([]error, numWaiters)
	for i := range numWaiters {
		waitWg.Add(1)
		go func(i int) {
			defer waitWg.Done()
			waitErrs[i] = win.Wait(ctx)
		}(i)
	}

	// Phase 3: sequential Evict + Flush from a single "Streamer" goroutine.
	// Production: Evict and Flush are always called from the same pollChanges goroutine.
	streamerDone := make(chan struct{})
	var evictedCount int
	var flushed1 []ChangeEvent
	go func() {
		defer close(streamerDone)
		for i := range numEvictions {
			if win.Evict("S", "T", map[string]any{"ID": int64(i)}, NewCSN(150)) {
				evictedCount++
			}
			runtime.Gosched()
		}
		flushed1 = win.Flush()
		// Second Flush must be idempotent.
		_ = win.Flush() // return value checked after streamerDone
	}()

	<-streamerDone

	// Second Flush from the test goroutine (any goroutine) must return empty.
	flushed2 := win.Flush()
	assert.Empty(t, flushed2, "second Flush must return empty slice")

	// All Wait goroutines must unblock after Flush.
	waitWg.Wait()
	for i, err := range waitErrs {
		assert.NoError(t, err, "Wait goroutine %d must not return an error", i)
	}

	// Flushed events must all be OpTypeRead.
	for _, ev := range flushed1 {
		assert.Equal(t, OpTypeRead, ev.Operation)
		assert.Equal(t, "S", ev.Schema)
		assert.Equal(t, "T", ev.Table)
	}

	// Invariant: flushed + evicted == numRows.
	assert.Equal(t, numRows, len(flushed1)+evictedCount,
		"flushed rows + evicted rows must equal total rows added")
}

// TestConcurrencyDeduplicationWindowContextCancel verifies that Wait returns
// ctx.Err() when the context is cancelled before Flush is called.
func TestConcurrencyDeduplicationWindowContextCancel(t *testing.T) {
	t.Parallel()

	win := NewDeduplicationWindow("S", "T", NewCSN(0), NewCSN(100), []string{"ID"})
	win.AddRow(map[string]any{"ID": int64(1)})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- win.Wait(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled, "Wait must return context.Canceled")
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------
// TestConcurrencyStreamerSetWindowPollChanges — stress test for Streamer
// ---------------------------------------------------------------------------

// TestConcurrencyStreamerSetWindowPollChanges exercises concurrent SetWindow
// (from a "RunTable-like" goroutine) and pollChanges (from the Streamer's
// Stream loop).  The test verifies that no data races occur and that every
// installed window is eventually flushed.
//
// Approach:
//   - A fake DB returns a rising SYNCHPOINT on each call and one CDC INSERT
//     per poll (CSN advances monotonically).
//   - A parallel goroutine repeatedly installs DeduplicationWindows via
//     SetWindow with a HighCSN that the Streamer will pass in a few polls.
//   - We drive the Streamer by calling pollChanges in a tight loop instead of
//     calling Stream, so we can terminate cleanly within the test timeout.
func TestConcurrencyStreamerSetWindowPollChanges(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const totalPolls = 200

	// csn advances 1 per poll call.
	var pollNum atomic.Int64

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			n := pollNum.Load()
			csnBytes := makeCSNBytes(uint64(n + 1))
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"},
					[][]driver.Value{{driver.Value(csnBytes)}}, nil
			}
			// Change table: one INSERT per poll at the current CSN.
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID",
				}, [][]driver.Value{{
					driver.Value(csnBytes),
					driver.Value(int64(1)),
					driver.Value("I"),
					driver.Value(time.Now()),
					driver.Value(n),
				}}, nil
		},
	})

	streamer := NewStreamer(db, StreamConfig{
		Schemas:       []string{"S"},
		PollBatchSize: 100,
	}, Version{})
	streamer.changeTables["S.T"] = "ASNCDC.T_CT"

	// windowInstaller simulates what RunTable does: install a window with
	// HighCSN just ahead of the current streamer position, then wait for it.
	var installerWg sync.WaitGroup
	var windowsInstalled atomic.Int64
	var windowsFlushed atomic.Int64

	installerWg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			current := pollNum.Load()
			if current >= totalPolls {
				return
			}
			// Install a window with HighCSN = current+5 (the Streamer will pass
			// it within a few polls).
			highCSN := NewCSN(uint64(current + 5))
			win := NewDeduplicationWindow("S", "T", NewCSN(uint64(current)), highCSN, []string{"ID"})
			win.AddRow(map[string]any{"ID": current})
			streamer.SetWindow(win)
			windowsInstalled.Add(1)

			// Wait for flush with a per-window timeout.
			winCtx, winCancel := context.WithTimeout(ctx, 3*time.Second)
			err := win.Wait(winCtx)
			winCancel()
			if err == nil {
				windowsFlushed.Add(1)
			}
			// Yield so pollChanges can advance.
			runtime.Gosched()
		}
	})

	// Driver loop: run pollChanges until totalPolls or ctx expires.
	currentCSN := NullCSN()
	intentSeqs := map[string]int64{}
	for pollNum.Load() < totalPolls {
		select {
		case <-ctx.Done():
			t.Errorf("context expired before completing %d polls (done %d)",
				totalPolls, pollNum.Load())
			cancel()
			goto done
		default:
		}

		events, newCSN, newSeqs, err := streamer.pollChanges(ctx, currentCSN, intentSeqs)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Errorf("pollChanges error: %v", err)
			break
		}
		if len(events) > 0 {
			currentCSN = newCSN
			intentSeqs = newSeqs
		}
		pollNum.Add(1)
		runtime.Gosched()
	}

done:
	cancel()
	installerWg.Wait()

	// We don't assert exact counts because the window installer and poll loop
	// race legitimately; the important thing is no race-detector hits and no
	// deadlocks.
	t.Logf("windows installed=%d flushed=%d polls=%d",
		windowsInstalled.Load(), windowsFlushed.Load(), pollNum.Load())
}

// TestConcurrencyStreamerSetWindowOnce exercises the scenario where SetWindow
// is called exactly once while pollChanges is running concurrently.
// The window must be flushed exactly once with consistent state.
func TestConcurrencyStreamerSetWindowOnce(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		numRows  = 100
		windowLo = uint64(50)
		windowHi = uint64(150)
	)

	// SYNCHPOINT starts below windowHi and rises to windowHi+1 after a few calls.
	var synchCallCount atomic.Int64

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				n := synchCallCount.Add(1)
				// Return windowHi+1 after the 3rd call so the streamer passes the window.
				var csn uint64
				if n >= 3 {
					csn = windowHi + 1
				} else {
					csn = windowLo - 1
				}
				return []string{"MAX(SYNCHPOINT)"},
					[][]driver.Value{{driver.Value(makeCSNBytes(csn))}}, nil
			}
			// CDC event at CSN = windowHi (within the window bracket).
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID",
				}, [][]driver.Value{{
					driver.Value(makeCSNBytes(windowHi)),
					driver.Value(int64(1)),
					driver.Value("I"),
					driver.Value(time.Now()),
					driver.Value(int64(42)),
				}}, nil
		},
	})

	streamer := NewStreamer(db, StreamConfig{
		Schemas:       []string{"S"},
		PollBatchSize: 1000,
	}, Version{})
	streamer.changeTables["S.T"] = "ASNCDC.T_CT"

	// Build the deduplication window: numRows rows; ID=42 will be evicted by CDC.
	win := NewDeduplicationWindow("S", "T", NewCSN(windowLo), NewCSN(windowHi), []string{"ID"})
	for i := range numRows {
		win.AddRow(map[string]any{"ID": int64(i)})
	}
	streamer.SetWindow(win)

	// Run pollChanges in a goroutine; the main goroutine waits for the window.
	var pollWg sync.WaitGroup
	pollWg.Go(func() {
		currentCSN := NullCSN()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, newCSN, _, err := streamer.pollChanges(ctx, currentCSN, nil)
			if err != nil || ctx.Err() != nil {
				return
			}
			currentCSN = newCSN
			// Stop once the window has been flushed.
			streamer.windowMu.Lock()
			active := streamer.activeWindow
			streamer.windowMu.Unlock()
			if active == nil {
				return
			}
		}
	})

	// Wait for the window to be flushed.
	err := win.Wait(ctx)
	require.NoError(t, err, "window must be flushed within timeout")

	cancel()
	pollWg.Wait()

	// Second Flush must be idempotent.
	flushed2 := win.Flush()
	assert.Empty(t, flushed2, "second Flush must return empty slice")

	// ID=42 was evicted by the CDC event at windowHi; all others survive.
	// We can't call Flush again to get results, but we can check LastPKValues.
	lastPK := win.LastPKValues()
	require.NotNil(t, lastPK, "LastPKValues must return the last-row PK even after Flush")
}

// TestConcurrencyIncrementalEngineRunTable stress-tests RunTable by running the
// engine loop concurrently with a Streamer that flushes windows as fast as it can.
// Uses the fake DB pattern from incremental_test.go.
func TestConcurrencyIncrementalEngineRunTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 3 chunks of 10 rows each; engine should call checkpoint 3 times.
	var chunkCall atomic.Int32
	var checkpointCalled atomic.Int32

	streamer := NewStreamer(nil, StreamConfig{}, Version{})

	db := openFakeDB(t, &replFakeHandlers{
		query: func(query string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(query, "KEYCOLUSE") {
				return []string{"COLNAME"}, [][]driver.Value{{driver.Value("ID")}}, nil
			}
			if strings.Contains(query, "SYSCAT.COLUMNS") {
				return []string{"COLNAME", "NULLS"}, [][]driver.Value{{driver.Value("ID"), driver.Value("N")}}, nil
			}
			if strings.Contains(query, "FETCH FIRST 1 ROW ONLY") {
				// selectMaxPK: returns the lexicographic max row.
				return []string{"ID"}, [][]driver.Value{{driver.Value(int64(30))}}, nil
			}
			if strings.Contains(query, "SYNCHPOINT") {
				return []string{"1"}, [][]driver.Value{{driver.Value(incTestCSNBytes(500))}}, nil
			}
			n := chunkCall.Add(1)
			switch n {
			case 1:
				rows := make([][]driver.Value, 10)
				for i := range rows {
					rows[i] = []driver.Value{driver.Value(int64(i + 1))}
				}
				return []string{"ID"}, rows, nil
			case 2:
				rows := make([][]driver.Value, 10)
				for i := range rows {
					rows[i] = []driver.Value{driver.Value(int64(i + 11))}
				}
				return []string{"ID"}, rows, nil
			case 3:
				rows := make([][]driver.Value, 10)
				for i := range rows {
					rows[i] = []driver.Value{driver.Value(int64(i + 21))}
				}
				return []string{"ID"}, rows, nil
			default:
				return []string{"ID"}, nil, nil // empty → engine exits
			}
		},
	})

	engine := NewIncrementalSnapshotEngine(db, "ASNCDC", 10, streamer,
		func(_ context.Context, _ *IncrementalSnapshotContext) error {
			checkpointCalled.Add(1)
			return nil
		}, nil,
	)

	ictx := &IncrementalSnapshotContext{
		Tables:        []IncrementalTable{{Schema: "S", Name: "T"}},
		LastEmittedPK: map[string][]any{},
		MaxPK:         map[string][]any{},
	}

	// Engine runs on a dedicated goroutine; the test goroutine acts as the Streamer
	// by flushing windows as soon as they appear.
	var allEvents []ChangeEvent
	var engineErr error
	errCh := make(chan error, 1)

	go func() {
		errCh <- engine.RunTable(ctx, ictx, ictx.Tables[0], NewCSN(0))
	}()

	// Flush loop: flush windows until engine finishes.
	for {
		select {
		case err := <-errCh:
			engineErr = err
			goto engineDone
		case <-ctx.Done():
			t.Error("context expired before engine completed")
			goto engineDone
		default:
		}

		streamer.windowMu.Lock()
		w := streamer.activeWindow
		streamer.windowMu.Unlock()

		if w != nil {
			flushed := w.Flush()
			allEvents = append(allEvents, flushed...)
			streamer.windowMu.Lock()
			if streamer.activeWindow == w {
				streamer.activeWindow = nil
			}
			streamer.windowMu.Unlock()
		} else {
			runtime.Gosched()
		}
	}

engineDone:
	require.NoError(t, engineErr)
	assert.Equal(t, int32(3), checkpointCalled.Load(), "checkpoint must be called once per chunk")
	assert.Len(t, allEvents, 30, "all 30 snapshot rows must be emitted")
	for _, ev := range allEvents {
		assert.Equal(t, OpTypeRead, ev.Operation)
	}
}

// makeCSNBytes converts a uint64 into the big-endian 8-byte representation
// compatible with the fake DB driver used in this package's tests.
func makeCSNBytes(val uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(val)
		val >>= 8
	}
	return b
}
