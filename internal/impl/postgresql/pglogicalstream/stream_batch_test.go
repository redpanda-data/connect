// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func row(lsn LSN) StreamMessage {
	s := lsn.String()
	return StreamMessage{Operation: InsertOpType, LSN: &s}
}

func TestStreamBatchInitialTakeIsEmptyWithStartLSN(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	require.False(t, b.shouldFlush())
	msgs, last, commit := b.take()
	require.Empty(t, msgs)
	require.Equal(t, LSN(100), last)
	require.Equal(t, LSN(100), commit)
}

func TestStreamBatchCommitFlushesAndPromotesCommitLSN(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	b.append(row(110), 50, LSN(110))
	b.append(row(120), 50, LSN(120))
	require.False(t, b.shouldFlush(), "rows alone below caps must not flush")
	b.markCommit(LSN(130))
	require.True(t, b.shouldFlush())

	msgs, last, commit := b.take()
	require.Len(t, msgs, 2)
	require.Equal(t, LSN(120), last, "last emitted is the transaction's final row")
	require.Equal(t, LSN(130), commit, "commit LSN is the commit record")

	require.False(t, b.shouldFlush(), "take resets the commit trigger")
	msgs, last, commit = b.take()
	require.Empty(t, msgs)
	require.Equal(t, LSN(120), last, "promoted values persist across an empty take")
	require.Equal(t, LSN(130), commit)
}

func TestStreamBatchRowCapFlushesMidTransaction(t *testing.T) {
	b := newStreamBatch(2, 1<<20, LSN(100))
	b.append(row(110), 50, LSN(110))
	require.False(t, b.shouldFlush())
	b.append(row(120), 50, LSN(120))
	require.True(t, b.shouldFlush())

	msgs, last, commit := b.take()
	require.Len(t, msgs, 2)
	require.Equal(t, LSN(120), last)
	require.Equal(t, LSN(120), commit, "mid-transaction flush maps commit to the last row, as the per-row path did")
}

func TestStreamBatchByteCapFlushesMidTransaction(t *testing.T) {
	b := newStreamBatch(1000, 100, LSN(100))
	b.append(row(110), 60, LSN(110))
	require.False(t, b.shouldFlush())
	b.append(row(120), 60, LSN(120))
	require.True(t, b.shouldFlush())
	msgs, _, _ := b.take()
	require.Len(t, msgs, 2)
	require.False(t, b.shouldFlush(), "byte counter resets on take")
}

func TestStreamBatchByteCapFlushesAtExactBoundary(t *testing.T) {
	b := newStreamBatch(1000, 100, LSN(100))
	b.append(row(110), 40, LSN(110))
	require.False(t, b.shouldFlush())
	b.append(row(120), 60, LSN(120))
	require.True(t, b.shouldFlush(), "bytes == maxBytes must flush")
}

func TestStreamBatchSuppressedCommitWithNothingPending(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	b.markCommit(LSN(150))
	require.True(t, b.shouldFlush())
	msgs, last, commit := b.take()
	require.Empty(t, msgs, "nothing to send")
	require.Equal(t, LSN(100), last, "last emitted unchanged")
	require.Equal(t, LSN(150), commit, "commit LSN still advances, matching the old suppressed-commit path")
}

func TestStreamBatchCapFlushThenCommit(t *testing.T) {
	b := newStreamBatch(2, 1<<20, LSN(100))
	b.append(row(110), 10, LSN(110))
	b.append(row(120), 10, LSN(120))
	_, _, _ = b.take()
	b.append(row(130), 10, LSN(130))
	b.markCommit(LSN(140))
	msgs, last, commit := b.take()
	require.Len(t, msgs, 1)
	require.Equal(t, LSN(130), last)
	require.Equal(t, LSN(140), commit)
}

func TestStreamBatchTakeReturnsIndependentSlice(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	b.append(row(110), 10, LSN(110))
	first, _, _ := b.take()
	b.append(row(120), 10, LSN(120))
	second, _, _ := b.take()
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	require.Equal(t, LSN(110).String(), *first[0].LSN, "a later append must not overwrite a slice already handed out")
	require.Equal(t, LSN(120).String(), *second[0].LSN)
}

func TestCommitRemapLookupMissOnEmpty(t *testing.T) {
	var r commitRemap
	_, ok := r.lookup(LSN(110))
	require.False(t, ok)
}

func TestCommitRemapRecordsAndLooksUp(t *testing.T) {
	var r commitRemap
	r.record(LSN(110), LSN(130))
	r.record(LSN(210), LSN(230))

	commit, ok := r.lookup(LSN(110))
	require.True(t, ok)
	require.Equal(t, LSN(130), commit)

	commit, ok = r.lookup(LSN(210))
	require.True(t, ok)
	require.Equal(t, LSN(230), commit)

	_, ok = r.lookup(LSN(120))
	require.False(t, ok)
}

func TestCommitRemapSkipsIdentityPairs(t *testing.T) {
	var r commitRemap
	r.record(LSN(110), LSN(110))
	_, ok := r.lookup(LSN(110))
	require.False(t, ok)
}

func TestCommitRemapEvictsOldest(t *testing.T) {
	var r commitRemap
	for i := 1; i <= commitRemapRingSize+1; i++ {
		r.record(LSN(i*10), LSN(i*10+5))
	}

	_, ok := r.lookup(LSN(10))
	require.False(t, ok, "the oldest pair must have been evicted")

	commit, ok := r.lookup(LSN((commitRemapRingSize + 1) * 10))
	require.True(t, ok)
	require.Equal(t, LSN((commitRemapRingSize+1)*10+5), commit)

	commit, ok = r.lookup(LSN(20))
	require.True(t, ok)
	require.Equal(t, LSN(25), commit)
}

// TestCommitRemapUpdatesNewestInPlace: a heartbeat or an empty transaction
// after a flushed transaction moves that transaction's last row to a later
// commit. It must not take a new slot each time, or a long output stall under
// frequent heartbeats evicts the pairs whose acks are still pending.
func TestCommitRemapUpdatesNewestInPlace(t *testing.T) {
	var r commitRemap
	r.record(LSN(110), LSN(130))
	for i := range commitRemapRingSize * 2 {
		r.record(LSN(210), LSN(230+i))
	}

	commit, ok := r.lookup(LSN(110))
	require.True(t, ok, "the earlier transaction must survive the heartbeats")
	require.Equal(t, LSN(130), commit)

	commit, ok = r.lookup(LSN(210))
	require.True(t, ok)
	require.Equal(t, LSN(230+commitRemapRingSize*2-1), commit, "the newest commit wins")
}

// TestNewCommitRemapCoversInFlightWindow: acks can be outstanding for as many
// transactions as checkpoint_limit admits, so the window is sized to it
// rather than to the channel depth.
func TestNewCommitRemapCoversInFlightWindow(t *testing.T) {
	const window = 1030
	r := newCommitRemap(window)
	for i := 1; i <= window; i++ {
		r.record(LSN(i*10), LSN(i*10+5))
	}

	commit, ok := r.lookup(LSN(10))
	require.True(t, ok, "the oldest in-flight transaction must still resolve")
	require.Equal(t, LSN(15), commit)

	r.record(LSN((window+1)*10), LSN((window+1)*10+5))
	_, ok = r.lookup(LSN(10))
	require.False(t, ok, "one past the window evicts the oldest")

	small := newCommitRemap(1)
	require.Len(t, small.pairs, commitRemapRingSize, "never smaller than the default window")
}

// TestCommitRemapEvictionForgetsTheRow: the index must not outlive its ring
// slot, or an evicted row would resolve to whatever commit reused the slot.
func TestCommitRemapEvictionForgetsTheRow(t *testing.T) {
	r := newCommitRemap(commitRemapRingSize)
	for i := 1; i <= commitRemapRingSize*3; i++ {
		r.record(LSN(i*10), LSN(i*10+5))
	}
	require.Len(t, r.slot, commitRemapRingSize, "the index tracks exactly the live slots")
	for i := 1; i <= commitRemapRingSize*2; i++ {
		_, ok := r.lookup(LSN(i * 10))
		require.False(t, ok, "row %d was evicted", i*10)
	}
	for i := commitRemapRingSize*2 + 1; i <= commitRemapRingSize*3; i++ {
		commit, ok := r.lookup(LSN(i * 10))
		require.True(t, ok)
		require.Equal(t, LSN(i*10+5), commit)
	}
}

func BenchmarkCommitRemapLookupMiss(b *testing.B) {
	r := newCommitRemap(1030)
	for i := 1; i <= 1030; i++ {
		r.record(LSN(i*10), LSN(i*10+5))
	}
	b.ReportAllocs()
	for i := range b.N {
		r.lookup(LSN(i*10 + 3))
	}
}
