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

// ackStamps returns each message's AckLSN, the position the consumer will
// acknowledge for it.
func ackStamps(t *testing.T, msgs []StreamMessage) []LSN {
	t.Helper()
	out := make([]LSN, 0, len(msgs))
	for _, m := range msgs {
		require.NotNil(t, m.AckLSN, "every emitted message is stamped")
		lsn, err := ParseLSN(*m.AckLSN)
		require.NoError(t, err)
		out = append(out, lsn)
	}
	return out
}

func TestStreamBatchInitialTakeIsEmptyWithStartLSN(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	require.False(t, b.shouldFlush())
	msgs, ack, commit := b.take()
	require.Empty(t, msgs)
	require.Equal(t, LSN(100), ack)
	require.Equal(t, LSN(100), commit)
}

func TestStreamBatchCommitFlushesAndPromotesCommitLSN(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	b.append(row(110), 50, LSN(110))
	b.append(row(120), 50, LSN(120))
	require.False(t, b.shouldFlush(), "rows alone below caps must not flush")
	b.markCommit(LSN(130))
	require.True(t, b.shouldFlush())

	msgs, ack, commit := b.take()
	require.Len(t, msgs, 2)
	require.Equal(t, LSN(130), ack, "the batch closed at a commit, so its ack target is the commit record")
	require.Equal(t, LSN(130), commit, "commit LSN is the commit record")
	require.Equal(t, []LSN{110, 130}, ackStamps(t, msgs),
		"rows are stamped with their own LSN except the last, which carries the commit")

	require.False(t, b.shouldFlush(), "take resets the commit trigger")
	msgs, ack, commit = b.take()
	require.Empty(t, msgs)
	require.Equal(t, LSN(120), ack, "with nothing pending the ack target reverts to the last row")
	require.Equal(t, LSN(130), commit, "the commit persists across an empty take")
}

func TestStreamBatchRowCapFlushesMidTransaction(t *testing.T) {
	b := newStreamBatch(2, 1<<20, LSN(100))
	b.append(row(110), 50, LSN(110))
	require.False(t, b.shouldFlush())
	b.append(row(120), 50, LSN(120))
	require.True(t, b.shouldFlush())

	msgs, ack, commit := b.take()
	require.Len(t, msgs, 2)
	require.Equal(t, LSN(120), ack, "no commit closed this batch, so the ack target is the last row")
	require.Equal(t, LSN(120), commit, "mid-transaction flush maps commit to the last row, as the per-row path did")
	require.Equal(t, []LSN{110, 120}, ackStamps(t, msgs),
		"a cap-triggered flush stamps every row with its own LSN: confirming a commit here would skip the rest of the transaction")
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
	msgs, ack, commit := b.take()
	require.Empty(t, msgs, "nothing to send")
	require.Equal(t, LSN(100), ack, "ack target unchanged: no row was emitted to stamp")
	require.Equal(t, LSN(150), commit, "commit LSN still advances, matching the old suppressed-commit path")
}

func TestStreamBatchCapFlushThenCommit(t *testing.T) {
	b := newStreamBatch(2, 1<<20, LSN(100))
	b.append(row(110), 10, LSN(110))
	b.append(row(120), 10, LSN(120))
	_, _, _ = b.take()
	b.append(row(130), 10, LSN(130))
	b.markCommit(LSN(140))
	msgs, ack, commit := b.take()
	require.Len(t, msgs, 1)
	require.Equal(t, LSN(140), ack)
	require.Equal(t, LSN(140), commit)
	require.Equal(t, []LSN{140}, ackStamps(t, msgs), "the transaction's final row carries the commit")
}

// TestStreamBatchCommitWithNothingPendingLeavesEarlierStampsAlone: a
// transaction whose last row went out in a cap-triggered flush cannot have
// that row re-stamped with the commit. The reader covers this case with its
// last-emitted bookkeeping (see commitLSN in the read loop); take only
// reports the commit so that bookkeeping can advance.
func TestStreamBatchCommitWithNothingPendingLeavesEarlierStampsAlone(t *testing.T) {
	b := newStreamBatch(2, 1<<20, LSN(100))
	b.append(row(110), 10, LSN(110))
	b.append(row(120), 10, LSN(120))
	first, ack, _ := b.take()
	require.Equal(t, []LSN{110, 120}, ackStamps(t, first))
	require.Equal(t, LSN(120), ack)

	b.markCommit(LSN(125))
	msgs, ack, commit := b.take()
	require.Empty(t, msgs)
	require.Equal(t, LSN(120), ack, "ack target is still the row already emitted")
	require.Equal(t, LSN(125), commit)
	require.Equal(t, []LSN{110, 120}, ackStamps(t, first), "the emitted slice is not touched after the fact")
}

// TestStreamBatchEmittedCommitMarkerIsItsOwnStamp: with
// include_transaction_markers the commit record is itself a message, so the
// stamp and the commit coincide.
func TestStreamBatchEmittedCommitMarkerIsItsOwnStamp(t *testing.T) {
	b := newStreamBatch(10, 1<<20, LSN(100))
	b.append(row(110), 10, LSN(110))
	c := LSN(120).String()
	b.append(StreamMessage{Operation: CommitOpType, LSN: &c}, 5, LSN(120))
	b.markCommit(LSN(120))
	msgs, ack, commit := b.take()
	require.Equal(t, LSN(120), ack)
	require.Equal(t, LSN(120), commit)
	require.Equal(t, []LSN{110, 120}, ackStamps(t, msgs))
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
