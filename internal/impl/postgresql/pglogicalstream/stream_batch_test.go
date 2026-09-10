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
