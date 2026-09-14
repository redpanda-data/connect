// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

const (
	// streamBatchMaxRows caps the rows accumulated by the streaming reader
	// before it hands a batch to the consumer, bounding memory for one very
	// large transaction. Normal transactions flush at their commit record.
	streamBatchMaxRows = 1000
	// streamBatchMaxBytes caps the summed WAL payload of a pending batch, so
	// wide rows cannot buffer unbounded memory before a flush. The cap counts
	// raw WAL payload bytes; the decoded rows held in memory (maps, boxed
	// values, before-images) are typically several times larger, so resident
	// memory per batch is a multiple of this figure.
	streamBatchMaxBytes = 4 << 20
	// streamChannelDepth is the number of batches the messages channel buffers,
	// letting the reader keep decoding while the consumer marshals.
	//
	// It also bounds the rows resident between reader and consumer during a
	// consumer stall, none of which checkpoint_limit counts: the buffered
	// batches, the one the reader is filling, and the one the consumer holds,
	// so (streamChannelDepth + 2) batches of up to streamBatchMaxBytes of WAL
	// each, several times that once decoded.
	streamChannelDepth = 4
	// commitRemapRingSize is the smallest window of recent (last row, commit)
	// pairs the reader remembers, used when the caller supplies no
	// checkpoint_limit. The real window must cover every transaction that can
	// be in flight; see newCommitRemap.
	commitRemapRingSize = 16
)

// streamBatch accumulates decoded streaming messages and decides when the
// reader must hand them to the consumer. It also carries the "would-be" LSN
// bookkeeping the ack path depends on: lastLSN is the LSN of the last row that
// would be considered emitted, commitLSN the LSN a downstream ack of that row
// should be remapped to. Both are only observable through take, i.e. once the
// batch is actually handed over, which preserves the invariant that "emitted"
// means "sent to the consumer".
type streamBatch struct {
	maxRows  int
	maxBytes int

	msgs       []StreamMessage
	bytes      int
	commitSeen bool

	lastLSN   LSN
	commitLSN LSN
}

func newStreamBatch(maxRows, maxBytes int, startLSN LSN) *streamBatch {
	return &streamBatch{
		maxRows:   maxRows,
		maxBytes:  maxBytes,
		lastLSN:   startLSN,
		commitLSN: startLSN,
	}
}

// append records an emitted message. walBytes is the size of the WAL payload
// it was decoded from, used for the byte cap.
func (b *streamBatch) append(msg StreamMessage, walBytes int, msgLSN LSN) {
	b.msgs = append(b.msgs, msg)
	b.bytes += walBytes
	b.lastLSN = msgLSN
	b.commitLSN = msgLSN
}

// markCommit records that a commit record (emitted or suppressed) was
// processed at msgLSN and forces the next shouldFlush to return true.
func (b *streamBatch) markCommit(msgLSN LSN) {
	b.commitLSN = msgLSN
	b.commitSeen = true
}

func (b *streamBatch) shouldFlush() bool {
	return b.commitSeen || len(b.msgs) >= b.maxRows || b.bytes >= b.maxBytes
}

// take hands back the pending messages (possibly empty) together with the
// promoted LSN pair, and resets the pending state. The LSN pair is retained so
// subsequent takes keep reporting the current values.
func (b *streamBatch) take() (msgs []StreamMessage, lastLSN, commitLSN LSN) {
	msgs = b.msgs
	b.msgs = nil
	b.bytes = 0
	b.commitSeen = false
	return msgs, b.lastLSN, b.commitLSN
}

// commitRemap remembers, for recently flushed transactions, the LSN of the
// last emitted row and the LSN of the commit record that closed it. When the
// consumer acks that last row, the reader confirms the commit LSN instead so
// Postgres does not replay the transaction on restart.
//
// Keeping many pairs (rather than only the latest) matters because acks
// arrive out of order and late: a transaction's last row can be acked while
// up to checkpoint_limit later messages are already tracked downstream, plus
// the batches buffered in the channel. A miss is not data loss -- the row LSN
// is below its commit, so the transaction is replayed rather than skipped --
// but it is a duplicate the ring exists to prevent.
//
// The zero value works with a window of commitRemapRingSize; newCommitRemap
// sizes it to the caller's in-flight bound.
//
// lookup runs on every received WAL frame, so it is a map read rather than a
// scan of the ring: slot holds each live last row's ring position, and
// eviction removes the evicted row from it.
type commitRemap struct {
	pairs []commitRemapPair
	slot  map[LSN]int
	next  int
	n     int
}

type commitRemapPair struct{ lastRow, commit LSN }

// newCommitRemap sizes the window for inFlightBatches, the most batches whose
// acks can still be outstanding, each holding at least one transaction's last
// row. Callers pass checkpoint_limit plus the channel depth and the batches
// the reader and consumer each hold.
func newCommitRemap(inFlightBatches int) commitRemap {
	size := max(inFlightBatches, commitRemapRingSize)
	return commitRemap{
		pairs: make([]commitRemapPair, size),
		slot:  make(map[LSN]int, size),
	}
}

// record stores a pair. Pairs whose commit equals the last row carry no
// information (a cap-triggered flush mid-transaction) and are skipped. A pair
// for the same last row as the newest entry replaces it in place: a
// heartbeat or an empty transaction moves that row's commit forward without
// costing a slot, so a long output stall under frequent heartbeats cannot
// evict the transactions whose acks are still pending.
func (r *commitRemap) record(lastRow, commit LSN) {
	if commit == lastRow {
		return
	}
	if r.pairs == nil {
		*r = newCommitRemap(commitRemapRingSize)
	}
	size := len(r.pairs)
	if r.n > 0 {
		newest := (r.next - 1 + size) % size
		if r.pairs[newest].lastRow == lastRow {
			r.pairs[newest].commit = commit
			return
		}
	}
	if r.n == size {
		// The slot about to be reused holds the oldest pair; forget its row
		// unless a newer slot has since claimed the same row.
		if evicted := r.pairs[r.next]; r.slot[evicted.lastRow] == r.next {
			delete(r.slot, evicted.lastRow)
		}
	}
	r.pairs[r.next] = commitRemapPair{lastRow: lastRow, commit: commit}
	r.slot[lastRow] = r.next
	r.next = (r.next + 1) % size
	if r.n < size {
		r.n++
	}
}

// lookup returns the commit LSN recorded for lastRow, if any. A repeated row
// resolves to its most recent commit.
func (r *commitRemap) lookup(lastRow LSN) (LSN, bool) {
	idx, ok := r.slot[lastRow]
	if !ok {
		return 0, false
	}
	return r.pairs[idx].commit, true
}
