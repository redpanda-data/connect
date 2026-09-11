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
	streamChannelDepth = 4
	// commitRemapRingSize bounds how many recent (last row, commit) pairs the
	// reader remembers. It comfortably exceeds the channel depth plus the batch
	// being decoded plus what the consumer may hold, so an ack for the last row
	// of any transaction still in flight can be remapped to its commit record.
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
// Postgres does not replay the transaction on restart. Keeping several pairs
// (rather than only the latest) matters because up to streamChannelDepth
// batches can sit in the channel unconsumed: an ack for an earlier
// transaction's last row must still find its commit.
type commitRemap struct {
	pairs [commitRemapRingSize]commitRemapPair
	next  int
	n     int
}

type commitRemapPair struct{ lastRow, commit LSN }

// record stores a pair. Pairs whose commit equals the last row carry no
// information (a cap-triggered flush mid-transaction) and are skipped.
func (r *commitRemap) record(lastRow, commit LSN) {
	if commit == lastRow {
		return
	}
	r.pairs[r.next] = commitRemapPair{lastRow: lastRow, commit: commit}
	r.next = (r.next + 1) % commitRemapRingSize
	if r.n < commitRemapRingSize {
		r.n++
	}
}

// lookup returns the commit LSN recorded for lastRow, if any. Newest first,
// so a repeated LSN resolves to its most recent commit.
func (r *commitRemap) lookup(lastRow LSN) (LSN, bool) {
	for i := 1; i <= r.n; i++ {
		p := r.pairs[(r.next-i+commitRemapRingSize)%commitRemapRingSize]
		if p.lastRow == lastRow {
			return p.commit, true
		}
	}
	return 0, false
}
