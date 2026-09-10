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
	// wide rows cannot buffer unbounded memory before a flush.
	streamBatchMaxBytes = 4 << 20
	// streamChannelDepth is the number of batches the messages channel buffers,
	// letting the reader keep decoding while the consumer marshals.
	streamChannelDepth = 4
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
