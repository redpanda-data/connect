// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"context"
	"time"
)

// IngestChannel is the seam the Benthos output depends on. Keeping it an
// interface lets the output be tested with a fake, without a live account.
type IngestChannel interface {
	Name() string
	// AppendRows sends rows tagged with the batch's first (startOffsetToken)
	// and last (endOffsetToken) row offset tokens. A single-row batch passes
	// the same value for both. It threads the continuation token internally,
	// so callers must not overlap calls on one channel: the token chain is
	// strictly sequential.
	//
	// The start/end pair matches what Snowflake's reference ingestion SDK
	// sends. A single offsetToken param also works -- verified against a
	// live account -- so this is a choice of which reference to match, not
	// a compatibility requirement.
	//
	// sent reports whether a request was actually made: rows that are empty
	// or whitespace-only after trimming are skipped entirely (EncodeRows),
	// and a batch consisting only of such rows makes no request at all --
	// sent is false in that case, with err nil (this is not a failure).
	// Callers must not wait for startOffsetToken/endOffsetToken to commit
	// when sent is false: nothing was sent for that token, so nothing will
	// ever cause it to commit, and a caller that waits anyway blocks for its
	// full commit timeout on every attempt, forever, since the same rows
	// re-encode to nothing on every redelivery too. A batch consisting
	// entirely of Kafka/Redpanda tombstones (null values, which arrive as an
	// empty message body) hits this in practice.
	AppendRows(ctx context.Context, rows [][]byte, startOffsetToken, endOffsetToken string) (sent bool, err error)
	// LatestOffsetToken is the last token Snowflake reports as committed. Empty
	// means nothing has committed yet.
	LatestOffsetToken(ctx context.Context) (string, error)
	// WaitUntilCommitted blocks until the committed token is at or beyond
	// offsetToken, or the timeout elapses. It does not fail on rows Snowflake
	// rejected -- see RowsRejected.
	WaitUntilCommitted(ctx context.Context, offsetToken string, timeout time.Duration) error
	// RowsRejected reports how many rows Snowflake counted as rejected
	// (rows_error_count) since the last-reported baseline, not the lifetime
	// total: rows_error_count is cumulative and never resets, so an absolute
	// reading would keep reporting the same historical error forever. A
	// given increase is reported exactly once -- the baseline advances to
	// match as soon as a positive delta is returned. Call after
	// WaitUntilCommitted returns for the batch in question.
	RowsRejected() int64
	// RowsErrorCount is Snowflake's absolute, cumulative rows_error_count for
	// this channel -- the lifetime total, unlike RowsRejected's delta. Used
	// to report both "new" and "total" error counts, and to detect
	// pre-existing errors at open that a zero RowsRejected delta wouldn't
	// otherwise surface.
	RowsErrorCount() int64
	// LastErrorMessage is Snowflake's most recently observed
	// last_error_message, for logging alongside RowsRejected.
	LastErrorMessage() string
}
