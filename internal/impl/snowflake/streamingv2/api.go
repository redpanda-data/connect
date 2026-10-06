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

// IngestChannel is the seam the output depends on, so it can be tested with a
// fake. See *Channel for the semantics of each method.
type IngestChannel interface {
	Name() string
	// AppendRows sends rows tagged with the batch's first and last offset
	// tokens (equal for one row). Calls on one channel must not overlap.
	// sent is false, with a nil err, when every row was empty or
	// whitespace-only and no request was made; callers must not wait for
	// such a token to commit, since nothing will ever commit it.
	AppendRows(ctx context.Context, rows [][]byte, startOffsetToken, endOffsetToken string) (sent bool, err error)
	// LatestOffsetToken is the last committed token; "" means none yet.
	LatestOffsetToken(ctx context.Context) (string, error)
	// WaitUntilCommitted blocks until the committed token is at or beyond
	// offsetToken, or timeout elapses (<= 0 means no limit). Rejected rows
	// do not fail it; see RowsRejected.
	WaitUntilCommitted(ctx context.Context, offsetToken string, timeout time.Duration) error
	// RowsRejected reports rows rejected since the last positive report and
	// advances the baseline, so each increase is reported once. Call after
	// WaitUntilCommitted.
	RowsRejected() int64
	// RowsErrorCount is the channel's cumulative lifetime rows_error_count.
	RowsErrorCount() int64
	// LastErrorMessage is the latest last_error_message, for logging.
	LastErrorMessage() string
}
