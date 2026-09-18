// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// ChannelStatus mirrors the :bulk-channel-status payload. rows_parsed excludes
// rows a pipe's WHERE clause filtered out, so it cannot be used to reconcile
// "did everything arrive".
type ChannelStatus struct {
	StatusCode               string `json:"channel_status_code"`
	LastCommittedOffsetToken string `json:"last_committed_offset_token"`
	RowsInserted             int64  `json:"rows_inserted"`
	RowsParsed               int64  `json:"rows_parsed"`
	RowsErrorCount           int64  `json:"rows_error_count"`
	LastError                string `json:"last_error_message"`
}

// Channel is one SSv2 channel on a pipe. Appends are serialised.
//
// rowsErrorCount and lastErrorMessage track Snowflake's cumulative,
// per-channel rows_error_count/last_error_message so callers can learn about
// rows newly rejected since a baseline rather than the channel's lifetime
// total: rows_error_count never resets, not on a clean commit and not on
// reopening the channel (see WaitUntilCommitted), so an absolute reading
// would report the same historical error forever. rowsErrorCount is seeded
// from OpenChannel's response -- which already reflects any pre-existing
// history on a reopened channel -- and refreshed on every WaitUntilCommitted
// poll. rowsErrorCountBaseline is seeded once, at open, to that same
// starting value (mirroring the Kafka Connector's initialErrorCount, set at
// channel open) and is advanced to the latest rowsErrorCount only when
// RowsRejected reports a positive delta -- not on every AppendRows call --
// so a delta that's already been reported once is never reported again.
type Channel struct {
	c    *Client
	name string

	mu                     sync.Mutex
	continuation           string
	rowsErrorCount         int64
	rowsErrorCountBaseline int64
	lastErrorMessage       string
}

func (c *Client) pipePath() string {
	return fmt.Sprintf("/databases/%s/schemas/%s/pipes/%s",
		url.PathEscape(c.cfg.Database), url.PathEscape(c.cfg.Schema), url.PathEscape(c.cfg.Pipe))
}

func (c *Client) authorise(ctx context.Context, req *http.Request) error {
	tok, err := c.tokens.scoped(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "OAUTH")
	return nil
}

type openChannelResponse struct {
	NextContinuationToken string        `json:"next_continuation_token"`
	ChannelStatus         ChannelStatus `json:"channel_status"`
}

// defaultOpenRetryMaxAttempts bounds OpenChannel's retry loop (see
// OpenChannel below) to 5 total attempts -- 4 retries beyond the first. This
// bound is this codebase's own choice, not inherited from anywhere else:
// OpenChannel here is called synchronously from a single WriteBatch call,
// and an unbounded retry-with-sleep inside it would block that call -- and
// anything awaiting it, including Benthos shutdown -- indefinitely, with no
// way for the caller to observe or react to sustained rate-limiting or
// open-channel contention. 5 attempts at the 2s/4s/8s/8s-capped schedule
// below is ~22s of worst-case cumulative backoff: long enough to ride out a
// brief throttling or open-collision burst, short enough that a caller (or
// ctx cancellation) regains control in bounded time.
const defaultOpenRetryMaxAttempts = 5

// defaultOpenRetryDelay is this codebase's own backoff schedule for
// OpenChannel's retry loop: 2s, doubling each attempt, capped at 8s, plus up
// to 200ms of jitter. attempt is 1-based (the delay before the 2nd, 3rd, ...
// request); it is never called for attempt 1, which has no preceding delay.
// The specific numbers are this client's own choice, not matched against
// any other implementation -- see defaultOpenRetryMaxAttempts for why
// bounded backoff matters for a call made synchronously from WriteBatch.
//
// What IS grounded elsewhere is which failures are worth retrying at all:
// the SSv2 SDK's own open-retry policy already retries HTTP 429 on
// open-channel; this client extends that same policy to the HTTP 409
// ERR_OPEN_CHANNEL_IN_PROGRESS channel-open collision handled by
// IsOpenChannelInProgress (errors.go) -- a normal outcome of concurrent
// channel opens under load, not a hard failure (observed directly during
// benchmarking under concurrent load, not just a theoretical case).
func defaultOpenRetryDelay(attempt int) time.Duration {
	const (
		initial    = 2 * time.Second
		maxDelay   = 8 * time.Second
		multiplier = 2.0
		jitterMax  = 200 * time.Millisecond
	)
	d := initial
	// attempt=2 (the first retry) uses initial (2s) as-is -- doubling starts
	// from the second retry onward, so the loop below only runs for attempt
	// >= 3. Starting the loop at i:=1 here would double before the very
	// first retry too, skipping the 2s tier entirely and producing
	// 4s/8s/8s/8s instead of the intended 2s/4s/8s/8s schedule.
	for i := 2; i < attempt; i++ {
		d = time.Duration(float64(d) * multiplier)
		if d >= maxDelay {
			d = maxDelay
			break
		}
	}
	return d + time.Duration(rand.Int63n(int64(jitterMax)+1))
}

// defaultOpenRetrySleep waits for d, or returns ctx's error immediately if
// ctx is cancelled first -- the same ctx-aware wait pattern WaitUntilCommitted
// uses in its poll loop, so a retry-in-progress never outlives shutdown.
func defaultOpenRetrySleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// OpenChannel opens or reopens a channel and returns it ready to append. A
// 2xx here does not by itself mean the channel is usable: an invalid pipe
// still answers 200 with a non-SUCCESS channel_status, so that field is
// checked explicitly and any other value fails loudly rather than handing
// back a Channel whose appends would be silently discarded.
//
// It retries on exactly two conditions: HTTP 429, and HTTP 409 with
// error_code=ERR_OPEN_CHANNEL_IN_PROGRESS (see IsOpenChannelInProgress,
// errors.go) -- a concurrent open of the same channel name already in
// flight, which is the ordinary case under concurrent load (one channel per
// Kafka partition, with concurrent opens expected) rather than a hard
// failure. The Kafka Connector's live open path (openChannelForTable) does
// not retry at all: it makes a single streamingClient.openChannel call and
// throws ERROR_5028 on any non-SUCCESS result, so this retry behavior has no
// KC counterpart to mirror -- it goes further than KC's own open path. What
// IS grounded elsewhere: the SSv2 SDK's own open-retry policy already
// retries HTTP 429, and this client extends that same policy to the 409
// case handled here, since it is the same kind of transient, non-fatal
// condition. Both conditions share the same exponential backoff plus jitter
// -- see
// defaultOpenRetryDelay -- up to openRetryMaxAttempts total attempts (see
// defaultOpenRetryMaxAttempts for why this is bounded). Any other failure,
// including a different HTTP status/error_code or a non-SUCCESS
// channel_status, returns immediately without retrying: rate-limited or
// collided channel open are the only retryable outcomes in this client, not
// a general retry policy.
//
// In particular, HTTP 409 error_code=ERR_CHANNEL_HAS_UNCOMMITTED_DATA (see
// IsUncommittedDataConflict, errors.go) is deliberately NOT retried or
// forced past here, unlike the ERR_OPEN_CHANNEL_IN_PROGRESS 409 above. That
// conflict means the channel already has appended data that has not yet
// committed; retrying blindly would just repeat the same conflict, and
// forcing a reopen would discard those uncommitted rows. This function only
// classifies and surfaces the conflict -- the error returned carries its
// 409 status and ERR_CHANNEL_HAS_UNCOMMITTED_DATA code so a caller can see
// exactly what happened -- it does not decide or implement a resolution.
// Whether to wait for the pending commit (and for how long), or to force a
// reopen under an explicit fail_on_uncommitted_rows-style opt-in (a pattern
// already adopted elsewhere, by Openflow, on its own
// openChannel/dropChannel), is a data-safety decision left to an operator
// and is an explicitly open gap.
func (c *Client) OpenChannel(ctx context.Context, name string) (*Channel, error) {
	if c.openRetryMaxAttempts < 1 {
		return nil, fmt.Errorf("streamingv2: misconfigured openRetryMaxAttempts=%d, want >= 1", c.openRetryMaxAttempts)
	}
	var lastErr error
	for attempt := 1; attempt <= c.openRetryMaxAttempts; attempt++ {
		if attempt > 1 {
			d := c.openRetryDelay(attempt)
			if err := c.openRetrySleep(ctx, d); err != nil {
				return nil, err
			}
		}
		ch, err := c.openChannelOnce(ctx, name)
		if err == nil {
			return ch, nil
		}
		lastErr = err
		// IsBackpressure covers 429 and 503 by status, plus the retryable
		// error codes by name -- including ERR_GENERAL_EXCEPTION_RETRY_REQUEST,
		// which matters specifically here. GS turns
		// ERR_PIPE_NOT_LOCALLY_OWNED_OR_NOT_STARTED into that code with a 500
		// on the open-channel path (SnowpipeStreamingRowsetService, the
		// createInternalServerErrorResponse branch), i.e. "the pipe is not yet
		// owned by the node that took this request". That is a routing
		// condition that resolves on its own, and failing the open on it means
		// the output refuses to start for a reason that would have cleared.
		//
		// A 500 was previously fatal here, so only the 429 and 409 cases were
		// retried.
		if !IsBackpressure(err) && !IsOpenChannelInProgress(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("open channel %q: exhausted %d attempts, last error: %w", name, c.openRetryMaxAttempts, lastErr)
}

// openChannelOnce is a single, non-retrying attempt at opening a channel.
// OpenChannel above wraps it with the 429 retry loop.
func (c *Client) openChannelOnce(ctx context.Context, name string) (*Channel, error) {
	if c.ingestBase == "" {
		return nil, errors.New("streamingv2: Connect must succeed before opening a channel")
	}
	u := c.ingestBase + "/v2/streaming" + c.pipePath() + "/channels/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, err
	}
	if err := c.authorise(ctx, req); err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("open channel %q: %w", name, err)
	}
	var out openChannelResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode open-channel response: %w", err)
	}
	if out.ChannelStatus.StatusCode != "SUCCESS" {
		return nil, fmt.Errorf("channel %q opened with status %q: %s", name, out.ChannelStatus.StatusCode, out.ChannelStatus.LastError)
	}
	return &Channel{
		c:                      c,
		name:                   name,
		continuation:           out.NextContinuationToken,
		rowsErrorCount:         out.ChannelStatus.RowsErrorCount,
		rowsErrorCountBaseline: out.ChannelStatus.RowsErrorCount,
		lastErrorMessage:       out.ChannelStatus.LastError,
	}, nil
}

// ChannelStatuses reads status for several channels in one control-plane call.
func (c *Client) ChannelStatuses(ctx context.Context, names []string) (map[string]ChannelStatus, error) {
	payload, err := json.Marshal(map[string]any{"channel_names": names})
	if err != nil {
		return nil, err
	}
	u := c.accountBase() + "/v2/streaming" + c.pipePath() + ":bulk-channel-status"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	jwt, err := c.controlPlaneJWT()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	req.Header.Set("Content-Type", "application/json")
	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("channel status: %w", err)
	}
	var out struct {
		ChannelStatuses map[string]ChannelStatus `json:"channel_statuses"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode channel status: %w", err)
	}
	return out.ChannelStatuses, nil
}

// Name returns the channel's name.
func (ch *Channel) Name() string { return ch.name }

type appendResponse struct {
	StatusCode            int    `json:"status_code"`
	Message               string `json:"message"`
	NextContinuationToken string `json:"next_continuation_token"`
}

// AppendRows posts rows as NDJSON. A 2xx here does NOT prove the data will
// land: an invalid pipe accepts appends and discards them. Confirm with
// WaitUntilCommitted.
//
// startOffsetToken and endOffsetToken are the first and last row's offset
// token in this batch (equal for a single-row batch). Both are sent, even
// when startOffsetToken is empty -- an empty start is rendered as
// startOffsetToken=, not omitted, matching the current reference SDK
// (snowflake-eng/snowflake-ingest-sdk, rust/src/util/urls.rs). A single
// offsetToken param also works against a real account -- verified against a
// live account, 2026-08 -- so sending the pair here is a choice of
// which reference to match, not a compatibility requirement.
//
// The continuation token is read, sent and updated while holding mu, so
// concurrent callers on the same Channel serialise instead of racing on it or
// presenting an interleaved chain to the server.
func (ch *Channel) AppendRows(ctx context.Context, rows [][]byte, startOffsetToken, endOffsetToken string) (sent bool, err error) {
	body, uncompressed, err := EncodeRows(rows, ch.c.cfg.Compress)
	if err != nil {
		return false, err
	}
	// Checked on the uncompressed length, not len(body): with Compress on,
	// zstd emits a small but non-empty frame for empty input, which would
	// otherwise send a zero-row request and report sent=true for it.
	if uncompressed == 0 {
		return false, nil
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()

	u := fmt.Sprintf("%s/v2/streaming/data%s/channels/%s/rows?startOffsetToken=%s&endOffsetToken=%s&continuationToken=%s",
		ch.c.ingestBase, ch.c.pipePath(), url.PathEscape(ch.name),
		url.QueryEscape(startOffsetToken), url.QueryEscape(endOffsetToken), url.QueryEscape(ch.continuation))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	if err := ch.c.authorise(ctx, req); err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if ch.c.cfg.Compress {
		req.Header.Set("Content-Encoding", "zstd")
		req.Header.Set("x-snowflake-uncompressed-content-length", strconv.Itoa(uncompressed))
	}
	respBody, err := ch.c.do(req)
	if err != nil {
		return false, fmt.Errorf("append rows to channel %q: %w", ch.name, err)
	}
	var out appendResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return false, fmt.Errorf("decode append response: %w", err)
	}
	if out.StatusCode != 0 {
		return false, fmt.Errorf("append rejected on channel %q: status_code=%d message=%q", ch.name, out.StatusCode, out.Message)
	}
	ch.continuation = out.NextContinuationToken
	return true, nil
}

func (ch *Channel) status(ctx context.Context) (ChannelStatus, error) {
	all, err := ch.c.ChannelStatuses(ctx, []string{ch.name})
	if err != nil {
		return ChannelStatus{}, err
	}
	st, ok := all[ch.name]
	if !ok {
		return ChannelStatus{}, fmt.Errorf("channel %q missing from status response", ch.name)
	}
	return st, nil
}

// LatestOffsetToken is the last token Snowflake reports as committed. Empty
// means nothing has committed yet.
func (ch *Channel) LatestOffsetToken(ctx context.Context) (string, error) {
	st, err := ch.status(ctx)
	if err != nil {
		return "", err
	}
	return st.LastCommittedOffsetToken, nil
}

// commitPollInterval is deliberately short: commit latency has been measured at
// several seconds, so a tight-ish poll keeps the blocking window honest without
// hammering the control plane.
var commitPollInterval = 250 * time.Millisecond

// commitBackpressureDelay is WaitUntilCommitted's wait before retrying after
// a *consecutive* backpressure failure while polling for a commit:
// commitPollInterval itself (unchanged) for the first one, doubling per
// additional consecutive failure, capped at 4s. Sustained backpressure
// during a commit wait used to be re-polled at the same steady cadence as
// ordinary healthy polling -- fine for one transient hiccup (see
// TestWaitUntilCommittedRetriesRetryableStatusPollFailures), but hammering
// an already-saturated control plane at 250ms for the whole commit_timeout
// budget is the wrong response to a *sustained* one, unlike OpenChannel's
// own retry loop, which backs off for exactly this reason. streak is
// 1-based (the streak length including the failure that just happened);
// the caller resets it to 0 the moment a poll succeeds.
func commitBackpressureDelay(streak int) time.Duration {
	const maxDelay = 4 * time.Second
	d := commitPollInterval
	for i := 1; i < streak; i++ {
		d *= 2
		if d >= maxDelay {
			return maxDelay
		}
	}
	return d
}

// WaitUntilCommitted blocks until the committed offset token is at or beyond
// offsetToken. A non-SUCCESS channel status fails immediately rather than
// waiting out the timeout.
//
// It deliberately does NOT fail on RowsErrorCount > 0. Snowflake can report
// channel_status_code="SUCCESS" and a caught-up committed token while having
// silently dropped a malformed row from an earlier batch (rows_inserted <
// rows_parsed) -- but RowsErrorCount is cumulative for the channel's whole
// lifetime and is never reset, not by a clean intervening commit and not by
// reopening the channel (confirmed empirically: a control row committed
// clean, then a malformed row on the same channel left rows_parsed=2/
// rows_inserted=1/rows_error_count=1; reopening that channel by name left
// rows_error_count=1 unchanged even after a subsequent good row committed
// successfully). An earlier version of this method failed loudly on any
// nonzero RowsErrorCount to close a silent-data-loss path, but that was
// reverted: AppendRows's own error-recovery path reopens the channel on
// error, and reopen never clears the counter, so a single malformed row
// would wedge the channel forever -- every later WaitUntilCommitted call on
// it fails, Benthos retries the batch, offset-dedup empties the retry
// (already committed), and the commit wait fails again on the same stale
// count, with no recovery short of renaming the channel.
//
// This method only cares whether the awaited offset token has committed.
// Callers that need to know about rejected rows should read
// Channel.RowsRejected after this returns: it reports the delta since the
// last-reported baseline, not the lifetime total, so it does not resurface
// a historical error on every later batch.
//
// A timeout of zero (or less) means no limit: the wait is bounded only by
// ctx. That is what the output's commit_timeout field documents for 0, so
// this method has to honour it rather than treating 0 as "already expired",
// which would fail essentially every batch on its first poll.
func (ch *Channel) WaitUntilCommitted(ctx context.Context, offsetToken string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	expired := func() bool { return timeout > 0 && time.Now().After(deadline) }
	var lastRetryableErr error
	backpressureStreak := 0
	for {
		st, err := ch.status(ctx)
		if err != nil {
			// A retryable status-poll failure is not a commit failure. The
			// status endpoint returns 503 ERR_GENERAL_EXCEPTION_RETRY_REQUEST
			// under transient server-side load -- the error name is the
			// server telling us to try again -- and returning it here failed
			// the whole batch over a hiccup that had nothing to do with
			// whether the data committed. Measured against a live account:
			// one such 503 mid-wait failed a run that passed on retry.
			//
			// Retrying is bounded by the same deadline as the commit wait
			// itself, so this cannot extend how long a caller blocks; it only
			// changes what happens to the time already budgeted. A poll that
			// keeps failing still ends in an error, but one that names the
			// last transport failure rather than a bare commit timeout, so
			// the two causes stay distinguishable in a log.
			if !IsBackpressure(err) {
				return err
			}
			lastRetryableErr = err
			if expired() {
				return fmt.Errorf("channel %q: offset %q not confirmed within %s: status polling kept failing retryably, last error: %w",
					ch.name, offsetToken, timeout, err)
			}
			backpressureStreak++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(commitBackpressureDelay(backpressureStreak)):
			}
			continue
		}
		backpressureStreak = 0
		ch.mu.Lock()
		ch.rowsErrorCount = st.RowsErrorCount
		ch.lastErrorMessage = st.LastError
		ch.mu.Unlock()
		if st.StatusCode != "" && st.StatusCode != "SUCCESS" {
			return fmt.Errorf("channel %q status %s while waiting for offset %q: %s", ch.name, st.StatusCode, offsetToken, st.LastError)
		}
		if st.LastCommittedOffsetToken != "" &&
			CompareOffsetTokens(st.LastCommittedOffsetToken, offsetToken) >= 0 {
			return nil
		}
		if expired() {
			// Mention transient poll failures if any occurred on the way to
			// the timeout. Otherwise "not committed within 30s" reads as "the
			// server accepted the rows and never committed them", when the
			// real story can be that most of the budget went to failed polls.
			if lastRetryableErr != nil {
				return fmt.Errorf("channel %q: offset %q not committed within %s (committed=%q, inserted=%d, errors=%d); status polling also failed retryably during the wait, last such error: %w",
					ch.name, offsetToken, timeout, st.LastCommittedOffsetToken, st.RowsInserted, st.RowsErrorCount, lastRetryableErr)
			}
			return fmt.Errorf("channel %q: offset %q not committed within %s (committed=%q, inserted=%d, errors=%d)",
				ch.name, offsetToken, timeout, st.LastCommittedOffsetToken, st.RowsInserted, st.RowsErrorCount)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(commitPollInterval):
		}
	}
}

// RowsRejected reports how many rows Snowflake has counted as rejected
// (rows_error_count) since the last-reported baseline, not the lifetime
// total. rows_error_count is cumulative and never resets -- not on a clean
// commit, not on reopening the channel (see WaitUntilCommitted) -- so an
// absolute reading would keep reporting the same historical error forever,
// including on a freshly reopened channel that already had one. The
// baseline starts at OpenChannel's snapshot (mirroring the Kafka
// Connector's initialErrorCount) and only advances -- to the latest
// rowsErrorCount -- when this call finds a positive delta to report; a
// delta of zero leaves the baseline untouched. That means a given increase
// in rows_error_count is reported exactly once, on whichever call first
// observes it, and never resurfaces on a later call even though
// rows_error_count itself keeps carrying it forever. Call this after
// WaitUntilCommitted returns for the batch in question. Guards against a
// negative result (e.g. from a status read that raced ahead of the
// baseline) by clamping to zero rather than reporting a nonsensical
// negative delta.
func (ch *Channel) RowsRejected() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.rowsErrorCount <= ch.rowsErrorCountBaseline {
		return 0
	}
	delta := ch.rowsErrorCount - ch.rowsErrorCountBaseline
	ch.rowsErrorCountBaseline = ch.rowsErrorCount
	return delta
}

// RowsErrorCount is Snowflake's absolute, cumulative rows_error_count for
// this channel -- the lifetime total, not the delta RowsRejected reports.
// Callers use it alongside RowsRejected to distinguish "N of these errors
// are new since baseline" from "the channel has N errors total", e.g. to
// report both counts, or to detect pre-existing errors at open that
// RowsRejected alone (delta-only) would never surface.
func (ch *Channel) RowsErrorCount() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.rowsErrorCount
}

// LastErrorMessage is Snowflake's most recently observed last_error_message
// for this channel, for logging alongside RowsRejected. It reflects the
// latest status poll rather than necessarily the row(s) RowsRejected counts.
func (ch *Channel) LastErrorMessage() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.lastErrorMessage
}
