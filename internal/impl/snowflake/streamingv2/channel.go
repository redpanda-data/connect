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
	// LastErrorOffsetUpperBound is the end token of the last rowset (one
	// append request) that contained errors, so it names the batch the
	// latest error belongs to.
	LastErrorOffsetUpperBound string `json:"last_error_offset_upper_bound"`
}

// Channel is one SSv2 channel on a pipe. Appends are serialised.
//
// rowsErrorCount mirrors Snowflake's cumulative rows_error_count, which never
// resets (not on commit, not on reopen). rowsErrorCountBaseline is the value
// at open and advances only when RowsRejected reports a positive delta, so
// each increase is reported exactly once.
type Channel struct {
	c    *Client
	name string

	mu                        sync.Mutex
	continuation              string
	committedAtOpen           string
	rowsErrorCount            int64
	rowsErrorCountBaseline    int64
	lastErrorMessage          string
	lastErrorOffsetUpperBound string
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

// DefaultOpenRetryMaxAttempts bounds OpenChannel's retry loop. OpenChannel
// runs synchronously inside WriteBatch, so the retry budget must be finite:
// 5 attempts at 2s/4s/8s/8s is ~22s worst case.
const DefaultOpenRetryMaxAttempts = 5

// Defaults for the operator-tunable durations on Config, exported so the
// output's config spec can quote them as field defaults.
const (
	// DefaultOpenRetryInitialDelay is the wait before the first retry; each
	// further retry doubles it up to DefaultOpenRetryMaxDelay.
	DefaultOpenRetryInitialDelay = 2 * time.Second
	DefaultOpenRetryMaxDelay     = 8 * time.Second
	// DefaultCommitPollInterval is how often WaitUntilCommitted polls
	// channel status. Commits typically take a few seconds.
	DefaultCommitPollInterval = 250 * time.Millisecond
	// DefaultRequestTimeout bounds a single HTTP exchange when Config
	// supplies no HTTPClient.
	DefaultRequestTimeout = 60 * time.Second
)

// openRetryDelayFor builds OpenChannel's backoff: initial, doubling per
// attempt, capped at maxDelay, plus up to 200ms of jitter. attempt is
// 1-based and the func is never called for attempt 1.
func openRetryDelayFor(initial, maxDelay time.Duration) func(attempt int) time.Duration {
	const (
		multiplier = 2.0
		jitterMax  = 200 * time.Millisecond
	)
	return func(attempt int) time.Duration {
		d := initial
		// attempt 2 (the first retry) waits initial; doubling starts at 3.
		for i := 2; i < attempt; i++ {
			d = time.Duration(float64(d) * multiplier)
			if d >= maxDelay {
				d = maxDelay
				break
			}
		}
		return d + time.Duration(rand.Int63n(int64(jitterMax)+1))
	}
}

// defaultOpenRetryDelay is openRetryDelayFor at the default schedule.
var defaultOpenRetryDelay = openRetryDelayFor(DefaultOpenRetryInitialDelay, DefaultOpenRetryMaxDelay)

// defaultOpenRetrySleep waits for d or until ctx is cancelled.
func defaultOpenRetrySleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// OpenChannel opens or reopens a channel. A 200 with a non-SUCCESS
// channel_status (an invalid pipe does this) is an error, not a usable
// channel. Retries with bounded backoff on backpressure (see IsBackpressure)
// and on the 409 open-collision (IsOpenChannelInProgress); everything else,
// including 409 ERR_CHANNEL_HAS_UNCOMMITTED_DATA, is returned as-is -- forcing
// past uncommitted data would discard rows, so that decision is the caller's.
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
		// IsBackpressure includes ERR_GENERAL_EXCEPTION_RETRY_REQUEST, which
		// Snowflake returns (as a 500) while a pipe is not yet owned by the
		// node handling the request -- a routing condition that clears on
		// its own.
		if !IsBackpressure(err) && !IsOpenChannelInProgress(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("open channel %q: exhausted %d attempts, last error: %w", name, c.openRetryMaxAttempts, lastErr)
}

// openChannelOnce is a single, non-retrying open attempt.
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
		return nil, &ChannelStatusError{Channel: name, Code: out.ChannelStatus.StatusCode, Message: out.ChannelStatus.LastError, OnOpen: true}
	}
	return &Channel{
		c:                         c,
		name:                      name,
		continuation:              out.NextContinuationToken,
		committedAtOpen:           out.ChannelStatus.LastCommittedOffsetToken,
		rowsErrorCount:            out.ChannelStatus.RowsErrorCount,
		rowsErrorCountBaseline:    out.ChannelStatus.RowsErrorCount,
		lastErrorMessage:          out.ChannelStatus.LastError,
		lastErrorOffsetUpperBound: out.ChannelStatus.LastErrorOffsetUpperBound,
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

// AppendRows posts rows as NDJSON. A 2xx does not prove the data will land
// (an invalid pipe accepts and discards); confirm with WaitUntilCommitted.
// startOffsetToken/endOffsetToken are the batch's first and last row tokens
// (equal for one row) and both are always sent, matching the reference SDK.
// payloadBytes is the size of the body sent (compressed when compression is
// on), for metrics; it is zero when nothing was sent. The continuation token
// is read, sent and updated under mu so concurrent callers serialise instead
// of presenting an interleaved chain.
func (ch *Channel) AppendRows(ctx context.Context, rows [][]byte, startOffsetToken, endOffsetToken string) (sent bool, payloadBytes int, err error) {
	body, uncompressed, err := EncodeRows(rows, ch.c.cfg.Compress)
	if err != nil {
		return false, 0, err
	}
	// Uncompressed length, not len(body): zstd emits a non-empty frame for
	// empty input.
	if uncompressed == 0 {
		return false, 0, nil
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()

	u := fmt.Sprintf("%s/v2/streaming/data%s/channels/%s/rows?startOffsetToken=%s&endOffsetToken=%s&continuationToken=%s",
		ch.c.ingestBase, ch.c.pipePath(), url.PathEscape(ch.name),
		url.QueryEscape(startOffsetToken), url.QueryEscape(endOffsetToken), url.QueryEscape(ch.continuation))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return false, 0, err
	}
	if err := ch.c.authorise(ctx, req); err != nil {
		return false, 0, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if ch.c.cfg.Compress {
		req.Header.Set("Content-Encoding", "zstd")
		req.Header.Set("x-snowflake-uncompressed-content-length", strconv.Itoa(uncompressed))
	}
	respBody, err := ch.c.do(req)
	if err != nil {
		return false, 0, fmt.Errorf("append rows to channel %q: %w", ch.name, err)
	}
	var out appendResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return false, 0, fmt.Errorf("decode append response: %w", err)
	}
	if out.StatusCode != 0 {
		return false, 0, fmt.Errorf("append rejected on channel %q: status_code=%d message=%q", ch.name, out.StatusCode, out.Message)
	}
	ch.continuation = out.NextContinuationToken
	return true, len(body), nil
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

// CommittedAtOpen is the channel's last committed offset token as reported
// in the open-channel response, so callers that only need the value at open
// avoid a status round trip. Empty means nothing had committed.
func (ch *Channel) CommittedAtOpen() string { return ch.committedAtOpen }

// LatestOffsetToken is the last token Snowflake reports as committed. Empty
// means nothing has committed yet.
func (ch *Channel) LatestOffsetToken(ctx context.Context) (string, error) {
	st, err := ch.status(ctx)
	if err != nil {
		return "", err
	}
	return st.LastCommittedOffsetToken, nil
}

// commitBackpressureDelay is the wait after the streak-th consecutive
// backpressure failure while polling for a commit: pollInterval for the
// first, doubling per further failure, capped at 4s, so a saturated control
// plane is not hammered at the healthy poll cadence.
func commitBackpressureDelay(streak int, pollInterval time.Duration) time.Duration {
	const maxDelay = 4 * time.Second
	d := pollInterval
	for i := 1; i < streak; i++ {
		d *= 2
		if d >= maxDelay {
			return maxDelay
		}
	}
	return d
}

// isTransientPollError reports whether a status-poll failure should be
// retried within the commit deadline. Polling is read-only, so anything
// that says nothing about the commit is retried: backpressure (429/503 and
// the retry codes), any other 5xx (a gateway in front of the account host),
// and a transport failure from the HTTP client (connection reset, EOF, a
// request timeout). A 4xx, a malformed body, a channel missing from the
// response, or a local failure such as signing the JWT is definite and
// ends the wait. A done ctx always ends it.
func isTransientPollError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if IsBackpressure(err) {
		return true
	}
	if code, ok := statusCodeOf(err); ok {
		return code >= 500
	}
	var ue *url.Error
	return errors.As(err, &ue)
}

// maxPollOutage bounds how long WaitUntilCommitted keeps retrying transient
// poll failures when it has no deadline of its own (timeout <= 0). Without
// it, a dead control plane would hang the batch silently for as long as the
// ctx lives.
const maxPollOutage = 2 * time.Minute

// WaitUntilCommitted blocks until the committed offset token is at or beyond
// offsetToken. A non-SUCCESS channel status fails immediately. It does not
// fail on RowsErrorCount > 0: that counter is cumulative and never resets
// (not on commit, not on reopen), so failing on it would wedge a channel
// forever after one bad row -- callers read RowsRejected instead. A timeout
// of zero or less means no limit; the wait is then bounded only by ctx.
func (ch *Channel) WaitUntilCommitted(ctx context.Context, offsetToken string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	expired := func() bool { return timeout > 0 && time.Now().After(deadline) }
	var lastRetryableErr error
	var outageSince time.Time
	backpressureStreak := 0
	for {
		st, err := ch.status(ctx)
		if err != nil {
			if outageSince.IsZero() {
				outageSince = time.Now()
			}
			if timeout <= 0 && time.Since(outageSince) > maxPollOutage && isTransientPollError(ctx, err) {
				return fmt.Errorf("channel %q: offset %q not confirmed: status polling has failed for %s, last error: %w",
					ch.name, offsetToken, maxPollOutage, err)
			}
			// A retryable poll failure is not a commit failure: keep polling
			// within the same deadline, and name the last such failure if
			// the wait ultimately times out. Status polling is read-only, so
			// a transport error (connection reset, EOF, a timeout on the
			// request itself) is retried too; only a definite answer from
			// the server (a 4xx, a malformed body) or the caller's ctx ends
			// the wait early.
			if !isTransientPollError(ctx, err) {
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
			case <-time.After(commitBackpressureDelay(backpressureStreak, ch.c.cfg.CommitPollInterval)):
			}
			continue
		}
		backpressureStreak = 0
		outageSince = time.Time{}
		ch.mu.Lock()
		ch.rowsErrorCount = st.RowsErrorCount
		ch.lastErrorMessage = st.LastError
		ch.lastErrorOffsetUpperBound = st.LastErrorOffsetUpperBound
		ch.mu.Unlock()
		if st.StatusCode != "" && st.StatusCode != "SUCCESS" {
			return &ChannelStatusError{Channel: ch.name, Code: st.StatusCode, Message: st.LastError, OffsetToken: offsetToken}
		}
		if st.LastCommittedOffsetToken != "" &&
			CompareOffsetTokens(st.LastCommittedOffsetToken, offsetToken) >= 0 {
			return nil
		}
		if expired() {
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
		case <-time.After(ch.c.cfg.CommitPollInterval):
		}
	}
}

// RowsRejected reports rows Snowflake has rejected since the last positive
// report, not the lifetime total (see Channel), and advances the baseline so
// each increase is reported exactly once. Call after WaitUntilCommitted.
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

// RowsErrorCount is Snowflake's cumulative rows_error_count for this channel,
// the lifetime total alongside RowsRejected's delta.
func (ch *Channel) RowsErrorCount() int64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.rowsErrorCount
}

// LastErrorMessage is the last_error_message from the latest status poll.
func (ch *Channel) LastErrorMessage() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.lastErrorMessage
}

// LastErrorOffsetUpperBound is the end token of the last append that
// contained rejected rows, as of the last status poll ("" if none).
func (ch *Channel) LastErrorOffsetUpperBound() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.lastErrorOffsetUpperBound
}
