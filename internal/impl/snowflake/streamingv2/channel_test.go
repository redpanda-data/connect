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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer implements the four endpoints the client uses. Fields are
// written by tests and read by the handler goroutine, so all access goes
// through the mu-guarded methods below.
type fakeServer struct {
	t *testing.T

	mu             sync.Mutex
	committed      string
	statusCode     string
	rowsSeen       []string
	lastQuery      map[string]string
	lastRawQuery   string
	lastHdrs       http.Header
	appendCall     int
	rowsParsed     int64
	rowsInserted   int64
	rowsErrorCount int64
	lastError      string
	rowsOverride   map[int]string // 1-indexed append-call number -> raw /rows response body

	statusCall      int // how many :bulk-channel-status requests have arrived
	statusFailFirst int // fail this many leading status calls with statusFailCode
	statusFailCode  int // HTTP status to return while statusFailFirst is unmet
	statusFailBody  string
}

// statusCalls reports how many :bulk-channel-status requests have arrived.
func (f *fakeServer) statusCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statusCall
}

// failFirstStatusCalls makes the next n :bulk-channel-status requests return
// code with body.
func (f *fakeServer) failFirstStatusCalls(n, code int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusFailFirst = n
	f.statusFailCode = code
	f.statusFailBody = body
}

// overrideRowsResponse makes the given /rows call (1-indexed) return raw
// verbatim.
func (f *fakeServer) overrideRowsResponse(call int, raw string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rowsOverride == nil {
		f.rowsOverride = map[int]string{}
	}
	f.rowsOverride[call] = raw
}

// appendCalls reports how many /rows requests have arrived so far.
func (f *fakeServer) appendCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.appendCall
}

func (f *fakeServer) setCommitted(v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = v
}

func (f *fakeServer) setStatusCode(v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCode = v
}

// setRowErrorStats overrides the bulk-channel-status counters the handler
// reports; tests use it to simulate Snowflake's cumulative, per-channel
// rows_parsed/rows_inserted/rows_error_count/last_error_message behaviour.
func (f *fakeServer) setRowErrorStats(parsed, inserted, errorCount int64, lastError string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rowsParsed = parsed
	f.rowsInserted = inserted
	f.rowsErrorCount = errorCount
	f.lastError = lastError
}

func (f *fakeServer) rowsSeenAt(i int) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rowsSeen[i]
}

func (f *fakeServer) lastQueryValue(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastQuery[key]
}

// lastRawQueryStr returns the unparsed query string of the last /rows
// request, for asserting which param names were present on the wire.
func (f *fakeServer) lastRawQueryStr() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastRawQuery
}

func (f *fakeServer) lastHdr(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastHdrs.Get(key)
}

func newFakeServer(t *testing.T) (*fakeServer, *httptest.Server) {
	f := &fakeServer{t: t, statusCode: "SUCCESS", rowsParsed: 1, rowsInserted: 1}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, "/rows"):
			f.mu.Lock()
			f.appendCall++
			f.lastHdrs = r.Header.Clone()
			f.lastQuery = map[string]string{
				"continuationToken": r.URL.Query().Get("continuationToken"),
				"offsetToken":       r.URL.Query().Get("offsetToken"),
				"startOffsetToken":  r.URL.Query().Get("startOffsetToken"),
				"endOffsetToken":    r.URL.Query().Get("endOffsetToken"),
			}
			f.lastRawQuery = r.URL.RawQuery
			b, _ := io.ReadAll(r.Body)
			f.rowsSeen = append(f.rowsSeen, string(b))
			appendCall := f.appendCall
			override, overridden := f.rowsOverride[appendCall]
			f.mu.Unlock()
			if overridden {
				_, _ = io.WriteString(w, override)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status_code": 0, "message": "ok",
				"next_continuation_token": "ct-" + strings.Repeat("x", appendCall),
			})
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			f.mu.Lock()
			f.statusCall++
			if f.statusCall <= f.statusFailFirst {
				code, body := f.statusFailCode, f.statusFailBody
				f.mu.Unlock()
				w.WriteHeader(code)
				_, _ = io.WriteString(w, body)
				return
			}
			statusCode, committed := f.statusCode, f.committed
			rowsParsed, rowsInserted, rowsErrorCount, lastError := f.rowsParsed, f.rowsInserted, f.rowsErrorCount, f.lastError
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"channel_statuses": map[string]any{
					"ch1": map[string]any{
						"channel_status_code":         statusCode,
						"last_committed_offset_token": committed,
						"rows_inserted":               rowsInserted,
						"rows_parsed":                 rowsParsed,
						"rows_error_count":            rowsErrorCount,
						"last_error_message":          lastError,
					},
				},
			})
		case strings.Contains(r.URL.Path, "/channels/"):
			f.mu.Lock()
			committed := f.committed
			rowsParsed, rowsInserted, rowsErrorCount, lastError := f.rowsParsed, f.rowsInserted, f.rowsErrorCount, f.lastError
			f.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "ct-0",
				"channel_status": map[string]any{
					"channel_status_code":         "SUCCESS",
					"last_committed_offset_token": committed,
					"rows_inserted":               rowsInserted,
					"rows_parsed":                 rowsParsed,
					"rows_error_count":            rowsErrorCount,
					"last_error_message":          lastError,
				},
			})
		default:
			f.t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	return f, srv
}

func connectedClient(t *testing.T, srv *httptest.Server, compress bool) *Client {
	t.Helper()
	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(), Compress: compress,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return c
}

func TestAppendRowsSendsNDJSONOnTheDataPlane(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "42", "42"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if got := f.rowsSeenAt(0); got != "{\"a\":1}\n" {
		t.Errorf("body = %q, want NDJSON", got)
	}
	if f.lastHdr("X-Snowflake-Authorization-Token-Type") != "OAUTH" {
		t.Errorf("data plane must use OAUTH, got %q", f.lastHdr("X-Snowflake-Authorization-Token-Type"))
	}
	if f.lastHdr("Content-Type") != "application/x-ndjson" {
		t.Errorf("Content-Type = %q", f.lastHdr("Content-Type"))
	}
	if f.lastQueryValue("startOffsetToken") != "42" {
		t.Errorf("startOffsetToken = %q", f.lastQueryValue("startOffsetToken"))
	}
	if f.lastQueryValue("endOffsetToken") != "42" {
		t.Errorf("endOffsetToken = %q", f.lastQueryValue("endOffsetToken"))
	}
	if f.lastQueryValue("continuationToken") != "ct-0" {
		t.Errorf("first append must use the token from OpenChannel, got %q", f.lastQueryValue("continuationToken"))
	}
}

// TestAppendRowsSendsStartEndOffsetTokenPair pins the wire format: the
// startOffsetToken/endOffsetToken pair the reference SDK sends. A single
// offsetToken param also works live; the pair is a choice of reference.
func TestAppendRowsSendsStartEndOffsetTokenPair(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`), []byte(`{"a":2}`)}, "5", "6"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if f.lastQueryValue("startOffsetToken") != "5" {
		t.Errorf("startOffsetToken = %q, want 5", f.lastQueryValue("startOffsetToken"))
	}
	if f.lastQueryValue("endOffsetToken") != "6" {
		t.Errorf("endOffsetToken = %q, want 6", f.lastQueryValue("endOffsetToken"))
	}
	q := f.lastRawQueryStr()
	if strings.Contains(q, "offsetToken=") && !strings.Contains(q, "startOffsetToken") {
		// Guard against a substring false-positive: "offsetToken=" is also a
		// substring of "startOffsetToken=" and "endOffsetToken=". Only fail if
		// a bare, unprefixed offsetToken param is present.
		for kv := range strings.SplitSeq(q, "&") {
			if strings.HasPrefix(kv, "offsetToken=") {
				t.Errorf("rows URL query = %q, must not contain the legacy bare offsetToken param", q)
			}
		}
	}
}

// TestAppendRowsSendsEqualStartEndOffsetTokenForSingleRowBatch: a one-row
// batch sends start == end == that row's token.
func TestAppendRowsSendsEqualStartEndOffsetTokenForSingleRowBatch(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "7", "7"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if f.lastQueryValue("startOffsetToken") != "7" || f.lastQueryValue("endOffsetToken") != "7" {
		t.Errorf("start=%q end=%q, want both to equal the single row's offset 7", f.lastQueryValue("startOffsetToken"), f.lastQueryValue("endOffsetToken"))
	}
}

// TestAppendRowsSendsEmptyStartOffsetTokenPresentNotOmitted: an empty start
// token is sent as "startOffsetToken=", not omitted (matches the SDK).
func TestAppendRowsSendsEmptyStartOffsetTokenPresentNotOmitted(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "", "9"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	q := f.lastRawQueryStr()
	if !strings.Contains(q, "startOffsetToken=&") && !strings.HasSuffix(q, "startOffsetToken=") {
		t.Errorf("rows URL query = %q, want startOffsetToken= present (empty) not omitted", q)
	}
	if f.lastQueryValue("endOffsetToken") != "9" {
		t.Errorf("endOffsetToken = %q, want 9", f.lastQueryValue("endOffsetToken"))
	}
}

func TestAppendRowsThreadsContinuationToken(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	ctx := t.Context()
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":2}`)}, "2", "2"); err != nil {
		t.Fatalf("second: %v", err)
	}
	if f.lastQueryValue("continuationToken") != "ct-x" {
		t.Errorf("second append must reuse the returned token, got %q", f.lastQueryValue("continuationToken"))
	}
}

// TestAppendRowsLeavesContinuationTokenIntactOnRejectedAppend: a rejected
// append (non-zero status_code) must not advance the continuation token, even
// though the rejection carries one.
func TestAppendRowsLeavesContinuationTokenIntactOnRejectedAppend(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	ctx := t.Context()
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	f.overrideRowsResponse(2, `{"status_code":5,"message":"rejected","next_continuation_token":"ct-should-not-be-used"}`)
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":2}`)}, "2", "2"); err == nil {
		t.Fatal("expected an error for a rejected append (non-zero status_code), got nil")
	}
	// The third append must present the token set by the FIRST append, not
	// the rejected response's next_continuation_token.
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":3}`)}, "3", "3"); err != nil {
		t.Fatalf("third: %v", err)
	}
	if got := f.lastQueryValue("continuationToken"); got != "ct-x" {
		t.Errorf("third append must reuse the token from before the rejected append, got %q", got)
	}
}

// TestAppendRowsLeavesContinuationTokenIntactOnMalformedResponse: the same
// for a response that fails to decode.
func TestAppendRowsLeavesContinuationTokenIntactOnMalformedResponse(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	ctx := t.Context()
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	f.overrideRowsResponse(2, "not valid json")
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":2}`)}, "2", "2"); err == nil {
		t.Fatal("expected a decode error for a malformed response, got nil")
	}
	if _, err := ch.AppendRows(ctx, [][]byte{[]byte(`{"a":3}`)}, "3", "3"); err != nil {
		t.Fatalf("third: %v", err)
	}
	if got := f.lastQueryValue("continuationToken"); got != "ct-x" {
		t.Errorf("third append must reuse the token from before the malformed response, got %q", got)
	}
}

func TestAppendRowsSetsCompressionHeaders(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, true).OpenChannel(t.Context(), "ch1")
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if f.lastHdr("Content-Encoding") != "zstd" {
		t.Errorf("Content-Encoding = %q, want zstd", f.lastHdr("Content-Encoding"))
	}
	if f.lastHdr("x-snowflake-uncompressed-content-length") != "8" {
		t.Errorf("uncompressed length header = %q, want 8", f.lastHdr("x-snowflake-uncompressed-content-length"))
	}
}

func TestWaitUntilCommittedReturnsWhenTokenCatchesUp(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("41")
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.setCommitted("42")
	}()
	if err := ch.WaitUntilCommitted(t.Context(), "42", 5*time.Second); err != nil {
		t.Fatalf("WaitUntilCommitted: %v", err)
	}
}

// TestWaitUntilCommittedRetriesRetryableStatusPollFailures: a retryable 503
// from the status endpoint is re-polled, not returned as a commit failure.
func TestWaitUntilCommittedRetriesRetryableStatusPollFailures(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("42")
	// Connect's pipe pre-flight already consumed a status call, so fail a
	// window starting after it rather than the very first call.
	f.failFirstStatusCalls(f.statusCalls()+3, 503, `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"`)
	if err := ch.WaitUntilCommitted(t.Context(), "42", 10*time.Second); err != nil {
		t.Fatalf("a retryable 503 during the wait must be retried, not returned: %v", err)
	}
}

// The converse, so the retry cannot silently swallow real failures: anything
// IsBackpressure does not classify as retryable must still fail immediately
// rather than burn the caller's whole commit budget re-polling a hard error.
func TestWaitUntilCommittedDoesNotRetryNonRetryableStatusPollFailures(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("42")
	f.failFirstStatusCalls(f.statusCalls()+100, 400, `{"error_code":"ERR_SOMETHING_PERMANENT"}`)
	start := time.Now()
	err := ch.WaitUntilCommitted(t.Context(), "42", 10*time.Second)
	if err == nil {
		t.Fatalf("a non-retryable status-poll failure must be returned, not retried")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("non-retryable failure took %s: it should return on the first poll, not retry to the deadline", elapsed)
	}
}

func TestWaitUntilCommittedAcceptsAHigherCommittedToken(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("100") // numerically ahead of what we sent
	if err := ch.WaitUntilCommitted(t.Context(), "42", time.Second); err != nil {
		t.Fatalf("a committed token ahead of ours must satisfy the wait: %v", err)
	}
}

func TestWaitUntilCommittedTimesOut(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("1")
	err := ch.WaitUntilCommitted(t.Context(), "42", 150*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if !strings.Contains(err.Error(), "42") {
		t.Errorf("timeout error should name the offset token, got %v", err)
	}
}

// TestWaitUntilCommittedZeroTimeoutMeansNoLimit: a zero timeout waits
// indefinitely (commit_timeout: 0), bounded only by ctx.
func TestWaitUntilCommittedZeroTimeoutMeansNoLimit(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("1")
	go func() {
		time.Sleep(3 * DefaultCommitPollInterval)
		f.setCommitted("42")
	}()
	if err := ch.WaitUntilCommitted(t.Context(), "42", 0); err != nil {
		t.Fatalf("zero timeout must mean no limit, got: %v", err)
	}

	// ctx still bounds an unlimited wait.
	f.setCommitted("1")
	ctx, cancel := context.WithTimeout(t.Context(), 2*DefaultCommitPollInterval)
	defer cancel()
	if err := ch.WaitUntilCommitted(ctx, "99", 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ctx deadline to end an unlimited wait, got: %v", err)
	}
}

// TestAppendRowsWithAllEmptyBatchAndCompressionMakesNoRequest: the "nothing
// to send" guard must key off the uncompressed length -- zstd produces a
// small non-empty frame for empty input, so a len(body) check would send a
// zero-row request and report sent=true for it.
func TestAppendRowsWithAllEmptyBatchAndCompressionMakesNoRequest(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, true).OpenChannel(t.Context(), "ch1")
	before := f.appendCalls()
	sent, err := ch.AppendRows(t.Context(), [][]byte{{}, []byte(" \n")}, "1", "1")
	if err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	if sent {
		t.Error("expected sent=false for an all-empty batch with compression on")
	}
	if got := f.appendCalls(); got != before {
		t.Errorf("expected no /rows request, but append calls went %d -> %d", before, got)
	}
}

func TestWaitUntilCommittedFailsOnNonSuccessChannel(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setStatusCode("ERR_PIPE_IN_INVALID_STATE")
	err := ch.WaitUntilCommitted(t.Context(), "42", time.Second)
	if err == nil || !strings.Contains(err.Error(), "ERR_PIPE_IN_INVALID_STATE") {
		t.Fatalf("a non-SUCCESS channel must fail loudly, got %v", err)
	}
}

// TestWaitUntilCommittedSucceedsWhenRowsErrorCountZero is the base happy
// path: no row errors, token caught up.
func TestWaitUntilCommittedSucceedsWhenRowsErrorCountZero(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("2")
	f.setRowErrorStats(2, 2, 0, "")
	if err := ch.WaitUntilCommitted(t.Context(), "2", time.Second); err != nil {
		t.Fatalf("rows_error_count=0 must not fail the wait: %v", err)
	}
}

// TestWaitUntilCommittedSucceedsDespiteRowsErrorCount: a nonzero
// rows_error_count does not fail the wait once the token has committed
// (the count is cumulative and never resets, so failing on it would wedge
// the channel).
func TestWaitUntilCommittedSucceedsDespiteRowsErrorCount(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("2") // already at or beyond offsetToken "2"
	f.setRowErrorStats(2, 1, 1, `Failed to cast variant value "not-a-number" to NUMBER`)
	if err := ch.WaitUntilCommitted(t.Context(), "2", time.Second); err != nil {
		t.Fatalf("a nonzero rows_error_count must not fail the wait once the token has committed: %v", err)
	}
}

// TestRowsRejectedReportsDeltaNotAbsolute: RowsRejected reports the delta
// since the last report, so a historical error present at open is not
// re-reported for a clean batch.
func TestRowsRejectedReportsDeltaNotAbsolute(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	f.setRowErrorStats(2, 1, 1, `Failed to cast variant value "not-a-number" to NUMBER`) // pre-existing history, as OpenChannel will see it
	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "3", "3"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	f.setCommitted("3")
	// rows_error_count stays at its pre-existing value of 1 -- this batch
	// introduced no new error, matching the measured reopen behaviour.
	if err := ch.WaitUntilCommitted(t.Context(), "3", time.Second); err != nil {
		t.Fatalf("WaitUntilCommitted: %v", err)
	}
	if got := ch.RowsRejected(); got != 0 {
		t.Errorf("RowsRejected() = %d, want 0 -- must report the delta for this batch, not the channel's stale historical rows_error_count of 1", got)
	}

	// A new error on a later batch must still be reported.
	if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":2}`)}, "4", "4"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	f.setRowErrorStats(3, 1, 2, "Failed to cast variant value <redacted> to <redacted>")
	f.setCommitted("4")
	if err := ch.WaitUntilCommitted(t.Context(), "4", time.Second); err != nil {
		t.Fatalf("WaitUntilCommitted: %v", err)
	}
	if got := ch.RowsRejected(); got != 1 {
		t.Errorf("RowsRejected() = %d, want 1 for a genuinely new error introduced by this batch", got)
	}
}

func TestLatestOffsetTokenReadsStatus(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("77")
	got, err := ch.LatestOffsetToken(t.Context())
	if err != nil {
		t.Fatalf("LatestOffsetToken: %v", err)
	}
	if got != "77" {
		t.Errorf("LatestOffsetToken = %q, want 77", got)
	}
}

func TestChannelSatisfiesIngestChannel(_ *testing.T) {
	var _ IngestChannel = (*Channel)(nil)
}

// TestOpenChannelFailsLoudlyOnNonSuccessStatus: a 200 with a non-SUCCESS
// channel_status (an invalid pipe) is an error, not a usable channel.
func TestOpenChannelFailsLoudlyOnNonSuccessStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			// Connect's pipe pre-flight lands here; see the identical case in
			// client_test.go's TestConnectDiscoversIngestHostWithKeypairJWT.
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		case strings.Contains(r.URL.Path, "/channels/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "ct-0",
				"channel_status": map[string]any{
					"channel_status_code": "ERR_PIPE_IN_INVALID_STATE",
					"last_error_message":  "pipe is paused",
				},
			})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	_, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "ERR_PIPE_IN_INVALID_STATE") || !strings.Contains(err.Error(), "pipe is paused") {
		t.Errorf("error must name the status code and reason, got %v", err)
	}
}

// TestWaitUntilCommittedTimeoutErrorNamesLastObservedToken: the timeout error
// names the last committed token seen, not just the awaited one.
func TestWaitUntilCommittedTimeoutErrorNamesLastObservedToken(t *testing.T) {
	f, srv := newFakeServer(t)
	defer srv.Close()
	ch, _ := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	f.setCommitted("17")
	err := ch.WaitUntilCommitted(t.Context(), "999", 150*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if !strings.Contains(err.Error(), "999") {
		t.Errorf("timeout error must name the awaited offset token, got %v", err)
	}
	if !strings.Contains(err.Error(), "17") {
		t.Errorf("timeout error must name the last observed committed token, got %v", err)
	}
}

// TestAppendRowsSerializesConcurrentCallsOnOneChannel: concurrent appends on
// one channel present an unbroken continuation-token chain. Reconstructing
// the chain (rather than asserting no panic) catches a wrongly scoped mutex.
func TestAppendRowsSerializesConcurrentCallsOnOneChannel(t *testing.T) {
	var (
		mu     sync.Mutex
		seen   []string
		nextID int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			// Connect's pipe pre-flight lands here; see the identical case in
			// client_test.go's TestConnectDiscoversIngestHostWithKeypairJWT.
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		case strings.HasSuffix(r.URL.Path, "/rows"):
			_, _ = io.ReadAll(r.Body)
			mu.Lock()
			seen = append(seen, r.URL.Query().Get("continuationToken"))
			nextID++
			tok := fmt.Sprintf("ct-%d", nextID)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status_code": 0, "message": "ok",
				"next_continuation_token": tok,
			})
		case strings.Contains(r.URL.Path, "/channels/"):
			_, _ = io.WriteString(w, `{"next_continuation_token":"ct-0","channel_status":{"channel_status_code":"SUCCESS"}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	ch, err := connectedClient(t, srv, false).OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}

	const n = 25
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, strconv.Itoa(i), strconv.Itoa(i)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent AppendRows: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("server saw %d append requests, want %d", len(seen), n)
	}
	want := "ct-0"
	for i, got := range seen {
		if got != want {
			t.Fatalf("append #%d presented continuationToken %q, want %q -- the mutex let two appends interleave", i, got, want)
		}
		want = fmt.Sprintf("ct-%d", i+1)
	}
}

// TestWaitUntilCommittedReturnsPromptlyOnContextCancellation: cancellation
// during the poll sleep returns immediately. The poll interval is inflated to
// 2s so a missing ctx.Done() branch shows as a clear delay.
func TestWaitUntilCommittedReturnsPromptlyOnContextCancellation(t *testing.T) {
	_, srv := newFakeServer(t)
	defer srv.Close()
	c := connectedClient(t, srv, false)
	// Inflated on this client only (channels read their client's cfg), so
	// nothing here leaks into tests running in parallel.
	c.cfg.CommitPollInterval = 2 * time.Second
	ch, err := c.OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	// committed is left at its zero value ("") by newFakeServer, so the wait
	// never succeeds on its own -- the only way it returns is cancellation.

	ctx, cancel := context.WithCancel(t.Context())
	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- ch.WaitUntilCommitted(ctx, "42", 30*time.Second)
	}()

	select {
	case err := <-errCh:
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("WaitUntilCommitted took %s to react to cancellation, want well under the %s poll interval", elapsed, c.cfg.CommitPollInterval)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitUntilCommitted returned %v, want context.Canceled", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("WaitUntilCommitted did not return within 1.5s of context cancellation -- the ctx.Done() branch may be missing from the select")
	}
}

// openChannelServerWithBody answers PUT .../channels/{name} with
// respForAttempt(attempt), 1-based, so a test can script a response
// sequence for OpenChannel's retry loop.
func openChannelServerWithBody(t *testing.T, respForAttempt func(attempt int) (status int, body string)) (attempts func() int, srv *httptest.Server) {
	t.Helper()
	var (
		mu    sync.Mutex
		count int
	)
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		case strings.Contains(r.URL.Path, "/channels/"):
			mu.Lock()
			count++
			attempt := count
			mu.Unlock()
			status, body := respForAttempt(attempt)
			if status/100 != 2 {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, body)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "ct-0",
				"channel_status":          map[string]any{"channel_status_code": "SUCCESS"},
			})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	return func() int { mu.Lock(); defer mu.Unlock(); return count }, srv
}

// openChannelServer is openChannelServerWithBody with a fixed non-2xx body:
// existing tests here only care about status codes, not the error_code the
// body carries.
func openChannelServer(t *testing.T, statusForAttempt func(attempt int) int) (attempts func() int, srv *httptest.Server) {
	t.Helper()
	return openChannelServerWithBody(t, func(attempt int) (int, string) {
		return statusForAttempt(attempt), `{"error_code":"RATE_LIMITED"}`
	})
}

// injectedRetryClient builds a connected Client with openRetryDelay and
// openRetrySleep replaced by test doubles: delays are recorded rather than
// slept, so the retry-schedule tests below never sleep a real second.
func injectedRetryClient(t *testing.T, srv *httptest.Server, maxAttempts int) (*Client, *[]time.Duration) {
	t.Helper()
	c := connectedClient(t, srv, false)
	var delays []time.Duration
	c.openRetryMaxAttempts = maxAttempts
	c.openRetryDelay = func(attempt int) time.Duration {
		return time.Duration(attempt) * time.Millisecond
	}
	c.openRetrySleep = func(ctx context.Context, d time.Duration) error {
		delays = append(delays, d)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return c, &delays
}

// TestOpenChannelRetriesOn429ThenSucceeds: a 429 on open is retried, with a
// delay before each attempt after the first.
func TestOpenChannelRetriesOn429ThenSucceeds(t *testing.T) {
	attempts, srv := openChannelServer(t, func(attempt int) int {
		if attempt < 3 {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	})
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	ch, err := c.OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if ch == nil || ch.Name() != "ch1" {
		t.Fatalf("expected an opened channel named ch1, got %+v", ch)
	}
	if got := attempts(); got != 3 {
		t.Errorf("server saw %d open attempts, want 3 (429, 429, 200)", got)
	}
	// Two retries after the first attempt: delays before attempt 2 and 3.
	if len(*delays) != 2 {
		t.Fatalf("openRetrySleep called %d times, want 2, delays=%v", len(*delays), *delays)
	}
	for i, d := range *delays {
		if want := time.Duration(i+2) * time.Millisecond; d != want {
			t.Errorf("delay[%d] = %s, want %s (openRetryDelay(attempt=%d))", i, d, want, i+2)
		}
	}
}

// TestOpenChannelExhaustsRetriesAndFailsLoud: sustained 429s stop after
// openRetryMaxAttempts with an error naming the count and the last failure.
func TestOpenChannelExhaustsRetriesAndFailsLoud(t *testing.T) {
	attempts, srv := openChannelServer(t, func(int) int { return http.StatusTooManyRequests })
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 3)

	_, err := c.OpenChannel(t.Context(), "ch1")
	if err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if got := attempts(); got != 3 {
		t.Errorf("server saw %d open attempts, want exactly openRetryMaxAttempts=3", got)
	}
	if len(*delays) != 2 {
		t.Errorf("openRetrySleep called %d times, want 2 (before attempts 2 and 3)", len(*delays))
	}
	if !strings.Contains(err.Error(), "3 attempts") {
		t.Errorf("error %q should name the attempt count", err.Error())
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error %q should carry the last (429) failure", err.Error())
	}
}

// TestOpenChannelDoesNotRetryNonRateLimitFailures: a non-retryable open
// failure returns on the first attempt.
func TestOpenChannelDoesNotRetryNonRateLimitFailures(t *testing.T) {
	attempts, srv := openChannelServer(t, func(int) int { return http.StatusBadRequest })
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	_, err := c.OpenChannel(t.Context(), "ch1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := attempts(); got != 1 {
		t.Errorf("server saw %d open attempts, want exactly 1 -- a 400 must not be retried", got)
	}
	if len(*delays) != 0 {
		t.Errorf("openRetrySleep called %d times, want 0 -- no retry means no delay", len(*delays))
	}
}

// TestOpenChannelRetriesOnRetryRequestCodeThenSucceeds: a 500
// ERR_GENERAL_EXCEPTION_RETRY_REQUEST on open (pipe not yet owned by the
// serving node) is retried like a 429.
func TestOpenChannelRetriesOnRetryRequestCodeThenSucceeds(t *testing.T) {
	attempts, srv := openChannelServerWithBody(t, func(attempt int) (int, string) {
		if attempt < 3 {
			// Bare JSON string, the shape observed live -- not an object.
			return http.StatusInternalServerError, `"ERR_GENERAL_EXCEPTION_RETRY_REQUEST"`
		}
		return http.StatusOK, ""
	})
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	ch, err := c.OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: a 500 carrying the server's own retry-request code must be retried: %v", err)
	}
	if ch == nil || ch.Name() != "ch1" {
		t.Fatalf("expected an opened channel named ch1, got %+v", ch)
	}
	if got := attempts(); got != 3 {
		t.Errorf("server saw %d open attempts, want 3 (500, 500, 200)", got)
	}
	if len(*delays) != 2 {
		t.Fatalf("openRetrySleep called %d times, want 2, delays=%v", len(*delays), *delays)
	}
}

// The converse: a 500 that is not the retry-request code must still fail on the
// first attempt. Retrying every 500 would turn a genuine server-side fault into
// a delayed failure and hide it behind the retry budget.
func TestOpenChannelDoesNotRetryUnrelated500s(t *testing.T) {
	attempts, srv := openChannelServerWithBody(t, func(int) (int, string) {
		return http.StatusInternalServerError, `{"error_code":"ERR_SOMETHING_PERMANENT"}`
	})
	defer srv.Close()
	c, _ := injectedRetryClient(t, srv, 5)

	if _, err := c.OpenChannel(t.Context(), "ch1"); err == nil {
		t.Fatal("expected a 500 without the retry-request code to fail immediately")
	}
	if got := attempts(); got != 1 {
		t.Errorf("server saw %d open attempts, want 1: an unrelated 500 must not be retried", got)
	}
}

func TestOpenChannelRetriesOn409ChannelOpenInProgressThenSucceeds(t *testing.T) {
	attempts, srv := openChannelServerWithBody(t, func(attempt int) (int, string) {
		if attempt < 3 {
			return http.StatusConflict, `{"error_code":"ERR_OPEN_CHANNEL_IN_PROGRESS"}`
		}
		return http.StatusOK, ""
	})
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	ch, err := c.OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if ch == nil || ch.Name() != "ch1" {
		t.Fatalf("expected an opened channel named ch1, got %+v", ch)
	}
	if got := attempts(); got != 3 {
		t.Errorf("server saw %d open attempts, want 3 (409, 409, 200)", got)
	}
	if len(*delays) != 2 {
		t.Fatalf("openRetrySleep called %d times, want 2, delays=%v", len(*delays), *delays)
	}
}

// TestOpenChannelDoesNotRetryOtherChannelOpen409s: the 409 retry is scoped
// to ERR_OPEN_CHANNEL_IN_PROGRESS; any other 409 code fails immediately.
func TestOpenChannelDoesNotRetryOtherChannelOpen409s(t *testing.T) {
	attempts, srv := openChannelServerWithBody(t, func(_ int) (int, string) {
		return http.StatusConflict, `{"error_code":"ERR_PIPE_IN_INVALID_STATE"}`
	})
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	_, err := c.OpenChannel(t.Context(), "ch1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := attempts(); got != 1 {
		t.Errorf("server saw %d open attempts, want exactly 1 -- a 409 with an unrecognised error_code must not be retried", got)
	}
	if len(*delays) != 0 {
		t.Errorf("openRetrySleep called %d times, want 0 -- no retry means no delay", len(*delays))
	}
	if !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "ERR_PIPE_IN_INVALID_STATE") {
		t.Errorf("error %q should carry both the status and the error_code", err.Error())
	}
}

// TestOpenChannelDoesNotRetryUncommittedDataConflict: a 409
// ERR_CHANNEL_HAS_UNCOMMITTED_DATA is neither retried nor forced past, and
// the error remains classifiable by IsUncommittedDataConflict.
func TestOpenChannelDoesNotRetryUncommittedDataConflict(t *testing.T) {
	attempts, srv := openChannelServerWithBody(t, func(_ int) (int, string) {
		return http.StatusConflict, `{"error_code":"ERR_CHANNEL_HAS_UNCOMMITTED_DATA"}`
	})
	defer srv.Close()
	c, delays := injectedRetryClient(t, srv, 5)

	_, err := c.OpenChannel(t.Context(), "ch1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := attempts(); got != 1 {
		t.Errorf("server saw %d open attempts, want exactly 1 -- ERR_CHANNEL_HAS_UNCOMMITTED_DATA must not be retried", got)
	}
	if len(*delays) != 0 {
		t.Errorf("openRetrySleep called %d times, want 0 -- no retry means no delay", len(*delays))
	}
	if !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "ERR_CHANNEL_HAS_UNCOMMITTED_DATA") {
		t.Errorf("error %q should carry both the status and the error_code", err.Error())
	}
	if IsOpenChannelInProgress(err) {
		t.Error("this 409 must NOT classify as IsOpenChannelInProgress -- it is the sibling ERR_CHANNEL_HAS_UNCOMMITTED_DATA conflict, not a concurrent-open collision")
	}
	if !IsUncommittedDataConflict(err) {
		t.Error("this 409 must classify as IsUncommittedDataConflict")
	}
}

// TestOpenChannelRetryAbortsPromptlyOnContextCancellation: cancellation
// during the retry backoff returns ctx.Err() immediately.
func TestOpenChannelRetryAbortsPromptlyOnContextCancellation(t *testing.T) {
	_, srv := openChannelServer(t, func(int) int { return http.StatusTooManyRequests })
	defer srv.Close()
	c := connectedClient(t, srv, false)
	c.openRetryMaxAttempts = 5
	c.openRetryDelay = func(int) time.Duration { return 2 * time.Second }
	ctx, cancel := context.WithCancel(t.Context())
	c.openRetrySleep = func(ctx context.Context, d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}

	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := c.OpenChannel(ctx, "ch1")
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("OpenChannel took %s to react to cancellation, want well under its 2s injected delay", elapsed)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("OpenChannel returned %v, want context.Canceled", err)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("OpenChannel did not return within 1.5s of context cancellation during retry backoff")
	}
}

// TestDefaultOpenRetryDelayMatchesSchedule: 2s/4s/8s/8s..., capped at 8s,
// plus up to 200ms jitter. A bounds check on the formula only.
func TestDefaultOpenRetryDelayMatchesSchedule(t *testing.T) {
	for _, tc := range []struct {
		attempt  int
		min, max time.Duration
	}{
		{2, 2 * time.Second, 2*time.Second + 200*time.Millisecond},
		{3, 4 * time.Second, 4*time.Second + 200*time.Millisecond},
		{4, 8 * time.Second, 8*time.Second + 200*time.Millisecond},
		{5, 8 * time.Second, 8*time.Second + 200*time.Millisecond}, // capped
	} {
		d := defaultOpenRetryDelay(tc.attempt)
		if d < tc.min || d > tc.max {
			t.Errorf("defaultOpenRetryDelay(%d) = %s, want within [%s, %s]", tc.attempt, d, tc.min, tc.max)
		}
	}
}

// TestCommitBackpressureDelayDoublesAndCaps pins the backpressure schedule:
// the poll interval, doubling per consecutive failure, capped at 4s.
func TestCommitBackpressureDelayDoublesAndCaps(t *testing.T) {
	const pollInterval = 100 * time.Millisecond
	for _, tc := range []struct {
		streak int
		want   time.Duration
	}{
		{1, 100 * time.Millisecond}, // first backpressure failure: unchanged from ordinary polling
		{2, 200 * time.Millisecond}, // doubles per additional consecutive failure
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 4 * time.Second}, // would double to 6.4s -- capped instead
		{8, 4 * time.Second}, // stays capped for any longer streak
	} {
		if got := commitBackpressureDelay(tc.streak, pollInterval); got != tc.want {
			t.Errorf("commitBackpressureDelay(%d) = %s, want %s", tc.streak, got, tc.want)
		}
	}
}
