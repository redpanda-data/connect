// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// awsErrorTransport fails every Scan of table with the AWS error errType,
// and, when gone is set, every DescribeTable of table with
// ResourceNotFoundException. Every other call goes to describingTransport.
type awsErrorTransport struct {
	describingTransport
	table   string
	errType string
	gone    bool
}

func (a awsErrorTransport) Do(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	var in struct{ TableName string }
	_ = json.Unmarshal(body, &in)
	target := req.Header.Get("X-Amz-Target")
	fail := func(errType string) *http.Response {
		resp := jsonResponse(req, 400, fmt.Sprintf(`{"__type":%q,"message":"stub"}`, errType))
		resp.Header.Set("X-Amzn-ErrorType", errType)
		return resp
	}
	if in.TableName == a.table {
		switch {
		case strings.HasSuffix(target, ".Scan"):
			// Record the Scan, so tests can count attempts.
			a.mu.Lock()
			a.scanTables = append(a.scanTables, in.TableName)
			a.mu.Unlock()
			return fail(a.errType), nil
		case strings.HasSuffix(target, ".DescribeTable") && a.gone:
			return fail("ResourceNotFoundException"), nil
		}
	}
	return a.describingTransport.Do(req)
}

// lockedBuffer is a log sink safe to read while background workers log.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogsLocked points d's logger at a lockedBuffer and returns it.
func captureLogsLocked(d *dynamoDBCDCInput) *lockedBuffer {
	buf := &lockedBuffer{}
	d.log = service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf
}

// retryPending reports whether table has a retry backoff recorded.
func (r *backfillRetries) retryPending(table string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, exists := r.boffs[table]
	return exists
}

// scansOf counts the recorded Scans of table.
func scansOf(tr *scanStubTransport, table string) int {
	n := 0
	for _, s := range tr.scannedTables() {
		if s == table {
			n++
		}
	}
	return n
}

func TestClassifyBackfillError(t *testing.T) {
	notFound := fmt.Errorf("scan: %w", &dynamodbtypes.ResourceNotFoundException{Message: new("stub")})
	for _, tc := range []struct {
		name string
		gone bool
		err  error
		want backfillFailure
	}{
		{name: "deleted table", gone: true, err: notFound, want: backfillFailureTableGone},
		{name: "missing checkpoint table", err: notFound, want: backfillFailureNeedsChange},
		{name: "access denied", err: &smithy.GenericAPIError{Code: "AccessDeniedException"}, want: backfillFailureNeedsChange},
		{name: "validation", err: &smithy.GenericAPIError{Code: "ValidationException"}, want: backfillFailureNeedsChange},
		{name: "internal server error", err: &smithy.GenericAPIError{Code: "InternalServerError"}, want: backfillFailureTransient},
		{name: "not an API error", err: context.DeadlineExceeded, want: backfillFailureTransient},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := &scanStubTransport{}
			d := newBackfillTestInput(t, scan)
			d.dynamoClient = newStubDynamoClient(awsErrorTransport{describingTransport: describingTransport{scan}, table: "t1", gone: tc.gone})
			assert.Equal(t, tc.want, d.classifyBackfillError(t.Context(), "t1", tc.err))
		})
	}
}

// runQueue runs d's backfill queue until the test ends.
func runQueue(t *testing.T, d *dynamoDBCDCInput) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.runBackfillQueue(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		requireBackgroundWorkersStopped(t, d)
	})
}

// requireBackfilled waits for table's one-item backfill and acks it.
func requireBackfilled(t *testing.T, d *dynamoDBCDCInput, table string) {
	t.Helper()
	select {
	case m := <-d.msgChan:
		tbl, _ := m.msg[0].MetaGet("dynamodb_table")
		assert.Equal(t, table, tbl)
		require.NoError(t, m.ackFn(t.Context(), nil))
	case <-time.After(5 * time.Second):
		t.Fatalf("table %s not backfilled", table)
	}
}

// TestRunBackfillQueueDropsDeletedTable: a table deleted during its backfill
// is dropped from the queue and logged once, not retried forever, and the
// other tables are still backfilled.
func TestRunBackfillQueueDropsDeletedTable(t *testing.T) {
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newBackfillTestInput(t, scan)
	logs := captureLogsLocked(d)
	d.dynamoClient = newStubDynamoClient(awsErrorTransport{
		describingTransport: describingTransport{scan}, table: "t1", errType: "ResourceNotFoundException", gone: true,
	})
	d.backfillRetryBackoff = constantRetryBackoff(10 * time.Millisecond)
	d.tableStreams = map[string]*tableStream{
		"t1": newQueueTableStream(t, d, "t1"),
		"t2": newQueueTableStream(t, d, "t2"),
	}
	d.backfills = incsnapshot.NewTableQueue()
	d.backfills.Push("t1")
	d.backfills.Push("t2")
	runQueue(t, d)

	requireBackfilled(t, d, "t2")
	assert.Never(t, func() bool { return scansOf(scan, "t1") > 1 },
		300*time.Millisecond, 10*time.Millisecond, "a deleted table is not retried")
	assert.Equal(t, 0, d.backfills.Len())
	assert.Contains(t, logs.String(), "Incremental snapshot of table t1 dropped: the table no longer exists")
	assert.NotContains(t, logs.String(), "retrying in")
	assert.False(t, d.backfillRetries.retryPending("t1"), "a dropped table leaves no retry pending")
}

// TestRunBackfillQueueWaitsLongerOnNeedsChangeError: a backfill AWS denies
// is retried at the long interval from its first failure, not on the
// growing backoff a transient failure starts with.
func TestRunBackfillQueueWaitsLongerOnNeedsChangeError(t *testing.T) {
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newBackfillTestInput(t, scan)
	logs := captureLogsLocked(d)
	d.dynamoClient = newStubDynamoClient(awsErrorTransport{
		describingTransport: describingTransport{scan}, table: "t1", errType: "AccessDeniedException",
	})
	d.backfillRetryBackoff = constantRetryBackoff(10 * time.Millisecond)
	d.backfillNeedsChangeWait = 400 * time.Millisecond
	d.tableStreams = map[string]*tableStream{"t1": newQueueTableStream(t, d, "t1")}
	d.backfills = incsnapshot.NewTableQueue()
	d.backfills.Push("t1")
	runQueue(t, d)

	require.Eventually(t, func() bool { return scansOf(scan, "t1") == 1 },
		5*time.Second, 5*time.Millisecond)
	assert.Never(t, func() bool { return scansOf(scan, "t1") > 1 },
		250*time.Millisecond, 10*time.Millisecond, "the retry waits the long interval, not the 10ms backoff")
	require.Eventually(t, func() bool { return scansOf(scan, "t1") >= 2 },
		5*time.Second, 5*time.Millisecond, "the table is still retried")
	assert.Contains(t, logs.String(), "retrying in 400ms (the table is still streamed); the error needs a permission or configuration change")
}

// TestLogPrepareRetryDropsDeletedTable: a prepare that fails because the
// table was deleted is not retried, and the table is no longer
// prepare-pending.
func TestLogPrepareRetryDropsDeletedTable(t *testing.T) {
	scan := &scanStubTransport{}
	d := newBackfillTestInput(t, scan)
	d.dynamoClient = newStubDynamoClient(awsErrorTransport{describingTransport: describingTransport{scan}, table: "t1", gone: true})
	d.backfillRetries.beginPrepare("t1")

	_, retry := d.logPrepareRetry(t.Context(), "t1", &dynamodbtypes.ResourceNotFoundException{Message: new("stub")})
	assert.False(t, retry)
	assert.False(t, d.backfillRetries.skipIfPreparing("t1"), "the deleted table is not left prepare-pending")
}
