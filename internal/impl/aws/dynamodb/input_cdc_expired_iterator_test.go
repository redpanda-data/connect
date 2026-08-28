// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestIsExpiredIteratorError(t *testing.T) {
	assert.False(t, isExpiredIteratorError(nil))
	assert.False(t, isExpiredIteratorError(errors.New("boom")))
	assert.False(t, isExpiredIteratorError(&types.TrimmedDataAccessException{}))
	assert.True(t, isExpiredIteratorError(&types.ExpiredIteratorException{}))
}

func TestIsStreamsResourceNotFoundError(t *testing.T) {
	assert.False(t, isStreamsResourceNotFoundError(nil))
	assert.False(t, isStreamsResourceNotFoundError(errors.New("boom")))
	assert.False(t, isStreamsResourceNotFoundError(&types.ExpiredIteratorException{}))
	// The DynamoDB table API's ResourceNotFoundException is a different type
	// (e.g. a missing checkpoint table) and must not be classified as a
	// permanently gone shard.
	assert.False(t, isStreamsResourceNotFoundError(&dynamodbtypes.ResourceNotFoundException{}))
	assert.True(t, isStreamsResourceNotFoundError(&types.ResourceNotFoundException{}))
	assert.True(t, isStreamsResourceNotFoundError(fmt.Errorf("refreshing: %w", &types.ResourceNotFoundException{})))
}

func TestResolveResumeIterator(t *testing.T) {
	tests := []struct {
		name       string
		lastSeq    string
		checkpoint string
		wantType   types.ShardIteratorType
		wantSeq    *string
	}{
		{
			name:       "prefers last read sequence over checkpoint",
			lastSeq:    "100",
			checkpoint: "50",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("100"),
		},
		{
			name:       "falls back to checkpoint when nothing read",
			lastSeq:    "",
			checkpoint: "50",
			wantType:   types.ShardIteratorTypeAfterSequenceNumber,
			wantSeq:    aws.String("50"),
		},
		{
			name:     "falls back to trim horizon when no sequence available",
			lastSeq:  "",
			wantType: types.ShardIteratorTypeTrimHorizon,
			wantSeq:  nil,
		},
		{
			// LATEST must never be re-acquired: the shard was already
			// positioned when the expired iterator was obtained, so LATEST
			// would silently skip everything published since.
			name:       "never re-acquires latest",
			lastSeq:    "",
			checkpoint: "",
			wantType:   types.ShardIteratorTypeTrimHorizon,
			wantSeq:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotType, gotSeq := resolveResumeIterator(tc.lastSeq, tc.checkpoint)
			assert.Equal(t, tc.wantType, gotType)
			if tc.wantSeq == nil {
				assert.Nil(t, gotSeq)
			} else {
				assert.Equal(t, *tc.wantSeq, *gotSeq)
			}
		})
	}
}

// TestInitialIteratorType locks in the start_from contract: latest applies
// only to the first discovery of a genuinely fresh pipeline; every other
// checkpoint-less shard (rotation children found on refresh cycles, or any
// shard after a restart with existing state) starts at TRIM_HORIZON so its
// backlog is never silently skipped.
func TestInitialIteratorType(t *testing.T) {
	cases := []struct {
		name      string
		startFrom string
		honor     bool
		want      types.ShardIteratorType
	}{
		{"fresh pipeline honors latest", "latest", true, types.ShardIteratorTypeLatest},
		{"fresh pipeline honors trim_horizon", "trim_horizon", true, types.ShardIteratorTypeTrimHorizon},
		{"rotation child ignores latest", "latest", false, types.ShardIteratorTypeTrimHorizon},
		{"restart with state ignores latest", "latest", false, types.ShardIteratorTypeTrimHorizon},
		{"trim_horizon unaffected by honor flag", "trim_horizon", false, types.ShardIteratorTypeTrimHorizon},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, initialIteratorType(tc.startFrom, tc.honor))
		})
	}
}

// stubStreamsTransport is a minimal net/http fake standing in for the real
// DynamoDB Streams endpoint, wired in via the AWS SDK's Options.HTTPClient
// hook. It replies to every GetRecords/GetShardIterator call with a canned
// AWS JSON-1.0 error, selected by the X-Amz-Target header the SDK's
// serializer sets, and lets the SDK's own deserializer turn that into the
// typed *types.XxxException the reader's classifier functions inspect.
type stubStreamsTransport struct {
	getRecordsErrorType       string
	getShardIteratorErrorType string

	getRecordsCalls       atomic.Int64
	getShardIteratorCalls atomic.Int64
}

func (s *stubStreamsTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")

	var errType string
	switch {
	case strings.HasSuffix(target, ".GetRecords"):
		s.getRecordsCalls.Add(1)
		errType = s.getRecordsErrorType
	case strings.HasSuffix(target, ".GetShardIterator"):
		s.getShardIteratorCalls.Add(1)
		errType = s.getShardIteratorErrorType
	default:
		return nil, fmt.Errorf("stubStreamsTransport: unexpected operation %q", target)
	}

	hdr := http.Header{}
	hdr.Set("X-Amzn-ErrorType", errType)
	hdr.Set("Content-Type", "application/x-amz-json-1.0")
	body := fmt.Sprintf(`{"__type":%q,"message":"stubbed for test"}`, errType)
	return &http.Response{
		StatusCode: 400,
		Header:     hdr,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

// newStubStreamsClient builds a real *dynamodbstreams.Client whose transport
// is entirely local (no network, no AWS account) so it deserializes responses
// exactly the way the production client would.
func newStubStreamsClient(t aws.HTTPClient) *dynamodbstreams.Client {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}
	return dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		o.HTTPClient = t
		o.Retryer = aws.NopRetryer{} // deterministic call counts
	})
}

const testStreamArn = "arn:aws:dynamodb:us-east-1:123456789012:table/test/stream/2024-01-01T00:00:00.000"

// TestStartShardReader_ExpiredIteratorShardGone: when a shard's iterator
// expires AND the shard has since been permanently deleted (GetShardIterator
// during refresh fails with ResourceNotFoundException), the reader must mark
// the shard exhausted, signal the coordinator, and return promptly instead of
// retrying the refresh forever.
func TestStartShardReader_ExpiredIteratorShardGone(t *testing.T) {
	transport := &stubStreamsTransport{
		getRecordsErrorType:       "ExpiredIteratorException",
		getShardIteratorErrorType: "ResourceNotFoundException", // shard is gone for good
	}

	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			batchSize:       10,
			pollInterval:    20 * time.Millisecond,
			throttleBackoff: 20 * time.Millisecond,
		},
		log:           service.MockResources().Logger(),
		streamsClient: newStubStreamsClient(transport),
		streamArn:     aws.String(testStreamArn),
		checkpointer:  nil, // never touched: lastSequenceNumber is non-empty below
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {
				shardID:            "shard-001",
				iterator:           aws.String("initial-iterator"),
				lastSequenceNumber: "100",
			},
		},
		shardRefreshCh: make(chan struct{}, 1),
	}

	// Generous backstop only; a correct implementation returns well before this.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.startShardReader(ctx, "shard-001")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startShardReader should give up promptly on a permanently gone shard")
	}

	d.mu.RLock()
	exhausted := d.shardReaders["shard-001"].exhausted
	d.mu.RUnlock()
	require.True(t, exhausted,
		"shard should be marked exhausted once refresh confirms it is permanently gone")

	select {
	case <-d.shardRefreshCh:
		// expected: coordinator was signalled to move on
	default:
		t.Fatal("expected a signal on shardRefreshCh")
	}
}

// TestStartTableShardReader_ExpiredIteratorShardGone covers the same
// permanent-failure classification on the multi-table reader path.
func TestStartTableShardReader_ExpiredIteratorShardGone(t *testing.T) {
	transport := &stubStreamsTransport{
		getRecordsErrorType:       "ExpiredIteratorException",
		getShardIteratorErrorType: "ResourceNotFoundException", // shard is gone for good
	}

	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			batchSize:       10,
			pollInterval:    20 * time.Millisecond,
			throttleBackoff: 20 * time.Millisecond,
		},
		log:           service.MockResources().Logger(),
		streamsClient: newStubStreamsClient(transport),
	}
	ts := &tableStream{
		tableName:    "test",
		streamArn:    testStreamArn,
		checkpointer: nil, // never touched: lastSequenceNumber is non-empty below
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {
				shardID:            "shard-001",
				iterator:           aws.String("initial-iterator"),
				lastSequenceNumber: "100",
			},
		},
		shardRefreshCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.startTableShardReader(ctx, "test", ts, "shard-001")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("startTableShardReader should give up promptly on a permanently gone shard")
	}

	ts.mu.RLock()
	exhausted := ts.shardReaders["shard-001"].exhausted
	ts.mu.RUnlock()
	require.True(t, exhausted,
		"shard should be marked exhausted once refresh confirms it is permanently gone")

	select {
	case <-ts.shardRefreshCh:
		// expected: coordinator was signalled to move on
	default:
		t.Fatal("expected a signal on shardRefreshCh")
	}
}

// TestStartShardReader_ExpiredIteratorTransientRefreshFailure locks in the
// transient-failure contract: a refresh failure that is NOT a permanent
// shard-gone signal (here LimitExceededException) must keep retrying at
// poll_interval and must not mark the shard exhausted.
func TestStartShardReader_ExpiredIteratorTransientRefreshFailure(t *testing.T) {
	transport := &stubStreamsTransport{
		getRecordsErrorType:       "ExpiredIteratorException",
		getShardIteratorErrorType: "LimitExceededException", // transient
	}

	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			batchSize:       10,
			pollInterval:    20 * time.Millisecond,
			throttleBackoff: 20 * time.Millisecond,
		},
		log:           service.MockResources().Logger(),
		streamsClient: newStubStreamsClient(transport),
		streamArn:     aws.String(testStreamArn),
		shardReaders: map[string]*dynamoDBShardReader{
			"shard-001": {
				shardID:            "shard-001",
				iterator:           aws.String("initial-iterator"),
				lastSequenceNumber: "100",
			},
		},
		shardRefreshCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.startShardReader(ctx, "shard-001")
		close(done)
	}()

	select {
	case <-done:
		// expected once ctx times out
	case <-time.After(time.Second):
		t.Fatal("startShardReader did not return after context timeout")
	}

	assert.Greater(t, transport.getShardIteratorCalls.Load(), int64(1),
		"transient refresh failures should be retried at poll_interval")

	d.mu.RLock()
	exhausted := d.shardReaders["shard-001"].exhausted
	d.mu.RUnlock()
	assert.False(t, exhausted, "a transient refresh failure must not mark the shard exhausted")
	assert.Empty(t, d.shardRefreshCh, "a transient refresh failure must not signal the coordinator")
}
