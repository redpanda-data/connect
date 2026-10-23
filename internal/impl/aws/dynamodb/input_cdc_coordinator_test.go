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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// flakyDescribeTransport fails the first failures DescribeStream calls, then
// serves refreshStubTransport; GetRecords returns empty pages so readers
// idle.
type flakyDescribeTransport struct {
	refreshStubTransport
	failures  int64
	describes atomic.Int64
}

func (s *flakyDescribeTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	switch {
	case strings.HasSuffix(target, ".DescribeStream"):
		if s.describes.Add(1) <= s.failures {
			resp := jsonResponse(req, 400, `{"__type":"LimitExceededException","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "LimitExceededException")
			return resp, nil
		}
	case strings.HasSuffix(target, ".GetRecords"):
		return jsonResponse(req, 200, `{"Records":[],"NextShardIterator":"next"}`), nil
	}
	return s.refreshStubTransport.Do(req)
}

// TestTableCoordinatorRetriesInitialRefresh: a table coordinator whose first
// shard refresh fails must keep retrying rather than return. Returning left
// the table unread until restart, with no error surfaced beyond one log line.
func TestTableCoordinatorRetriesInitialRefresh(t *testing.T) {
	transport := &flakyDescribeTransport{
		shardIDs: []string{"shard-001"},
		failures: 2,
	}
	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			startFrom:       "trim_horizon",
			batchSize:       10,
			pollInterval:    20 * time.Millisecond,
			throttleBackoff: 20 * time.Millisecond,
		},
		log:           service.MockResources().Logger(),
		metrics:       newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient: newStubStreamsClient(transport),
		msgChan:       make(chan asyncMessage, 10),
		shardInitRetryBackoff: func() backoff.BackOff {
			return backoff.NewConstantBackOff(10 * time.Millisecond)
		},
	}
	ts := &tableStream{
		tableName:      "test",
		streamArn:      testStreamArn,
		checkpointer:   newTestCheckpointer(t),
		recordBatcher:  NewRecordBatcher(100, 100, service.MockResources().Logger()),
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		d.startTableStreamCoordinator(ctx, "test", ts)
		close(done)
	}()

	require.Eventually(t, func() bool {
		ts.mu.RLock()
		defer ts.mu.RUnlock()
		_, ok := ts.shardReaders["shard-001"]
		return ok
	}, 5*time.Second, 5*time.Millisecond, "the coordinator must retry its first refresh until it succeeds")
	select {
	case <-done:
		t.Fatal("the coordinator returned instead of running")
	default:
	}
	require.GreaterOrEqual(t, transport.describes.Load(), int64(3))

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the coordinator should return once ctx ends")
	}
}

// TestTableCoordinatorInitialRefreshStopsOnCancel: a coordinator retrying a
// failing first refresh returns once ctx ends.
func TestTableCoordinatorInitialRefreshStopsOnCancel(t *testing.T) {
	transport := &flakyDescribeTransport{failures: 1 << 30}
	d := &dynamoDBCDCInput{
		log:           service.MockResources().Logger(),
		metrics:       newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient: newStubStreamsClient(transport),
		shardInitRetryBackoff: func() backoff.BackOff {
			return backoff.NewConstantBackOff(10 * time.Millisecond)
		},
	}
	ts := &tableStream{tableName: "test", streamArn: testStreamArn, shardReaders: map[string]*dynamoDBShardReader{}}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		d.startTableStreamCoordinator(ctx, "test", ts)
		close(done)
	}()
	require.Eventually(t, func() bool { return transport.describes.Load() >= 3 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the coordinator should return once ctx ends")
	}
}
