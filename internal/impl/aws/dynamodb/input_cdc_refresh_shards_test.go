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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newTestCheckpointer builds a Checkpointer for a pipeline with no prior
// checkpoint state: every resume lookup misses.
func newTestCheckpointer(t *testing.T) *Checkpointer {
	t.Helper()
	return &Checkpointer{
		tableName:       "checkpoints",
		sourceTable:     "test",
		streamArn:       testStreamArn,
		checkpointLimit: 1000,
		svc: &fakeCheckpointAPI{
			getItem: func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
				return &dynamodb.GetItemOutput{}, nil
			},
		},
		log: service.MockResources().Logger(),
	}
}

// refreshStubTransport fakes the DynamoDB Streams endpoint for shard
// discovery: DescribeStream returns a fixed shard list, and GetShardIterator
// succeeds or fails per shard. onGetShardIterator, when set, runs before each
// GetShardIterator response is built (used to cancel contexts mid-cycle).
type refreshStubTransport struct {
	mu                 sync.Mutex
	shardIDs           []string
	failShards         map[string]bool
	iteratorCalls      []string
	onGetShardIterator func(shardID string)
}

func (s *refreshStubTransport) setFailShards(shards ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failShards = map[string]bool{}
	for _, id := range shards {
		s.failShards[id] = true
	}
}

func (s *refreshStubTransport) iteratorCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.iteratorCalls)
}

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	hdr := http.Header{}
	hdr.Set("Content-Type", "application/x-amz-json-1.0")
	return &http.Response{
		StatusCode: status,
		Header:     hdr,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func (s *refreshStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	switch {
	case strings.HasSuffix(target, ".DescribeStream"):
		s.mu.Lock()
		shards := make([]map[string]any, 0, len(s.shardIDs))
		for _, id := range s.shardIDs {
			shards = append(shards, map[string]any{"ShardId": id})
		}
		s.mu.Unlock()
		desc, err := json.Marshal(map[string]any{
			"StreamDescription": map[string]any{
				"StreamArn":    testStreamArn,
				"StreamStatus": "ENABLED",
				"Shards":       shards,
			},
		})
		if err != nil {
			return nil, err
		}
		return jsonResponse(req, 200, string(desc)), nil

	case strings.HasSuffix(target, ".GetShardIterator"):
		payload, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		var in struct{ ShardId string }
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.iteratorCalls = append(s.iteratorCalls, in.ShardId)
		fail := s.failShards[in.ShardId]
		hook := s.onGetShardIterator
		s.mu.Unlock()
		if hook != nil {
			hook(in.ShardId)
		}
		if fail {
			resp := jsonResponse(req, 400, `{"__type":"LimitExceededException","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "LimitExceededException")
			return resp, nil
		}
		return jsonResponse(req, 200, fmt.Sprintf(`{"ShardIterator":"iter-%s"}`, in.ShardId)), nil

	default:
		return nil, fmt.Errorf("refreshStubTransport: unexpected operation %q", target)
	}
}

func newRefreshTestInput(t *testing.T, transport *refreshStubTransport) *dynamoDBCDCInput {
	t.Helper()
	return &dynamoDBCDCInput{
		conf:           dynamoDBCDCConfig{startFrom: "trim_horizon"},
		log:            service.MockResources().Logger(),
		metrics:        newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient:  newStubStreamsClient(transport),
		streamArn:      aws.String(testStreamArn),
		checkpointer:   newTestCheckpointer(t),
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
	}
}

func registeredShards(d *dynamoDBCDCInput) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ids := make([]string, 0, len(d.shardReaders))
	for id := range d.shardReaders {
		ids = append(ids, id)
	}
	return ids
}

// TestRefreshShards_SkipsFailedShardAndCommitsRest: one shard failing to
// resolve must not discard the whole discovery cycle. Healthy shards are
// registered (partial progress commits), the failed shard is retried on the
// next cycle, and per-shard failures are not fatal. This is the
// non-convergence wedge behind INC-2974's preprod pipeline: with the
// all-or-nothing cycle, a single error meant zero readers ever started.
func TestRefreshShards_SkipsFailedShardAndCommitsRest(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-001", "shard-002", "shard-003"}}
	transport.setFailShards("shard-002")

	d := newRefreshTestInput(t, transport)
	d.honorStartFrom.Store(true)

	require.NoError(t, d.refreshShards(t.Context()),
		"a per-shard failure must not fail the whole refresh cycle")
	assert.ElementsMatch(t, []string{"shard-001", "shard-003"}, registeredShards(d),
		"healthy shards must be registered even when another shard fails")
	assert.False(t, d.honorStartFrom.Load(),
		"the first committed discovery positions the pipeline; later retries of failed shards must use TRIM_HORIZON so no backlog is skipped")

	// The failed shard recovers on the next cycle.
	transport.setFailShards()
	require.NoError(t, d.refreshShards(t.Context()))
	assert.ElementsMatch(t, []string{"shard-001", "shard-002", "shard-003"}, registeredShards(d),
		"a previously failed shard must be picked up by the next refresh cycle")
}

// TestRefreshShards_ZeroCommitKeepsStartFrom: a discovery cycle that commits
// no shards has not positioned anything, so it must not consume the
// once-only start_from window - otherwise a transient full-cycle failure on
// a fresh pipeline silently converts start_from: latest into a 24h backlog
// replay.
func TestRefreshShards_ZeroCommitKeepsStartFrom(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-001", "shard-002"}}
	transport.setFailShards("shard-001", "shard-002")

	d := newRefreshTestInput(t, transport)
	d.honorStartFrom.Store(true)

	require.NoError(t, d.refreshShards(t.Context()))
	assert.Empty(t, registeredShards(d))
	assert.True(t, d.honorStartFrom.Load(),
		"a cycle that committed nothing must keep honoring start_from for the first real discovery")

	transport.setFailShards()
	require.NoError(t, d.refreshShards(t.Context()))
	assert.ElementsMatch(t, []string{"shard-001", "shard-002"}, registeredShards(d))
	assert.False(t, d.honorStartFrom.Load())
}

// TestRefreshShards_BudgetExhaustedCommitsProgress: when the refresh budget
// (the caller's context deadline) expires mid-cycle, the shards already
// resolved must still be committed - and the truncation is not an error, so
// successive cycles converge on a large backlog instead of restarting from
// zero every time.
func TestRefreshShards_BudgetExhaustedCommitsProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	transport := &refreshStubTransport{shardIDs: []string{"shard-001", "shard-002", "shard-003"}}
	transport.setFailShards("shard-002")
	// The budget dies while shard-002's call is in flight: the cycle must
	// stop (no call for shard-003) but keep shard-001.
	transport.onGetShardIterator = func(shardID string) {
		if shardID == "shard-002" {
			cancel()
		}
	}

	d := newRefreshTestInput(t, transport)

	require.NoError(t, d.refreshShards(ctx),
		"a truncated-but-progressing cycle is not a failure")
	assert.ElementsMatch(t, []string{"shard-001"}, registeredShards(d),
		"shards resolved before the budget expired must be committed")
	assert.Equal(t, 2, transport.iteratorCallCount(),
		"the cycle must stop iterating once the budget is exhausted")
}

// TestRefreshTableShards_SkipsFailedShardAndCommitsRest mirrors the partial
// progress contract on the multi-table path.
func TestRefreshTableShards_SkipsFailedShardAndCommitsRest(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-001", "shard-002", "shard-003"}}
	transport.setFailShards("shard-002")

	d := &dynamoDBCDCInput{
		conf:          dynamoDBCDCConfig{startFrom: "trim_horizon"},
		log:           service.MockResources().Logger(),
		metrics:       newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient: newStubStreamsClient(transport),
	}
	ts := &tableStream{
		tableName:      "test",
		streamArn:      testStreamArn,
		checkpointer:   newTestCheckpointer(t),
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
	}
	ts.honorStartFrom.Store(true)

	require.NoError(t, d.refreshTableShards(t.Context(), "test", ts),
		"a per-shard failure must not fail the whole refresh cycle")

	ts.mu.RLock()
	ids := make([]string, 0, len(ts.shardReaders))
	for id := range ts.shardReaders {
		ids = append(ids, id)
	}
	ts.mu.RUnlock()
	assert.ElementsMatch(t, []string{"shard-001", "shard-003"}, ids,
		"healthy shards must be registered even when another shard fails")
	assert.False(t, ts.honorStartFrom.Load())
}
