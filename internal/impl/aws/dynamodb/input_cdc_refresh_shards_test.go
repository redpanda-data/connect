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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Jeffail/shutdown"

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
	mu       sync.Mutex
	shardIDs []string
	// parents maps a shard to its ParentShardId in the DescribeStream listing.
	parents    map[string]string
	failShards map[string]bool
	// trimmedShards fail AFTER_SEQUENCE_NUMBER requests with
	// TrimmedDataAccessException (a checkpoint older than the retention window).
	trimmedShards map[string]bool
	// notFoundShards fail with ResourceNotFoundException (the shard is gone).
	notFoundShards map[string]bool
	// nilIteratorShards succeed without a ShardIterator, as GetShardIterator
	// does for a closed shard with nothing left to read.
	nilIteratorShards map[string]bool
	iteratorCalls     []string
	// iteratorTypes records the ShardIteratorType of the latest
	// GetShardIterator call per shard.
	iteratorTypes      map[string]string
	onGetShardIterator func(shardID string)
	// missingDeadlines records operations whose request context carried no
	// deadline: every AWS call in a discovery cycle must be individually
	// bounded, or a single hung call consumes the whole cycle budget.
	missingDeadlines []string
}

func (s *refreshStubTransport) deadlineViolations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.missingDeadlines...)
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

func (s *refreshStubTransport) iteratorTypeFor(shardID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.iteratorTypes[shardID]
}

func (s *refreshStubTransport) iteratorCallsFor(shardID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, id := range s.iteratorCalls {
		if id == shardID {
			n++
		}
	}
	return n
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
	if _, hasDeadline := req.Context().Deadline(); !hasDeadline {
		s.mu.Lock()
		s.missingDeadlines = append(s.missingDeadlines, target)
		s.mu.Unlock()
	}
	switch {
	case strings.HasSuffix(target, ".DescribeStream"):
		s.mu.Lock()
		shards := make([]map[string]any, 0, len(s.shardIDs))
		for _, id := range s.shardIDs {
			shard := map[string]any{"ShardId": id}
			if parent := s.parents[id]; parent != "" {
				shard["ParentShardId"] = parent
			}
			shards = append(shards, shard)
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
		var in struct{ ShardId, ShardIteratorType string }
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.iteratorCalls = append(s.iteratorCalls, in.ShardId)
		if s.iteratorTypes == nil {
			s.iteratorTypes = map[string]string{}
		}
		s.iteratorTypes[in.ShardId] = in.ShardIteratorType
		fail := s.failShards[in.ShardId]
		trimmed := s.trimmedShards[in.ShardId] && in.ShardIteratorType == "AFTER_SEQUENCE_NUMBER"
		notFound := s.notFoundShards[in.ShardId]
		nilIterator := s.nilIteratorShards[in.ShardId]
		hook := s.onGetShardIterator
		s.mu.Unlock()
		if hook != nil {
			hook(in.ShardId)
		}
		if trimmed {
			resp := jsonResponse(req, 400, `{"__type":"TrimmedDataAccessException","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "TrimmedDataAccessException")
			return resp, nil
		}
		if notFound {
			resp := jsonResponse(req, 400, `{"__type":"ResourceNotFoundException","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "ResourceNotFoundException")
			return resp, nil
		}
		if fail {
			resp := jsonResponse(req, 400, `{"__type":"LimitExceededException","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "LimitExceededException")
			return resp, nil
		}
		if nilIterator {
			return jsonResponse(req, 200, `{}`), nil
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

	assert.Empty(t, transport.deadlineViolations(),
		"every AWS call in a discovery cycle must carry its own deadline, or one hung call consumes the whole cycle budget")
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

// TestRefreshShardsCompletedShardNotRediscoveredAfterCleanup (CON-599):
// DescribeStream keeps listing closed shards for the 24h retention window.
// Once a reader has read a closed shard to the end and the cleanup ticker has
// pruned it, the next discovery cycle must not register it again: doing so
// re-read the shard from its (lagging) checkpoint or TRIM_HORIZON every
// cleanup cycle, redelivering its tail and churning thousands of
// GetItem/GetShardIterator/GetRecords calls.
func TestRefreshShardsCompletedShardNotRediscoveredAfterCleanup(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-closed", "shard-open"}}
	d := newRefreshTestInput(t, transport)

	require.NoError(t, d.refreshShards(t.Context()))
	require.ElementsMatch(t, []string{"shard-closed", "shard-open"}, registeredShards(d))
	require.Equal(t, 2, transport.iteratorCallCount())

	// The reader reaches the end of the closed shard (NextShardIterator == nil).
	d.mu.Lock()
	d.shardReaders["shard-closed"].iterator = nil
	d.mu.Unlock()
	d.completeShard("shard-closed")

	// The cleanup ticker prunes the exhausted reader.
	d.cleanupExhaustedShards(map[string]context.CancelFunc{})
	require.ElementsMatch(t, []string{"shard-open"}, registeredShards(d))

	// The refresh ticker: DescribeStream still lists the closed shard.
	require.NoError(t, d.refreshShards(t.Context()))
	assert.ElementsMatch(t, []string{"shard-open"}, registeredShards(d),
		"a closed shard already read to the end must not be re-registered")
	assert.Equal(t, 2, transport.iteratorCallCount(),
		"no new iterator may be acquired for a completed shard")

	// Once the shard ages out of the stream listing it is forgotten, so the
	// completed set stays bounded to shards still in retention.
	transport.mu.Lock()
	transport.shardIDs = []string{"shard-open"}
	transport.mu.Unlock()
	require.NoError(t, d.refreshShards(t.Context()))
	d.mu.RLock()
	assert.Empty(t, d.completedShards, "completed shards no longer listed must be pruned")
	d.mu.RUnlock()
}

// TestRefreshTableShardsCompletedShardNotRediscoveredAfterCleanup mirrors
// the CON-599 contract on the multi-table path.
func TestRefreshTableShardsCompletedShardNotRediscoveredAfterCleanup(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-closed", "shard-open"}}
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
	tableShards := func() []string {
		ts.mu.RLock()
		defer ts.mu.RUnlock()
		ids := make([]string, 0, len(ts.shardReaders))
		for id := range ts.shardReaders {
			ids = append(ids, id)
		}
		return ids
	}

	require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
	require.ElementsMatch(t, []string{"shard-closed", "shard-open"}, tableShards())
	require.Equal(t, 2, transport.iteratorCallCount())

	ts.mu.Lock()
	ts.shardReaders["shard-closed"].iterator = nil
	ts.mu.Unlock()
	d.completeTableShard("test", ts, "shard-closed")

	d.cleanupTableExhaustedShards("test", ts, map[string]context.CancelFunc{})
	require.ElementsMatch(t, []string{"shard-open"}, tableShards())

	require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
	assert.ElementsMatch(t, []string{"shard-open"}, tableShards(),
		"a closed shard already read to the end must not be re-registered")
	assert.Equal(t, 2, transport.iteratorCallCount(),
		"no new iterator may be acquired for a completed shard")

	transport.mu.Lock()
	transport.shardIDs = []string{"shard-open"}
	transport.mu.Unlock()
	require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
	ts.mu.RLock()
	assert.Empty(t, ts.completedShards, "completed shards no longer listed must be pruned")
	ts.mu.RUnlock()
}

// tableRefreshHarness builds a multi-table input and table stream over
// transport, plus a helper listing the table's registered shards.
func tableRefreshHarness(t *testing.T, transport *refreshStubTransport, cp *Checkpointer) (*dynamoDBCDCInput, *tableStream, func() []string) {
	t.Helper()
	d := &dynamoDBCDCInput{
		conf:          dynamoDBCDCConfig{startFrom: "trim_horizon"},
		log:           service.MockResources().Logger(),
		metrics:       newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient: newStubStreamsClient(transport),
	}
	ts := &tableStream{
		tableName:      "test",
		streamArn:      testStreamArn,
		checkpointer:   cp,
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
	}
	return d, ts, func() []string {
		ts.mu.RLock()
		defer ts.mu.RUnlock()
		ids := make([]string, 0, len(ts.shardReaders))
		for id := range ts.shardReaders {
			ids = append(ids, id)
		}
		return ids
	}
}

// TestRefreshShardsChildHeldBackUntilParentCompleted (CON-599): a child shard
// must not be read while its parent is still listed and unfinished, or an
// item's newer updates (child) overtake its older ones (parent backlog). The
// child is registered on the first refresh after the parent completes, and a
// parent that aged out of the listing does not hold its child back.
func TestRefreshShardsChildHeldBackUntilParentCompleted(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs: []string{"shard-child", "shard-parent", "shard-orphan"},
			parents:  map[string]string{"shard-child": "shard-parent", "shard-orphan": "shard-aged-out"},
		}
	}

	t.Run("single-table", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)

		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-orphan"}, registeredShards(d),
			"the child must wait for its listed parent; a child of an unlisted parent must not")
		assert.Zero(t, transport.iteratorCallsFor("shard-child"))

		d.completeShard("shard-parent")
		select {
		case <-d.shardRefreshCh:
		default:
			t.Fatal("completing a parent must trigger an immediate refresh for its children")
		}
		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child", "shard-orphan"}, registeredShards(d))
	})

	t.Run("multi-table", func(t *testing.T) {
		transport := newTransport()
		d, ts, tableShards := tableRefreshHarness(t, transport, newTestCheckpointer(t))

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-orphan"}, tableShards())

		d.completeTableShard("test", ts, "shard-parent")
		select {
		case <-ts.shardRefreshCh:
		default:
			t.Fatal("completing a parent must trigger an immediate refresh for its children")
		}
		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child", "shard-orphan"}, tableShards())
	})
}

// TestRefreshShardsLatestDoesNotHoldBackChildren (CON-599): on a fresh
// pipeline with start_from: latest every shard starts at LATEST, so a closed
// parent has no backlog to drain and parent-before-child ordering must not
// apply. Holding the child back would clear honorStartFrom after the parents
// commit and start the child at TRIM_HORIZON on the next cycle, replaying up
// to 24h of records the user asked to skip.
func TestRefreshShardsLatestDoesNotHoldBackChildren(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs: []string{"shard-child", "shard-parent"},
			parents:  map[string]string{"shard-child": "shard-parent"},
		}
	}
	assertLatest := func(t *testing.T, transport *refreshStubTransport) {
		t.Helper()
		for _, id := range []string{"shard-parent", "shard-child"} {
			assert.Equal(t, "LATEST", transport.iteratorTypeFor(id), "shard %s", id)
		}
	}

	t.Run("single-table", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)
		d.conf.startFrom = "latest"
		d.honorStartFrom.Store(true)

		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, registeredShards(d))
		assertLatest(t, transport)
		assert.False(t, d.honorStartFrom.Load())
	})

	t.Run("multi-table", func(t *testing.T) {
		transport := newTransport()
		d, ts, tableShards := tableRefreshHarness(t, transport, newTestCheckpointer(t))
		d.conf.startFrom = "latest"
		ts.honorStartFrom.Store(true)

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, tableShards())
		assertLatest(t, transport)
		assert.False(t, ts.honorStartFrom.Load())
	})

	t.Run("restart with state still orders lineage", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)
		d.conf.startFrom = "latest"

		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent"}, registeredShards(d))
		assert.Equal(t, "TRIM_HORIZON", transport.iteratorTypeFor("shard-parent"))
	})
}

// completedMarkerCheckpointer returns a checkpointer whose store marks the
// given shards completed (as an earlier run's MarkShardCompleted would have).
func completedMarkerCheckpointer(t *testing.T, completed ...string) *Checkpointer {
	t.Helper()
	cp := newTestCheckpointer(t)
	marked := map[string]bool{}
	for _, id := range completed {
		marked[id] = true
	}
	cp.svc = &fakeCheckpointAPI{
		getItem: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			shard := in.Key[checkpointRangeKey].(*dynamodbtypes.AttributeValueMemberS).Value
			if !marked[shard] {
				return &dynamodb.GetItemOutput{}, nil
			}
			return &dynamodb.GetItemOutput{Item: map[string]dynamodbtypes.AttributeValue{
				"SequenceNumber":             &dynamodbtypes.AttributeValueMemberS{Value: "00009"},
				checkpointShardCompletedAttr: &dynamodbtypes.AttributeValueMemberBOOL{Value: true},
			}}, nil
		},
	}
	return cp
}

// TestRefreshShardsSkipsShardMarkedCompleted (CON-599): after a restart, a
// closed shard the previous run read to the end (completion marker on its
// checkpoint row) must not be read again: no iterator is acquired, it is
// recorded as completed, and its child is released in the same cycle.
func TestRefreshShardsSkipsShardMarkedCompleted(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			// The child is listed first so releasing it needs a second pass.
			shardIDs: []string{"shard-child", "shard-closed"},
			parents:  map[string]string{"shard-child": "shard-closed"},
		}
	}

	t.Run("single-table", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)
		d.checkpointer = completedMarkerCheckpointer(t, "shard-closed")

		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-child"}, registeredShards(d))
		assert.Zero(t, transport.iteratorCallsFor("shard-closed"), "no iterator may be acquired for a shard marked completed")
		d.mu.RLock()
		assert.Contains(t, d.completedShards, "shard-closed")
		d.mu.RUnlock()

		require.NoError(t, d.refreshShards(t.Context()))
		assert.Zero(t, transport.iteratorCallsFor("shard-closed"))
		assert.Equal(t, 1, transport.iteratorCallsFor("shard-child"))
	})

	t.Run("multi-table", func(t *testing.T) {
		transport := newTransport()
		d, ts, tableShards := tableRefreshHarness(t, transport, completedMarkerCheckpointer(t, "shard-closed"))

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-child"}, tableShards())
		assert.Zero(t, transport.iteratorCallsFor("shard-closed"))
		ts.mu.RLock()
		assert.Contains(t, ts.completedShards, "shard-closed")
		ts.mu.RUnlock()
	})
}

// TestRefreshShardsForgetsAgedOutShardTrackerState: when a completed shard
// ages out of the listing, any batcher state it left behind (its final write
// failed) is dropped with it.
func TestRefreshShardsForgetsAgedOutShardTrackerState(t *testing.T) {
	transport := &refreshStubTransport{shardIDs: []string{"shard-closed"}}
	d := newRefreshTestInput(t, transport)
	cp := &mockCheckpointer{checkpointLimit: 1000, completeErr: errors.New("boom")}
	d.recordBatcher = NewRecordBatcher(100, 1000, cp, nil, service.MockResources().Logger())

	require.NoError(t, d.refreshShards(t.Context()))
	tb := d.recordBatcher.AddMessages(createTestMessages(2, "shard-closed", 1), "shard-closed")
	d.recordBatcher.CloseShard("shard-closed")
	require.NoError(t, d.recordBatcher.AckBatch(tb))
	require.Error(t, d.recordBatcher.persist(t.Context(), false), "the completion write fails")
	d.mu.Lock()
	markShardCompleted(d.shardReaders, &d.completedShards, "shard-closed")
	d.mu.Unlock()
	require.Equal(t, 1, d.recordBatcher.TrackedShardCount())

	transport.mu.Lock()
	transport.shardIDs = nil
	transport.mu.Unlock()
	require.NoError(t, d.refreshShards(t.Context()))
	assert.Zero(t, d.recordBatcher.TrackedShardCount())
	persistNow(t, d.recordBatcher)
	assert.Zero(t, cp.calls(), "a forgotten shard is never written")
}

// TestUpdateActiveShardsMetricSumsTables: each table coordinator publishes
// its own active count and the shards_active gauge reports the sum, instead
// of whichever coordinator set it last.
func TestUpdateActiveShardsMetricSumsTables(t *testing.T) {
	a, b := &tableStream{tableName: "a"}, &tableStream{tableName: "b"}
	a.activeShards.Store(2)
	b.activeShards.Store(3)
	d := &dynamoDBCDCInput{
		metrics:      newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		tableStreams: map[string]*tableStream{"a": a, "b": b},
	}
	assert.Equal(t, int64(5), d.totalActiveShards())
	d.updateActiveShardsMetric() // must not panic with real gauges

	b.activeShards.Store(0)
	assert.Equal(t, int64(2), d.totalActiveShards())
}

// coordinatorStubTransport fakes the whole DynamoDB Streams read path for a
// coordinator: DescribeStream lists the shards, GetShardIterator returns
// "iter-<shard>", and GetRecords serves each closed shard's records once with
// no NextShardIterator (its final page) and every open shard an empty page.
// Calls are logged in order as "<op>:<shard>".
type coordinatorStubTransport struct {
	mu      sync.Mutex
	shards  []string
	parents map[string]string
	closed  map[string]string // closed shard -> its records page
	calls   []string
}

func (s *coordinatorStubTransport) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *coordinatorStubTransport) count(call string) int {
	n := 0
	for _, c := range s.log() {
		if c == call {
			n++
		}
	}
	return n
}

func (s *coordinatorStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var in struct{ ShardId, ShardIterator string }
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.HasSuffix(target, ".DescribeStream"):
		shards := make([]map[string]any, 0, len(s.shards))
		for _, id := range s.shards {
			shard := map[string]any{"ShardId": id}
			if parent := s.parents[id]; parent != "" {
				shard["ParentShardId"] = parent
			}
			shards = append(shards, shard)
		}
		desc, err := json.Marshal(map[string]any{"StreamDescription": map[string]any{
			"StreamArn": testStreamArn, "StreamStatus": "ENABLED", "Shards": shards,
		}})
		if err != nil {
			return nil, err
		}
		return jsonResponse(req, 200, string(desc)), nil

	case strings.HasSuffix(target, ".GetShardIterator"):
		s.calls = append(s.calls, "GetShardIterator:"+in.ShardId)
		return jsonResponse(req, 200, fmt.Sprintf(`{"ShardIterator":"iter-%s"}`, in.ShardId)), nil

	case strings.HasSuffix(target, ".GetRecords"):
		shardID := strings.TrimPrefix(in.ShardIterator, "iter-")
		s.calls = append(s.calls, "GetRecords:"+shardID)
		if records, isClosed := s.closed[shardID]; isClosed {
			return jsonResponse(req, 200, fmt.Sprintf(`{"Records":%s}`, records)), nil
		}
		return jsonResponse(req, 200, fmt.Sprintf(`{"Records":[],"NextShardIterator":"iter-%s"}`, shardID)), nil

	default:
		return nil, fmt.Errorf("coordinatorStubTransport: unexpected operation %q", target)
	}
}

// lockedStoreCheckpointer returns a checkpointer over an in-memory store. The
// store is written from ack goroutines and read by the test, so access to the
// shared fake is serialised.
func lockedStoreCheckpointer(t *testing.T) *Checkpointer {
	t.Helper()
	var storeMu sync.Mutex
	api := sharedStoreAPI(map[string]map[string]dynamodbtypes.AttributeValue{})
	putItem, updateItem, getItem := api.putItem, api.updateItem, api.getItem
	api.updateItem = func(ctx context.Context, in *dynamodb.UpdateItemInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
		storeMu.Lock()
		defer storeMu.Unlock()
		return updateItem(ctx, in, o...)
	}
	api.putItem = func(ctx context.Context, in *dynamodb.PutItemInput, o ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
		storeMu.Lock()
		defer storeMu.Unlock()
		return putItem(ctx, in, o...)
	}
	api.getItem = func(ctx context.Context, in *dynamodb.GetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
		storeMu.Lock()
		defer storeMu.Unlock()
		return getItem(ctx, in, o...)
	}
	cp, err := NewCheckpointer(t.Context(), api, CheckpointerConfig{
		TableName: "cps", SourceTable: "test", StreamArn: testStreamArn, CheckpointLimit: 1000,
	}, service.MockResources().Logger())
	require.NoError(t, err)
	return cp
}

// TestShardCoordinatorReadsClosedShardOnce (CON-599) drives the real
// single-table coordinator with millisecond refresh and cleanup intervals,
// so the reader itself (not the test) reaches the end of the closed shard,
// and many refresh/cleanup cycles run. The closed parent must be read exactly
// once, its child must start only after the parent's final page, the
// completion must be persisted once the final batch is acked, and no tracker
// state may be left behind.
func TestShardCoordinatorReadsClosedShardOnce(t *testing.T) {
	transport := &coordinatorStubTransport{
		shards:  []string{"shard-parent", "shard-child"},
		parents: map[string]string{"shard-child": "shard-parent"},
		closed:  map[string]string{"shard-parent": twoRecordsPage},
	}
	cp := lockedStoreCheckpointer(t)

	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			startFrom:       "trim_horizon",
			batchSize:       10,
			pollInterval:    2 * time.Millisecond,
			throttleBackoff: 2 * time.Millisecond,
		},
		log:             service.MockResources().Logger(),
		metrics:         newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient:   newStubStreamsClient(transport),
		streamArn:       aws.String(testStreamArn),
		checkpointer:    cp,
		recordBatcher:   NewRecordBatcher(100, 1000, cp, nil, service.MockResources().Logger()),
		shardReaders:    map[string]*dynamoDBShardReader{},
		shardRefreshCh:  make(chan struct{}, 1),
		msgChan:         make(chan asyncMessage, 10),
		shutSig:         shutdown.NewSignaller(),
		refreshInterval: 3 * time.Millisecond,
		cleanupInterval: 5 * time.Millisecond,
	}
	require.NoError(t, d.refreshShards(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	coordinatorDone := make(chan struct{})
	go func() {
		d.startShardCoordinator(ctx)
		close(coordinatorDone)
	}()
	// In production Connect starts the persister next to the coordinator.
	runPersister(t, d.recordBatcher)
	defer func() {
		cancel()
		<-coordinatorDone
	}()

	var am asyncMessage
	select {
	case am = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the closed shard's final batch")
	}
	require.Len(t, am.msg, 2)
	require.NoError(t, am.ackFn(t.Context(), nil))

	completed := func() bool {
		_, done, err := cp.getShardRow(t.Context(), "shard-parent")
		return err == nil && done
	}
	require.Eventually(t, completed, 5*time.Second, time.Millisecond,
		"acking the closed shard's final batch must persist its completion")

	// Let many refresh and cleanup cycles run.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, transport.count("GetShardIterator:shard-parent"), "a completed shard must not be re-read after cleanup")
	assert.Equal(t, 1, transport.count("GetRecords:shard-parent"))
	assert.Equal(t, 1, transport.count("GetShardIterator:shard-child"), "the child must start exactly once")
	callLog := transport.log()
	assert.Less(t, slices.Index(callLog, "GetRecords:shard-parent"), slices.Index(callLog, "GetShardIterator:shard-child"),
		"the child must not start before the parent's final page was read")
	assert.Zero(t, d.recordBatcher.TrackedShardCount(), "the completed shard's tracker state must be forgotten")
	assert.Empty(t, d.msgChan, "no batch may be redelivered")
}

// checkpointedCheckpointer returns a checkpointer whose store holds an exact
// resume sequence number for each of the given shards.
func checkpointedCheckpointer(t *testing.T, seqs map[string]string) *Checkpointer {
	t.Helper()
	cp := newTestCheckpointer(t)
	cp.svc = &fakeCheckpointAPI{
		getItem: func(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
			shard := in.Key[checkpointRangeKey].(*dynamodbtypes.AttributeValueMemberS).Value
			seq, ok := seqs[shard]
			if !ok {
				return &dynamodb.GetItemOutput{}, nil
			}
			return &dynamodb.GetItemOutput{Item: map[string]dynamodbtypes.AttributeValue{
				"SequenceNumber": &dynamodbtypes.AttributeValueMemberS{Value: seq},
			}}, nil
		},
	}
	return cp
}

// TestRefreshShardsTrimmedParentCheckpointDoesNotStallChildren (CON-599): in
// CDC-only mode a pipeline restarted after more than 24h down holds a
// checkpoint the stream has trimmed, so GetShardIterator AFTER_SEQUENCE_NUMBER
// fails with TrimmedDataAccessException on every cycle. The parent must be
// resumed from TRIM_HORIZON instead of failing forever: otherwise its whole
// descendant chain is held back until it ages out of the listing, and a
// single-lineage table ends up with no readers at all.
func TestRefreshShardsTrimmedParentCheckpointDoesNotStallChildren(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs:      []string{"shard-child", "shard-parent"},
			parents:       map[string]string{"shard-child": "shard-parent"},
			trimmedShards: map[string]bool{"shard-parent": true},
		}
	}
	newCheckpointer := func(t *testing.T) *Checkpointer {
		return checkpointedCheckpointer(t, map[string]string{"shard-parent": "00001"})
	}
	assertParentResumedFromTrimHorizon := func(t *testing.T, transport *refreshStubTransport, registered []string) {
		t.Helper()
		assert.ElementsMatch(t, []string{"shard-parent"}, registered,
			"the parent with a trimmed checkpoint must get a reader; the child still waits for it")
		assert.Equal(t, "TRIM_HORIZON", transport.iteratorTypeFor("shard-parent"))
		assert.Equal(t, 2, transport.iteratorCallsFor("shard-parent"),
			"one AFTER_SEQUENCE_NUMBER attempt, then one TRIM_HORIZON fallback")
		assert.Zero(t, transport.iteratorCallsFor("shard-child"))
	}

	t.Run("single-table", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)
		d.checkpointer = newCheckpointer(t)

		require.NoError(t, d.refreshShards(t.Context()))
		assertParentResumedFromTrimHorizon(t, transport, registeredShards(d))

		d.completeShard("shard-parent")
		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, registeredShards(d),
			"the child must be released once the parent's retained tail is read")
	})

	t.Run("multi-table", func(t *testing.T) {
		transport := newTransport()
		d, ts, tableShards := tableRefreshHarness(t, transport, newCheckpointer(t))

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assertParentResumedFromTrimHorizon(t, transport, tableShards())

		d.completeTableShard("test", ts, "shard-parent")
		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, tableShards())
	})
}

// TestShardCoordinatorReadsClosedShardOnceMultiTable is the multi-table form
// of TestShardCoordinatorReadsClosedShardOnce, driving the real table stream
// coordinator.
func TestShardCoordinatorReadsClosedShardOnceMultiTable(t *testing.T) {
	transport := &coordinatorStubTransport{
		shards:  []string{"shard-parent", "shard-child"},
		parents: map[string]string{"shard-child": "shard-parent"},
		closed:  map[string]string{"shard-parent": twoRecordsPage},
	}
	cp := lockedStoreCheckpointer(t)

	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			startFrom:       "trim_horizon",
			batchSize:       10,
			pollInterval:    2 * time.Millisecond,
			throttleBackoff: 2 * time.Millisecond,
		},
		log:             service.MockResources().Logger(),
		metrics:         newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		streamsClient:   newStubStreamsClient(transport),
		msgChan:         make(chan asyncMessage, 10),
		shutSig:         shutdown.NewSignaller(),
		refreshInterval: 3 * time.Millisecond,
		cleanupInterval: 5 * time.Millisecond,
	}
	ts := &tableStream{
		tableName:      "test",
		streamArn:      testStreamArn,
		checkpointer:   cp,
		recordBatcher:  NewRecordBatcher(100, 1000, cp, nil, service.MockResources().Logger()),
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
	}
	d.tableStreams = map[string]*tableStream{"test": ts}

	ctx, cancel := context.WithCancel(t.Context())
	coordinatorDone := make(chan struct{})
	go func() {
		d.startTableStreamCoordinator(ctx, "test", ts)
		close(coordinatorDone)
	}()
	// In production startTableCoordinator starts the persister next to the
	// coordinator.
	runPersister(t, ts.recordBatcher)
	defer func() {
		cancel()
		<-coordinatorDone
	}()

	var am asyncMessage
	select {
	case am = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the closed shard's final batch")
	}
	require.Len(t, am.msg, 2)
	require.NoError(t, am.ackFn(t.Context(), nil))

	require.Eventually(t, func() bool {
		_, done, err := cp.getShardRow(t.Context(), "shard-parent")
		return err == nil && done
	}, 5*time.Second, time.Millisecond, "acking the closed shard's final batch must persist its completion")

	// Let many refresh and cleanup cycles run.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 1, transport.count("GetShardIterator:shard-parent"), "a completed shard must not be re-read after cleanup")
	assert.Equal(t, 1, transport.count("GetRecords:shard-parent"))
	assert.Equal(t, 1, transport.count("GetShardIterator:shard-child"), "the child must start exactly once")
	callLog := transport.log()
	assert.Less(t, slices.Index(callLog, "GetRecords:shard-parent"), slices.Index(callLog, "GetShardIterator:shard-child"),
		"the child must not start before the parent's final page was read")
	assert.Zero(t, ts.recordBatcher.TrackedShardCount(), "the completed shard's tracker state must be forgotten")
	assert.Empty(t, d.msgChan, "no batch may be redelivered")
}

// TestSyncShardReadersRestartsReregisteredReader: a reader that gave up
// (unregistering itself) and was re-registered by discovery under the same
// shard ID before the coordinator noticed is a new reader. Its stale
// goroutine must be canceled and a fresh one started for the new reader.
func TestSyncShardReadersRestartsReregisteredReader(t *testing.T) {
	oldReader := &dynamoDBShardReader{shardID: "shard-1"}
	newReader := &dynamoDBShardReader{shardID: "shard-1"}

	oldCtx, oldCancel := context.WithCancel(t.Context())
	defer oldCancel()
	activeShards := map[string]context.CancelFunc{"shard-1": oldCancel}
	startedFor := map[string]*dynamoDBShardReader{"shard-1": oldReader}
	readers := map[string]shardReaderState{"shard-1": {reader: newReader}}

	var started []string
	var newCtx context.Context
	syncShardReaders(t.Context(), readers, activeShards, startedFor, func(shardCtx context.Context, shardID string) {
		started = append(started, shardID)
		newCtx = shardCtx
	})

	assert.ErrorIs(t, oldCtx.Err(), context.Canceled, "the old reader's goroutine must be canceled")
	assert.Equal(t, []string{"shard-1"}, started, "a goroutine must be started for the re-registered reader")
	require.NotNil(t, newCtx)
	assert.NoError(t, newCtx.Err(), "the new goroutine's context must be live")
	assert.Same(t, newReader, startedFor["shard-1"])
	assert.Contains(t, activeShards, "shard-1")

	// The next sync sees the same reader and leaves its goroutine alone.
	syncShardReaders(t.Context(), readers, activeShards, startedFor, func(context.Context, string) {
		t.Fatal("a running reader must not be restarted")
	})
	assert.NoError(t, newCtx.Err())
}

// TestRefreshShardsGoneParentReleasesChildren (CON-599): a listed parent
// whose GetShardIterator reports ResourceNotFoundException can never be read,
// so it must not hold its children back. It is not recorded as completed:
// nothing was read from it.
func TestRefreshShardsGoneParentReleasesChildren(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs:       []string{"shard-child", "shard-parent"},
			parents:        map[string]string{"shard-child": "shard-parent"},
			notFoundShards: map[string]bool{"shard-parent": true},
		}
	}

	t.Run("single-table", func(t *testing.T) {
		d := newRefreshTestInput(t, newTransport())

		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-child"}, registeredShards(d))
		assert.False(t, lookupShard(d.shardReaders, d.completedShards, "shard-parent").completed,
			"a gone parent must not be marked completed")
	})

	t.Run("multi-table", func(t *testing.T) {
		d, ts, tableShards := tableRefreshHarness(t, newTransport(), newTestCheckpointer(t))

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-child"}, tableShards())
		assert.False(t, lookupShard(ts.shardReaders, ts.completedShards, "shard-parent").completed)
	})
}

// TestRefreshShardsFailingParentReleasesChildrenAfterRetries (CON-599): a
// listed parent that keeps failing to prepare (for a reason other than
// trimmed data or a missing shard) holds its children back only for
// parentReleaseAfterFailures consecutive refresh cycles; after that the
// children are read anyway so the lineage cannot stall until the parent ages
// out of the listing.
func TestRefreshShardsFailingParentReleasesChildrenAfterRetries(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		transport := &refreshStubTransport{
			shardIDs: []string{"shard-child", "shard-parent"},
			parents:  map[string]string{"shard-child": "shard-parent"},
		}
		transport.setFailShards("shard-parent")
		return transport
	}
	run := func(t *testing.T, refresh func() error, registered func() []string, transport *refreshStubTransport) {
		t.Helper()
		for cycle := 1; cycle < parentReleaseAfterFailures; cycle++ {
			require.NoError(t, refresh())
			assert.Empty(t, registered(), "cycle %d: the child must still wait for its parent", cycle)
		}
		require.NoError(t, refresh())
		assert.ElementsMatch(t, []string{"shard-child"}, registered(),
			"the child must be released after %d failed parent prepares", parentReleaseAfterFailures)
		assert.Equal(t, parentReleaseAfterFailures, transport.iteratorCallsFor("shard-parent"))

		// The parent is still retried, and read once it recovers.
		transport.setFailShards()
		require.NoError(t, refresh())
		assert.ElementsMatch(t, []string{"shard-child", "shard-parent"}, registered())
	}

	t.Run("single-table", func(t *testing.T) {
		transport := newTransport()
		d := newRefreshTestInput(t, transport)
		run(t, func() error { return d.refreshShards(t.Context()) }, func() []string { return registeredShards(d) }, transport)
	})

	t.Run("multi-table", func(t *testing.T) {
		transport := newTransport()
		d, ts, tableShards := tableRefreshHarness(t, transport, newTestCheckpointer(t))
		run(t, func() error { return d.refreshTableShards(t.Context(), "test", ts) }, tableShards, transport)
	})
}

// TestRefreshShardsWarnsOnLongChildWait (CON-599): a parent that prepares but
// never finishes (its reader keeps failing) is never released, so its child
// would wait silently until the parent ages out. A warning must be logged
// once the child has waited childWaitWarnAfterCycles refresh cycles, and the
// count must reset once the child is read.
func TestRefreshShardsWarnsOnLongChildWait(t *testing.T) {
	const warning = "refresh cycles for parent shard shard-parent"
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs: []string{"shard-child", "shard-parent"},
			parents:  map[string]string{"shard-child": "shard-parent"},
		}
	}
	run := func(t *testing.T, d *dynamoDBCDCInput, parents *parentPrepareFailures, refresh func() error, complete func(), registered func() []string) {
		t.Helper()
		var (
			mu     sync.Mutex
			levels []slog.Level
		)
		d.log = service.NewLoggerFromSlog(slog.New(levelRecorder{substr: warning, mu: &mu, levels: &levels}))
		warnings := func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(levels)
		}

		for cycle := 1; cycle < childWaitWarnAfterCycles; cycle++ {
			require.NoError(t, refresh())
		}
		assert.ElementsMatch(t, []string{"shard-parent"}, registered(), "the child must still wait for its parent")
		assert.Zero(t, warnings(), "no warning before childWaitWarnAfterCycles cycles")

		require.NoError(t, refresh())
		require.Equal(t, 1, warnings(), "a warning must be logged after %d cycles of waiting", childWaitWarnAfterCycles)
		mu.Lock()
		assert.Equal(t, slog.LevelWarn, levels[0])
		mu.Unlock()

		complete()
		require.NoError(t, refresh())
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, registered())
		parents.mu.Lock()
		defer parents.mu.Unlock()
		assert.NotContains(t, parents.waits, "shard-child", "a child that is read must not keep its wait count")
	}

	t.Run("single-table", func(t *testing.T) {
		d := newRefreshTestInput(t, newTransport())
		run(t, d, &d.parentFailures,
			func() error { return d.refreshShards(t.Context()) },
			func() { d.completeShard("shard-parent") },
			func() []string { return registeredShards(d) })
	})

	t.Run("multi-table", func(t *testing.T) {
		d, ts, tableShards := tableRefreshHarness(t, newTransport(), newTestCheckpointer(t))
		run(t, d, &ts.parentFailures,
			func() error { return d.refreshTableShards(t.Context(), "test", ts) },
			func() { d.completeTableShard("test", ts, "shard-parent") },
			tableShards)
	})
}

// markRecordingCheckpointer returns a checkpointer over an empty store that
// records the shards MarkShardCompleted is called for, failing each call with
// markErr(ctx) when set.
func markRecordingCheckpointer(t *testing.T, markErr func(context.Context) error) (*Checkpointer, func() []string) {
	t.Helper()
	var (
		mu     sync.Mutex
		marked []string
	)
	cp := newTestCheckpointer(t)
	cp.svc.(*fakeCheckpointAPI).updateItem = func(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
		if markErr != nil {
			if err := markErr(ctx); err != nil {
				return nil, err
			}
		}
		mu.Lock()
		defer mu.Unlock()
		marked = append(marked, in.Key[checkpointRangeKey].(*dynamodbtypes.AttributeValueMemberS).Value)
		return &dynamodb.UpdateItemOutput{}, nil
	}
	return cp, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), marked...)
	}
}

// TestShardReaderNilIteratorCompletesClosedShard (CON-599): GetShardIterator
// returns no iterator for a closed shard with nothing left to read. The
// reader must take the closed-shard path (completion marker, child release)
// instead of returning silently and leaving the shard registered but unread,
// which would hold its children back until the shard ages out.
func TestShardReaderNilIteratorCompletesClosedShard(t *testing.T) {
	newTransport := func() *refreshStubTransport {
		return &refreshStubTransport{
			shardIDs:          []string{"shard-child", "shard-parent"},
			parents:           map[string]string{"shard-child": "shard-parent"},
			nilIteratorShards: map[string]bool{"shard-parent": true},
		}
	}

	t.Run("single-table", func(t *testing.T) {
		d := newRefreshTestInput(t, newTransport())
		d.conf.batchSize = 10
		cp, marked := markRecordingCheckpointer(t, nil)
		d.checkpointer = cp
		d.recordBatcher = NewRecordBatcher(100, 1000, cp, nil, service.MockResources().Logger())

		require.NoError(t, d.refreshShards(t.Context()))
		require.ElementsMatch(t, []string{"shard-parent"}, registeredShards(d))

		d.startShardReader(t.Context(), "shard-parent")

		persistNow(t, d.recordBatcher)
		assert.Equal(t, []string{"shard-parent"}, marked(), "the closed shard must be marked completed")
		assert.True(t, lookupShard(d.shardReaders, d.completedShards, "shard-parent").completed)
		select {
		case <-d.shardRefreshCh:
		default:
			t.Fatal("completing the shard must trigger an immediate refresh for its children")
		}
		require.NoError(t, d.refreshShards(t.Context()))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, registeredShards(d))
	})

	t.Run("multi-table", func(t *testing.T) {
		cp, marked := markRecordingCheckpointer(t, nil)
		d, ts, tableShards := tableRefreshHarness(t, newTransport(), cp)
		d.conf.batchSize = 10
		ts.recordBatcher = NewRecordBatcher(100, 1000, cp, nil, service.MockResources().Logger())

		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		require.ElementsMatch(t, []string{"shard-parent"}, tableShards())

		d.startTableShardReader(t.Context(), "test", ts, "shard-parent")

		persistNow(t, ts.recordBatcher)
		assert.Equal(t, []string{"shard-parent"}, marked())
		assert.True(t, lookupShard(ts.shardReaders, ts.completedShards, "shard-parent").completed)
		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child"}, tableShards())
	})
}

// countingMetricsExporter is a metrics exporter recording counter totals by
// name, so tests can observe the input's counters.
type countingMetricsExporter struct {
	mu       sync.Mutex
	counters map[string]int64
}

type countingMetricsCounter struct {
	e    *countingMetricsExporter
	name string
}

func (c countingMetricsCounter) Incr(count int64) { c.e.add(c.name, count) }

func (c countingMetricsCounter) IncrFloat64(count float64) { c.Incr(int64(count)) }

type noopMetricsTimer struct{}

func (noopMetricsTimer) Timing(int64) {}

type noopMetricsGauge struct{}

func (noopMetricsGauge) Set(int64)           {}
func (noopMetricsGauge) SetFloat64(float64)  {}
func (noopMetricsGauge) IncrFloat64(float64) {}
func (noopMetricsGauge) DecrFloat64(float64) {}

func (e *countingMetricsExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(...string) service.MetricsExporterCounter { return countingMetricsCounter{e: e, name: name} }
}

func (*countingMetricsExporter) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopMetricsTimer{} }
}

func (*countingMetricsExporter) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return noopMetricsGauge{} }
}

func (*countingMetricsExporter) Close(context.Context) error { return nil }

func (e *countingMetricsExporter) add(name string, count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.counters[name] += count
}

func (e *countingMetricsExporter) counter(name string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counters[name]
}

// newCountingMetrics returns service metrics backed by a counting exporter.
func newCountingMetrics(t *testing.T) (*service.Metrics, *countingMetricsExporter) {
	t.Helper()
	exporter := &countingMetricsExporter{counters: map[string]int64{}}
	env := service.NewEmptyEnvironment()
	require.NoError(t, env.RegisterMetricsExporter("counting", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return exporter, nil }))
	// The resource builder defaults to the "none" tracer, which lives in a
	// plugin package this test binary does not import.
	require.NoError(t, env.RegisterOtelTracerProvider("none", service.NewConfigSpec(),
		func(*service.ParsedConfig) (trace.TracerProvider, error) { return noop.NewTracerProvider(), nil }))
	builder := env.NewResourceBuilder()
	require.NoError(t, builder.SetMetricsYAML("counting: {}"))
	res, closeFn, err := builder.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn(context.Background()) })
	return res.Metrics(), exporter
}

// levelRecorder is a slog handler recording the level of every message that
// mentions substr.
type levelRecorder struct {
	substr string
	mu     *sync.Mutex
	levels *[]slog.Level
}

func (levelRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (h levelRecorder) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.substr) {
		h.mu.Lock()
		defer h.mu.Unlock()
		*h.levels = append(*h.levels, r.Level)
	}
	return nil
}

func (h levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h levelRecorder) WithGroup(string) slog.Handler      { return h }
