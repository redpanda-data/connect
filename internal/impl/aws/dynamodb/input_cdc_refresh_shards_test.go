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
	parents            map[string]string
	failShards         map[string]bool
	iteratorCalls      []string
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
	d.completeShard(t.Context(), "shard-closed")

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
	d.completeTableShard(t.Context(), "test", ts, "shard-closed")

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

		d.completeShard(t.Context(), "shard-parent")
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

		d.completeTableShard(t.Context(), "test", ts, "shard-parent")
		select {
		case <-ts.shardRefreshCh:
		default:
			t.Fatal("completing a parent must trigger an immediate refresh for its children")
		}
		require.NoError(t, d.refreshTableShards(t.Context(), "test", ts))
		assert.ElementsMatch(t, []string{"shard-parent", "shard-child", "shard-orphan"}, tableShards())
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
	d.recordBatcher = NewRecordBatcher(100, 1000, service.MockResources().Logger())
	cp := &mockCheckpointer{checkpointLimit: 1000, completeErr: errors.New("boom")}

	require.NoError(t, d.refreshShards(t.Context()))
	tb := d.recordBatcher.AddMessages(createTestMessages(2, "shard-closed", 1), "shard-closed")
	require.NoError(t, d.recordBatcher.CloseShard(t.Context(), cp, "shard-closed"))
	require.Error(t, d.recordBatcher.AckBatch(t.Context(), cp, tb))
	d.completeShard(t.Context(), "shard-closed")
	require.Equal(t, 1, d.recordBatcher.TrackedShardCount())

	transport.mu.Lock()
	transport.shardIDs = nil
	transport.mu.Unlock()
	require.NoError(t, d.refreshShards(t.Context()))
	assert.Zero(t, d.recordBatcher.TrackedShardCount())
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
	// The store is written from ack goroutines and read by the test, so
	// serialise access to the shared fake.
	var storeMu sync.Mutex
	api := sharedStoreAPI(map[string]map[string]dynamodbtypes.AttributeValue{})
	putItem, getItem := api.putItem, api.getItem
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
		recordBatcher:   NewRecordBatcher(100, 1000, service.MockResources().Logger()),
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
