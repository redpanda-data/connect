// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// incStreamsStub fakes DynamoDB Streams for input-level incremental tests.
// DescribeStream lists describe(streamArn); GetShardIterator fails as trimmed
// for an AFTER_SEQUENCE_NUMBER request on a shard trimmed reports; GetRecords
// serves records[shard] on a shard's first page and empty pages after.
type incStreamsStub struct {
	mu       sync.Mutex
	describe func(streamArn string) []string
	trimmed  func(shardID string) bool
	records  map[string]string
	served   map[string]bool
	log      *eventLog
}

func (s *incStreamsStub) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	body, _ := io.ReadAll(req.Body)
	var in struct {
		StreamArn         string
		ShardId           string
		ShardIteratorType string
		ShardIterator     string
	}
	_ = json.Unmarshal(body, &in)
	switch {
	case strings.HasSuffix(target, ".DescribeStream"):
		shards := []map[string]any{}
		for _, id := range s.describe(in.StreamArn) {
			shards = append(shards, map[string]any{"ShardId": id})
		}
		desc, err := json.Marshal(map[string]any{
			"StreamDescription": map[string]any{"StreamArn": in.StreamArn, "StreamStatus": "ENABLED", "Shards": shards},
		})
		if err != nil {
			return nil, err
		}
		return jsonResponse(req, 200, string(desc)), nil
	case strings.HasSuffix(target, ".GetShardIterator"):
		if in.ShardIteratorType == "AFTER_SEQUENCE_NUMBER" && s.trimmed != nil && s.trimmed(in.ShardId) {
			resp := jsonResponse(req, 400, `{"__type":"TrimmedDataAccessException","message":"stubbed trimmed"}`)
			resp.Header.Set("X-Amzn-ErrorType", "TrimmedDataAccessException")
			return resp, nil
		}
		return jsonResponse(req, 200, fmt.Sprintf(`{"ShardIterator":"iter|%s"}`, in.ShardId)), nil
	case strings.HasSuffix(target, ".GetRecords"):
		shard := strings.TrimPrefix(in.ShardIterator, "iter|")
		s.log.add("getrecords %s", shard)
		s.mu.Lock()
		records := "[]"
		if !s.served[shard] {
			if s.served == nil {
				s.served = map[string]bool{}
			}
			s.served[shard] = true
			if r, ok := s.records[shard]; ok {
				records = r
			}
		}
		s.mu.Unlock()
		return jsonResponse(req, 200, fmt.Sprintf(`{"Records":%s,"NextShardIterator":%q}`, records, in.ShardIterator)), nil
	default:
		return nil, fmt.Errorf("incStreamsStub: unexpected operation %q", target)
	}
}

// newIncConnectInput builds a multi-table-capable incremental input whose
// Scans go to scan and whose stream calls go to streams.
func newIncConnectInput(t *testing.T, scan *scanStubTransport, streams *incStreamsStub) *dynamoDBCDCInput {
	t.Helper()
	return &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			startFrom:        "trim_horizon",
			batchSize:        10,
			checkpointLimit:  10,
			maxTrackedShards: 100,
			pollInterval:     20 * time.Millisecond,
			throttleBackoff:  20 * time.Millisecond,
			snapshot: snapshotConfig{
				mode:      snapshotModeIncremental,
				segments:  1,
				batchSize: 10,
				throttle:  time.Millisecond,
			},
		},
		log:           service.MockResources().Logger(),
		dynamoClient:  newStubDynamoClient(scan),
		streamsClient: newStubStreamsClient(streams),
		metrics:       newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		msgChan:       make(chan asyncMessage, 100),
		shutSig:       shutdown.NewSignaller(),
		snapshot:      &snapshotState{segmentsTotal: 1},
		tableStreams:  map[string]*tableStream{},
	}
}

// newIncTableStream builds a table stream as initializeTableStream would.
func newIncTableStream(d *dynamoDBCDCInput, name, streamArn string, cp *Checkpointer) *tableStream {
	ts := &tableStream{
		tableName:       name,
		streamArn:       streamArn,
		keySchema:       []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash}},
		checkpointer:    cp,
		recordBatcher:   newInputRecordBatcher(d.conf, d.log),
		shardReaders:    map[string]*dynamoDBShardReader{},
		shardRefreshCh:  make(chan struct{}, 1),
		streamSpec:      &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: dynamodbtypes.StreamViewTypeNewImage},
		coordinatorDone: make(chan struct{}),
		incremental:     newIncrementalState(0, time.Minute),
	}
	return ts
}

// stopInput soft-stops d and waits for its background work to finish.
func stopInput(t *testing.T, d *dynamoDBCDCInput) {
	t.Helper()
	d.shutSig.TriggerSoftStop()
	done := make(chan struct{})
	go func() {
		d.backgroundWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("input did not stop")
	}
}

func testArn(table string) string {
	return "arn:aws:dynamodb:us-east-1:123456789012:table/" + table + "/stream/2026-01-01T00:00:00.000"
}

// TestConnectMultipleTablesIncrementalResetsStaleTableBeforeReaders: t1 has
// a completed snapshot whose stream checkpoint is stale (its checkpointed
// shard old1 is trimmed), and the shard that proves it ages out of the
// stream once t1's readers start. The reset must happen at connect, before
// any reader runs; a check made when the queue reaches t1 would find the
// proof gone and never backfill the trimmed changes. t2's completed snapshot
// is still valid, so t2 is never queued.
func TestConnectMultipleTablesIncrementalResetsStaleTableBeforeReaders(t *testing.T) {
	fastReleaseTick(t)
	log := &eventLog{}
	api := newMemCheckpointAPI(log)
	arn1, arn2 := testArn("t1"), testArn("t2")
	api.put(arn1, snapshotRow(arn1, "snapshot#complete", true, nil))
	api.put(arn1, snapshotRow(arn1, "snapshot#segment#0", true, nil))
	api.put(arn1, memShardRow(arn1, "old1", "100"))
	api.put(arn2, snapshotRow(arn2, "snapshot#complete", true, nil))
	api.put(arn2, memShardRow(arn2, "s2", "200"))

	streams := &incStreamsStub{log: log, trimmed: func(shard string) bool { return shard == "old1" }}
	streams.describe = func(arn string) []string {
		switch arn {
		case arn1:
			// old1 ages out of the stream once t1's reader has polled.
			if log.index("getrecords new1") >= 0 {
				return []string{"new1"}
			}
			return []string{"old1", "new1"}
		case arn2:
			return []string{"s2"}
		}
		return nil
	}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.tableStreams["t1"] = newIncTableStream(d, "t1", arn1, memCheckpointer(t, api, "t1", arn1))
	d.tableStreams["t2"] = newIncTableStream(d, "t2", arn2, memCheckpointer(t, api, "t2", arn2))

	require.NoError(t, d.connectMultipleTables(t.Context(), []string{"t1", "t2"}))
	log.add("connected")
	defer stopInput(t, d)

	require.Eventually(t, func() bool { return slices.Contains(scan.scannedTables(), "t1") },
		5*time.Second, 5*time.Millisecond, "the stale table is backfilled again")
	require.Eventually(t, func() bool { return log.index("getrecords new1") >= 0 },
		5*time.Second, 5*time.Millisecond, "t1's reader runs")

	reset := log.index("delete " + arn1 + " snapshot#complete")
	require.GreaterOrEqual(t, reset, 0, "t1's snapshot is reset")
	assert.Less(t, reset, log.index("getrecords new1"), "the reset precedes t1's first reader poll: %v", log.snapshot())
	assert.Less(t, reset, log.index("connected"), "the reset happens at connect")

	assert.Never(t, func() bool { return slices.Contains(scan.scannedTables(), "t2") },
		200*time.Millisecond, 10*time.Millisecond, "t2's valid snapshot is not rerun")
	events := log.snapshot()
	connected := slices.Index(events, "connected")
	for _, e := range events[connected:] {
		assert.NotContains(t, e, arn2+" snapshot#", "t2 is not queued, so nothing reads its snapshot state after connect")
	}
	for _, e := range events {
		assert.False(t, strings.HasPrefix(e, "delete "+arn2), "t2 is not reset: %s", e)
	}
}

// TestStartDiscoveredTableBackfillsNewTable: a table found by periodic
// discovery is prepared, streamed, queued, and backfilled.
func TestStartDiscoveredTableBackfillsNewTable(t *testing.T) {
	fastReleaseTick(t)
	api := newMemCheckpointAPI(nil)
	arn := testArn("t3")
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s3"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.backfills = newBackfillQueue()
	d.startBackgroundWorker("incremental snapshot queue", d.runBackfillQueue)
	defer stopInput(t, d)

	ts := newIncTableStream(d, "t3", arn, memCheckpointer(t, api, "t3", arn))
	d.mu.Lock()
	d.tableStreams["t3"] = ts
	d.mu.Unlock()
	// The stream is read past any page high, so the page releases once the
	// coordinator has settled the shard list.
	ts.incremental.progress.Observe("s3", time.Now().Add(time.Hour))

	d.startDiscoveredTable(t.Context(), "t3", ts)

	var m asyncMessage
	select {
	case m = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("discovered table not backfilled")
	}
	tbl, ok := m.msg[0].MetaGet("dynamodb_table")
	require.True(t, ok)
	assert.Equal(t, "t3", tbl)
	assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))
	require.NoError(t, m.ackFn(t.Context(), nil))
	require.Eventually(t, func() bool { return api.has(arn, "snapshot#complete") },
		5*time.Second, 5*time.Millisecond, "the discovered table's backfill completes")
}
