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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newStubDynamoClient builds a real *dynamodb.Client whose transport is
// entirely local, the DynamoDB counterpart of newStubStreamsClient.
func newStubDynamoClient(t aws.HTTPClient) *dynamodb.Client {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.HTTPClient = t
		o.Retryer = aws.NopRetryer{} // deterministic call counts
	})
}

// connectStubTransport fakes the DynamoDB control plane connectSingleTable
// touches before any shard is read: DescribeTable for the source table (a
// stream is enabled) and for the checkpoint table (it exists), and the Query
// behind Checkpointer.HasAnyState, which fails while failProbe is set so the
// connection attempt fails after the record batcher has been built.
type connectStubTransport struct {
	mu        sync.Mutex
	failProbe bool
	calls     []string
}

func (s *connectStubTransport) count(call string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c == call {
			n++
		}
	}
	return n
}

func (s *connectStubTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	op := target[strings.LastIndex(target, ".")+1:]
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var in struct{ TableName string }
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &in); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, op+":"+in.TableName)
	switch op {
	case "DescribeTable":
		if in.TableName == "cps" {
			return jsonResponse(req, 200, `{"Table":{"TableName":"cps","TableStatus":"ACTIVE"}}`), nil
		}
		return jsonResponse(req, 200, fmt.Sprintf(
			`{"Table":{"TableName":%q,"TableStatus":"ACTIVE","LatestStreamArn":%q,"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}}`,
			in.TableName, testStreamArn)), nil

	case "Query":
		if s.failProbe {
			resp := jsonResponse(req, 500, `{"__type":"InternalServerError","message":"stubbed for test"}`)
			resp.Header.Set("X-Amzn-ErrorType", "InternalServerError")
			return resp, nil
		}
		return jsonResponse(req, 200, `{"Items":[],"Count":0}`), nil

	default:
		return nil, fmt.Errorf("connectStubTransport: unexpected operation %q", target)
	}
}

// waitBackgroundWorkers reports whether every background worker of d exited
// within timeout.
func waitBackgroundWorkers(d *dynamoDBCDCInput, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		d.backgroundWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// TestConnectSingleTable_FailedAttemptStartsNoWorker (CON-613): Benthos
// retries a failed Connect about once a second for as long as it takes, so
// a connection attempt that fails must leave nothing running. The checkpoint
// persister is a background worker of the input; started before the
// attempt's remaining failure points (the checkpoint state probe, shard
// discovery, the no-active-shards check) it leaked one parked goroutine, one
// worker count and one batcher per failed attempt. It must start with the
// shard coordinator instead, so attempts that fail at the probe leave the
// worker group empty.
func TestConnectSingleTable_FailedAttemptStartsNoWorker(t *testing.T) {
	transport := &connectStubTransport{failProbe: true}
	d := &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			startFrom:        "trim_horizon",
			checkpointTable:  "cps",
			batchSize:        10,
			checkpointLimit:  1000,
			maxTrackedShards: 100,
			snapshot:         snapshotConfig{mode: snapshotModeNone},
		},
		awsConf:        aws.Config{Region: "us-east-1"},
		log:            service.MockResources().Logger(),
		metrics:        newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		dynamoClient:   newStubDynamoClient(transport),
		snapshot:       &snapshotState{},
		shardReaders:   map[string]*dynamoDBShardReader{},
		shardRefreshCh: make(chan struct{}, 1),
		msgChan:        make(chan asyncMessage, 10),
		shutSig:        shutdown.NewSignaller(),
	}

	for attempt := range 3 {
		err := d.connectSingleTable(t.Context(), "test")
		require.ErrorContains(t, err, "probing checkpoint state", "attempt %d must fail at the state probe", attempt)
	}
	require.Equal(t, 3, transport.count("Query:cps"), "every attempt reaches the state probe")

	// Nothing stopped the input (no soft stop), so a worker started by a
	// failed attempt would still be parked and the group would never drain.
	assert.True(t, waitBackgroundWorkers(d, 2*time.Second),
		"a failed connection attempt must not leave a background worker running")
}

// TestConnectCDCOnly_StartsPersister (CON-613): the persister is the only
// writer of checkpoints, and connectCDCOnly is where the single-table shard
// coordinator starts, so it must start the persister too or no acknowledged
// progress would ever be persisted. Drives the real CDC-only connect path
// over a stubbed stream with one closed shard, and checks that acking its
// final batch persists the shard's completion with no help from the test.
func TestConnectCDCOnly_StartsPersister(t *testing.T) {
	transport := &coordinatorStubTransport{
		shards: []string{"shard-001"},
		closed: map[string]string{"shard-001": twoRecordsPage},
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
		snapshot:        &snapshotState{},
		shardReaders:    map[string]*dynamoDBShardReader{},
		shardRefreshCh:  make(chan struct{}, 1),
		msgChan:         make(chan asyncMessage, 10),
		shutSig:         shutdown.NewSignaller(),
		refreshInterval: 3 * time.Millisecond,
		cleanupInterval: 5 * time.Millisecond,
	}

	require.NoError(t, d.connectCDCOnly(t.Context()))
	t.Cleanup(func() {
		d.shutSig.TriggerSoftStop()
		assert.True(t, waitBackgroundWorkers(d, 5*time.Second),
			"the coordinator and the persister must stop at soft stop")
	})

	var am asyncMessage
	select {
	case am = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("expected the closed shard's final batch")
	}
	require.Len(t, am.msg, 2)
	require.NoError(t, am.ackFn(t.Context(), nil))

	completed := func() bool {
		_, done, err := cp.getShardRow(t.Context(), "shard-001")
		return err == nil && done
	}
	require.Eventually(t, completed, 5*time.Second, time.Millisecond,
		"acking the closed shard's final batch must persist its completion: connectCDCOnly must start the persister")
}
