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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// failFirstScans returns a scanItems func whose first n Scans return a
// keyless item, which fails the backfill, and whose later Scans return item
// pk=a.
func failFirstScans(n int32) func(string) string {
	var calls atomic.Int32
	return func(string) string {
		if calls.Add(1) <= n {
			return `[{"other":{"S":"x"}}]`
		}
		return `[{"pk":{"S":"a"}}]`
	}
}

func constantRetryBackoff(d time.Duration) func() backoff.BackOff {
	return func() backoff.BackOff { return backoff.NewConstantBackOff(d) }
}

// signalingBackOff is a constant backoff that reports each NextBackOff call
// on scheduled, so a test knows a retry has been scheduled.
type signalingBackOff struct {
	*backoff.ConstantBackOff
	scheduled chan<- struct{}
}

func (b signalingBackOff) NextBackOff() time.Duration {
	select {
	case b.scheduled <- struct{}{}:
	default:
	}
	return b.ConstantBackOff.NextBackOff()
}

// TestConnectIncrementalSingleFailedBackfillIsRetried: a backfill that fails
// once is retried in-process after its backoff and completes, without a
// reconnect.
func TestConnectIncrementalSingleFailedBackfillIsRetried(t *testing.T) {
	streams := &incStreamsStub{
		describe: func(string) []string { return []string{"s1"} },
		records:  map[string]string{"s1": oneStreamRecord},
	}
	scan := &scanStubTransport{scanItems: failFirstScans(1)}
	api := newMemCheckpointAPI(nil)
	d := newIncSingleInput(t, scan, streams, api)
	d.backfillRetryBackoff = constantRetryBackoff(10 * time.Millisecond)
	// s1 is settled past every read, so pages release at once.
	d.incremental = settledIncrementalState(time.Now().Add(time.Hour))
	defer waitStopped(t, d)

	require.NoError(t, d.connectIncrementalSingle(t.Context(), "t"))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var snapshotPKsSeen []string
	for len(snapshotPKsSeen) == 0 {
		batch, ack, err := d.ReadBatch(ctx)
		require.NoError(t, err, "the failed backfill must not stop streaming")
		if event, _ := batch[0].MetaGet("dynamodb_event_name"); event == "READ" {
			snapshotPKsSeen = snapshotPKs(t, batch)
		}
		require.NoError(t, ack(t.Context(), nil))
	}
	assert.Equal(t, []string{"a"}, snapshotPKsSeen, "the retried backfill emits the item")
	require.Eventually(t, func() bool { return d.snapshot.state.Load() == snapshotStateComplete },
		5*time.Second, 5*time.Millisecond, "the retried backfill completes")
	assert.True(t, api.has(testStreamArn, "snapshot#complete"))
	scan.mu.Lock()
	assert.Len(t, scan.scanStartKeys, 2, "one failed Scan, one retried Scan")
	scan.mu.Unlock()
}

// TestRunBackfillQueueRequeuesFailedTable: a failed table does not hold up
// the serial queue. The next table is backfilled while the failed one waits
// out its backoff, and the failed one is then pushed again and completes.
func TestRunBackfillQueueRequeuesFailedTable(t *testing.T) {
	// The first Scan is t1's and fails; t2's and t1's retry succeed.
	tr := &scanStubTransport{scanItems: failFirstScans(1)}
	d := newBackfillTestInput(t, tr)
	// Long enough that t2 runs before t1 is pushed again.
	d.backfillRetryBackoff = constantRetryBackoff(200 * time.Millisecond)
	d.tableStreams = map[string]*tableStream{
		"t1": newQueueTableStream(t, d, "t1"),
		"t2": newQueueTableStream(t, d, "t2"),
	}
	d.backfills = incrementalsnapshot.NewTableQueue()
	d.backfills.Push("t1")
	d.backfills.Push("t2")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		d.runBackfillQueue(ctx)
	}()

	for _, want := range []string{"t2", "t1"} {
		var m asyncMessage
		select {
		case m = <-d.msgChan:
		case <-time.After(5 * time.Second):
			t.Fatalf("queue did not reach table %s", want)
		}
		tbl, ok := m.msg[0].MetaGet("dynamodb_table")
		require.True(t, ok)
		assert.Equal(t, want, tbl, "t2 runs while t1 waits, then t1 is retried")
		assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))
		require.NoError(t, m.ackFn(t.Context(), nil))
	}
	require.Eventually(t, func() bool { return completeMarkers(tr) == 2 },
		5*time.Second, 5*time.Millisecond, "both backfills complete")

	cancel()
	select {
	case <-runnerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not return on shutdown")
	}
}

// TestRunBackfillQueueNoRequeueAfterShutdown: a failed table's delayed push
// is dropped when the input stops first, and the runner does not return
// until the delayed push has exited.
func TestRunBackfillQueueNoRequeueAfterShutdown(t *testing.T) {
	tr := &scanStubTransport{scanItems: failFirstScans(1)}
	d := newBackfillTestInput(t, tr)
	const retryWait = 100 * time.Millisecond
	scheduled := make(chan struct{}, 1)
	d.backfillRetryBackoff = func() backoff.BackOff {
		return signalingBackOff{ConstantBackOff: backoff.NewConstantBackOff(retryWait), scheduled: scheduled}
	}
	d.tableStreams = map[string]*tableStream{"t1": newQueueTableStream(t, d, "t1")}
	d.backfills = incrementalsnapshot.NewTableQueue()
	d.backfills.Push("t1")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		d.runBackfillQueue(ctx)
	}()
	select {
	case <-scheduled:
	case <-time.After(5 * time.Second):
		t.Fatal("t1's failure did not schedule a retry")
	}
	cancel()
	select {
	case <-runnerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not return on shutdown")
	}

	waitBackgroundWorkers(t, d)
	time.Sleep(3 * retryWait)
	assert.Equal(t, 0, d.backfills.Len(), "nothing is pushed after shutdown")
	tr.mu.Lock()
	assert.Len(t, tr.scanStartKeys, 1, "t1 is not retried after shutdown")
	tr.mu.Unlock()
}

// newQueueTableStream builds a multi-table incremental stream for name whose
// shard progress is settled, so its pages release at once.
func newQueueTableStream(t *testing.T, d *dynamoDBCDCInput, name string) *tableStream {
	t.Helper()
	return &tableStream{
		tableName:       name,
		streamArn:       testStreamArn,
		keySchema:       []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk")}},
		checkpointer:    newBackfillCheckpointer(t, d.dynamoClient),
		streamSpec:      &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: dynamodbtypes.StreamViewTypeNewImage},
		incremental:     settledIncrementalState(time.Now().Add(time.Hour)),
		coordinatorDone: make(chan struct{}),
	}
}

// waitBackgroundWorkers waits for every background worker of d, delayed
// retries included, to return.
func waitBackgroundWorkers(t *testing.T, d *dynamoDBCDCInput) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		d.backgroundWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("background workers did not stop")
	}
}

// flakyCheckpointAPI is a memCheckpointAPI whose first failReads reads of
// the snapshot completion marker fail, like a transient AWS error while
// reading snapshot progress. Shard checkpoint reads are unaffected.
type flakyCheckpointAPI struct {
	*memCheckpointAPI
	failReads atomic.Int32
	reads     atomic.Int32
}

func (f *flakyCheckpointAPI) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if _, rng := splitKey(in.Key); rng == "snapshot#complete" {
		f.reads.Add(1)
		if f.failReads.Add(-1) >= 0 {
			return nil, errors.New("stubbed transient GetItem failure")
		}
	}
	return f.memCheckpointAPI.GetItem(ctx, in, opts...)
}

// newFlakyTableStream builds table name's stream with its checkpoints in a
// flakyCheckpointAPI failing the first failReads completion reads. Its one shard
// s1 is read past any page high, so pages release once the coordinator has
// settled the shard list.
func newFlakyTableStream(t *testing.T, d *dynamoDBCDCInput, name string, failReads int32) (*tableStream, *flakyCheckpointAPI) {
	t.Helper()
	api := &flakyCheckpointAPI{memCheckpointAPI: newMemCheckpointAPI(nil)}
	api.failReads.Store(failReads)
	arn := testArn(name)
	cp := memCheckpointer(t, api.memCheckpointAPI, name, arn)
	cp.svc = api
	ts := newIncTableStream(d, name, arn, cp)
	ts.incremental.Progress().Observe("s1", time.Now().Add(time.Hour))
	return ts, api
}

// expectBackfilled waits for table's snapshot page (item pk=a), acks it and
// waits for the snapshot to complete.
func expectBackfilled(t *testing.T, d *dynamoDBCDCInput, api *flakyCheckpointAPI, table string) {
	t.Helper()
	var m asyncMessage
	select {
	case m = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("table %s not backfilled", table)
	}
	tbl, ok := m.msg[0].MetaGet("dynamodb_table")
	require.True(t, ok)
	assert.Equal(t, table, tbl)
	assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))
	require.NoError(t, m.ackFn(t.Context(), nil))
	require.Eventually(t, func() bool { return api.has(testArn(table), "snapshot#complete") },
		5*time.Second, 5*time.Millisecond, "the backfill completes")
}

// TestConnectMultipleTablesRetriesFailedPrepare: a transient checkpoint read
// failure while preparing a table at connect does not leave it without a
// backfill: the prepare is retried after its backoff, and the table is then
// queued and backfilled. Connect itself succeeds and streaming starts.
func TestConnectMultipleTablesRetriesFailedPrepare(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.backfillRetryBackoff = constantRetryBackoff(20 * time.Millisecond)
	ts, api := newFlakyTableStream(t, d, "t1", 1)
	d.tableStreams["t1"] = ts

	require.NoError(t, d.connectMultipleTables(t.Context(), []string{"t1"}))
	defer stopInput(t, d)

	expectBackfilled(t, d, api, "t1")
}

// TestStartDiscoveredTableRetriesFailedPrepare: as at connect, a discovered
// table whose prepare fails transiently is prepared again after its backoff,
// then queued and backfilled.
func TestStartDiscoveredTableRetriesFailedPrepare(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.backfillRetryBackoff = constantRetryBackoff(20 * time.Millisecond)
	d.backfills = incrementalsnapshot.NewTableQueue()
	d.startBackgroundWorker("incremental snapshot queue", d.runBackfillQueue)
	defer stopInput(t, d)

	// Two failures: the retried prepare fails once more and backs off again.
	ts, api := newFlakyTableStream(t, d, "t3", 2)
	d.mu.Lock()
	d.tableStreams["t3"] = ts
	d.mu.Unlock()

	d.startDiscoveredTable(t.Context(), "t3", ts)

	expectBackfilled(t, d, api, "t3")
	assert.GreaterOrEqual(t, api.reads.Load(), int32(3), "two failed prepares, then one that succeeds")
}

// TestPrepareRetryStopsWithInput: a prepare retry waiting out its backoff is
// dropped when the input stops, and nothing is pushed afterwards.
func TestPrepareRetryStopsWithInput(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	d := newIncConnectInput(t, &scanStubTransport{}, streams)
	const retryWait = 100 * time.Millisecond
	scheduled := make(chan struct{}, 1)
	d.backfillRetryBackoff = func() backoff.BackOff {
		return signalingBackOff{ConstantBackOff: backoff.NewConstantBackOff(retryWait), scheduled: scheduled}
	}
	d.backfills = incrementalsnapshot.NewTableQueue()
	ts, api := newFlakyTableStream(t, d, "t1", 1)

	assert.False(t, d.prepareOrRetryTableBackfill(t.Context(), t.Context(), "t1", ts), "a failed prepare does not queue the table")
	select {
	case <-scheduled:
	case <-time.After(5 * time.Second):
		t.Fatal("the failed prepare did not schedule a retry")
	}
	d.shutSig.TriggerSoftStop()
	waitBackgroundWorkers(t, d)

	time.Sleep(3 * retryWait)
	assert.Equal(t, 0, d.backfills.Len(), "nothing is pushed after shutdown")
	assert.Equal(t, int32(1), api.reads.Load(), "the prepare is not retried after shutdown")
}

// TestPrepareRetryOutlivesCancelledConnect: the synchronous first attempt is
// bounded by Connect's context, and when that context has ended the retry is
// still scheduled on retryCtx, so the table is prepared and queued later
// instead of staying prepare-pending.
func TestPrepareRetryOutlivesCancelledConnect(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	d := newIncConnectInput(t, &scanStubTransport{}, streams)
	d.backfillRetryBackoff = constantRetryBackoff(10 * time.Millisecond)
	d.backfills = incrementalsnapshot.NewTableQueue()
	ts, _ := newFlakyTableStream(t, d, "t1", 1)

	connectCtx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.False(t, d.prepareOrRetryTableBackfill(connectCtx, t.Context(), "t1", ts), "the failed first attempt does not queue the table")
	assert.Eventually(t, func() bool { return d.backfills.Len() == 1 }, 5*time.Second, 5*time.Millisecond,
		"the retry runs on retryCtx and queues the table")
	d.shutSig.TriggerSoftStop()
	waitBackgroundWorkers(t, d)
}

// TestPrepareNoRetryWhenNoBackfillNeeded: a table whose completed snapshot is
// still valid needs no backfill, so nothing is queued or retried.
func TestPrepareNoRetryWhenNoBackfillNeeded(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	d := newIncConnectInput(t, &scanStubTransport{}, streams)
	scheduled := make(chan struct{}, 1)
	d.backfillRetryBackoff = func() backoff.BackOff {
		return signalingBackOff{ConstantBackOff: backoff.NewConstantBackOff(time.Millisecond), scheduled: scheduled}
	}
	d.backfills = incrementalsnapshot.NewTableQueue()
	ts, api := newFlakyTableStream(t, d, "t1", 0)
	arn := testArn("t1")
	api.put(arn, snapshotRow(arn, "snapshot#complete", true, nil))

	assert.False(t, d.prepareOrRetryTableBackfill(t.Context(), t.Context(), "t1", ts))
	select {
	case <-scheduled:
		t.Fatal("a table that needs no backfill must not be retried")
	case <-time.After(100 * time.Millisecond):
	}
	assert.Equal(t, 0, d.backfills.Len())
}

// TestSignalDuringPrepareRetryWaitsForPrepare: a snapshot-execute signal for
// a table whose prepare is waiting to be retried must not start its
// backfill alongside the retried prepare. The queue defers it, and once the
// prepare succeeds the table is backfilled exactly once.
func TestSignalDuringPrepareRetryWaitsForPrepare(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.conf.signalTable = "sig"
	const retryWait = time.Second
	d.backfillRetryBackoff = constantRetryBackoff(retryWait)
	d.backfills = incrementalsnapshot.NewTableQueue()
	d.startBackgroundWorker("incremental snapshot queue", d.runBackfillQueue)
	defer stopInput(t, d)

	ts, api := newFlakyTableStream(t, d, "t1", 1)
	d.mu.Lock()
	d.tableStreams["t1"] = ts
	d.mu.Unlock()
	d.startDiscoveredTable(t.Context(), "t1", ts)
	require.Equal(t, int32(1), api.reads.Load(), "the first prepare ran and failed")

	// What handleSnapshotSignal does for an incomplete table.
	require.NoError(t, ts.checkpointer.MarkSnapshotRequested(t.Context()))
	d.backfills.Push("t1")

	assert.Never(t, func() bool { return len(scan.scannedTables()) > 0 },
		retryWait/4, 10*time.Millisecond, "no backfill runs while the prepare is pending")

	expectBackfilled(t, d, api, "t1")
	assert.Never(t, func() bool { return len(scan.scannedTables()) > 1 },
		3*retryWait, 10*time.Millisecond, "the table is backfilled exactly once")
	assert.Equal(t, []string{"t1"}, scan.scannedTables())
}

// TestCDCCheckpointStaleOnlyOnTrimmedData: only a trimmed sequence number
// marks a checkpoint stale. A throttled probe is returned as an error, so a
// transient failure never restarts a snapshot from scratch.
func TestCDCCheckpointStaleOnlyOnTrimmedData(t *testing.T) {
	tests := []struct {
		name      string
		trimmed   bool
		throttled bool
		wantStale bool
		wantErr   bool
	}{
		{name: "trimmed sequence is stale", trimmed: true, wantStale: true},
		{name: "throttled probe is an error", throttled: true, wantErr: true},
		{name: "valid sequence is fresh"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streams := &incStreamsStub{
				describe:  func(string) []string { return []string{"s1"} },
				trimmed:   func(string) bool { return tt.trimmed },
				throttled: func(string) bool { return tt.throttled },
			}
			d := newIncConnectInput(t, &scanStubTransport{}, streams)
			api := newMemCheckpointAPI(nil)
			arn := testArn("t1")
			api.put(arn, memShardRow(arn, "s1", "100"))
			cp := memCheckpointer(t, api, "t1", arn)

			stale, err := d.isCDCCheckpointStaleFor(t.Context(), cp, aws.String(arn))
			if tt.wantErr {
				require.Error(t, err)
				// GetShardIterator does not model LimitExceededException, so
				// the SDK surfaces it as a generic API error.
				assert.ErrorContains(t, err, "LimitExceededException", "the probe error is returned")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantStale, stale)
		})
	}
}

// describingTransport serves DescribeTable for any table, as a NEW_IMAGE
// table keyed by pk whose stream is testArn(name), and sends every other
// call to the embedded scanStubTransport.
type describingTransport struct {
	*scanStubTransport
}

func (d describingTransport) Do(req *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".DescribeTable") {
		return d.scanStubTransport.Do(req)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var in struct{ TableName string }
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	return jsonResponse(req, 200, fmt.Sprintf(`{"Table":{"TableName":%q,"TableStatus":"ACTIVE","LatestStreamArn":%q,"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_IMAGE"}}}`, in.TableName, testArn(in.TableName))), nil
}

// TestSignalBeforeDiscoveredTablePrepareWaitsForPrepare: a table found by
// discovery is prepare-pending from the moment it is published, so a
// snapshot-execute signal that reaches the queue before its first prepare
// has even started does not run a backfill alongside that prepare. Once the
// prepare succeeds, the table is backfilled exactly once.
func TestSignalBeforeDiscoveredTablePrepareWaitsForPrepare(t *testing.T) {
	streams := &incStreamsStub{describe: func(string) []string { return []string{"s1"} }}
	scan := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newIncConnectInput(t, scan, streams)
	d.conf.checkpointTable = "ckpt"
	d.dynamoClient = newStubDynamoClient(describingTransport{scan})
	d.backfills = incrementalsnapshot.NewTableQueue()
	d.startBackgroundWorker("incremental snapshot queue", d.runBackfillQueue)
	defer stopInput(t, d)

	isNew, err := d.initializeTableStream(t.Context(), "t3")
	require.NoError(t, err)
	require.True(t, isNew)
	d.mu.RLock()
	ts := d.tableStreams["t3"]
	d.mu.RUnlock()
	ts.incremental.Progress().Observe("s1", time.Now().Add(time.Hour))

	// A signal's push lands before discovery has prepared the table.
	d.backfills.Push("t3")
	assert.Never(t, func() bool { return len(scan.scannedTables()) > 0 },
		200*time.Millisecond, 10*time.Millisecond, "no backfill runs before the table's first prepare")

	d.startDiscoveredTable(t.Context(), "t3", ts)

	var m asyncMessage
	select {
	case m = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("table t3 not backfilled")
	}
	assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))
	require.NoError(t, m.ackFn(t.Context(), nil))
	require.Eventually(t, func() bool { return completeMarkers(scan) == 1 },
		5*time.Second, 5*time.Millisecond, "the backfill completes")
	assert.Never(t, func() bool { return len(scan.scannedTables()) > 1 },
		300*time.Millisecond, 10*time.Millisecond, "the table is backfilled exactly once")
}

// TestInitializeTableStreamExistingTableLeavesNoMark: re-initializing a
// table that is already published does not mark it prepare-pending again,
// so its pushes are never deferred by a mark nothing would clear.
func TestInitializeTableStreamExistingTableLeavesNoMark(t *testing.T) {
	scan := &scanStubTransport{}
	d := newIncConnectInput(t, scan, &incStreamsStub{describe: func(string) []string { return nil }})
	d.conf.checkpointTable = "ckpt"
	d.dynamoClient = newStubDynamoClient(describingTransport{scan})

	_, err := d.initializeTableStream(t.Context(), "t1")
	require.NoError(t, err)
	assert.True(t, d.backfillRetries.skipIfPreparing("t1"), "a newly published table is prepare-pending")
	d.backfillRetries.endPrepare("t1")

	isNew, err := d.initializeTableStream(t.Context(), "t1")
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.False(t, d.backfillRetries.skipIfPreparing("t1"), "an existing table is not marked again")
}
