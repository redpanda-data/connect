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
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/replication"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

const (
	signalArnT1  = "arn:aws:dynamodb:us-east-1:123456789012:table/t1/stream/2024-01-01T00:00:00.000"
	signalArnT2  = "arn:aws:dynamodb:us-east-1:123456789012:table/t2/stream/2024-01-01T00:00:00.000"
	signalArnSig = "arn:aws:dynamodb:us-east-1:123456789012:table/sig/stream/2024-01-01T00:00:00.000"
	signalArnKO  = "arn:aws:dynamodb:us-east-1:123456789012:table/ko/stream/2024-01-01T00:00:00.000"
)

// failingPutAPI fails the first failPuts PutItem calls (all of them when
// failPuts is negative), then delegates to the in-memory table.
type failingPutAPI struct {
	*memCheckpointAPI
	failPuts int64
	puts     atomic.Int64
}

func (f *failingPutAPI) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	n := f.puts.Add(1)
	if f.failPuts < 0 || n <= f.failPuts {
		return nil, errors.New("stubbed PutItem failure")
	}
	return f.memCheckpointAPI.PutItem(ctx, in, opts...)
}

func signalCheckpointer(api checkpointDynamoAPI, table, arn string) *Checkpointer {
	return &Checkpointer{
		tableName:       "ckpt",
		sourceTable:     table,
		streamArn:       arn,
		checkpointLimit: 10,
		svc:             api,
		log:             service.MockResources().Logger(),
	}
}

func newImageSpec(view dynamodbtypes.StreamViewType) *dynamodbtypes.StreamSpecification {
	return &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: view}
}

// newSignalTestInput builds an incremental multi-table input watching t1
// (incremental, NEW_IMAGE, not yet backfilled), t2 (backfilled), ko
// (KEYS_ONLY) and the signal table sig. The checkpointers share mem; api is
// the API they call, so a test can wrap mem to inject failures.
func newSignalTestInput(api checkpointDynamoAPI, mem *memCheckpointAPI) *dynamoDBCDCInput {
	mem.put(signalArnT2, snapshotRow(signalArnT2, "snapshot#complete", true, nil))
	inc := func() *incsnapshot.State { return incsnapshot.NewState(0, time.Minute) }
	return &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			snapshot: snapshotConfig{mode: snapshotModeIncremental},
		},
		log:       service.MockResources().Logger(),
		metrics:   newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		backfills: incrementalsnapshot.NewTableQueue(),
		tableStreams: map[string]*tableStream{
			"t1": {
				tableName: "t1", streamArn: signalArnT1, incremental: inc(),
				streamSpec:   newImageSpec(dynamodbtypes.StreamViewTypeNewImage),
				checkpointer: signalCheckpointer(api, "t1", signalArnT1),
			},
			"t2": {
				tableName: "t2", streamArn: signalArnT2, incremental: inc(),
				streamSpec:   newImageSpec(dynamodbtypes.StreamViewTypeNewAndOldImages),
				checkpointer: signalCheckpointer(api, "t2", signalArnT2),
			},
			"ko": {
				tableName: "ko", streamArn: signalArnKO, incremental: inc(),
				streamSpec:   newImageSpec(dynamodbtypes.StreamViewTypeKeysOnly),
				checkpointer: signalCheckpointer(api, "ko", signalArnKO),
			},
			"sig": {
				tableName: "sig", streamArn: signalArnSig, isSignalTable: true,
				streamSpec:   newImageSpec(dynamodbtypes.StreamViewTypeNewImage),
				checkpointer: signalCheckpointer(api, "sig", signalArnSig),
			},
		},
	}
}

func snapshotSignalRecord(id, data string) types.Record {
	return signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
		"id": strAttr(id), "type": strAttr(replication.SnapshotSignalType), "data": strAttr(data),
	})
}

func TestHandleSnapshotSignalQueuesAndMarksRequested(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1"]}`)))

	assert.True(t, mem.has(signalArnT1, "snapshot#requested"), "t1's requested row is written")
	require.Equal(t, 1, d.backfills.Len())
	table, ok := d.backfills.Pop(t.Context())
	require.True(t, ok)
	assert.Equal(t, "t1", table)
}

func TestHandleSnapshotSignalSkipsCompleteTables(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1","t2"]}`)))

	assert.True(t, mem.has(signalArnT1, "snapshot#requested"))
	assert.False(t, mem.has(signalArnT2, "snapshot#requested"), "nothing is written for the backfilled t2")
	assert.Equal(t, 1, d.backfills.Len(), "only t1 is queued")
}

func TestHandleSnapshotSignalRejectsWholeRequestOnUnknownTable(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1","nope"]}`)))

	assert.False(t, mem.has(signalArnT1, "snapshot#requested"), "a rejected request writes nothing")
	assert.Equal(t, 0, d.backfills.Len(), "a rejected request queues nothing")
}

func TestHandleSnapshotSignalRejectsSignalTableAndKeysOnly(t *testing.T) {
	for _, bad := range []string{"sig", "ko"} {
		t.Run(bad, func(t *testing.T) {
			mem := newMemCheckpointAPI(nil)
			d := newSignalTestInput(mem, mem)

			require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1","`+bad+`"]}`)))

			assert.False(t, mem.has(signalArnT1, "snapshot#requested"))
			assert.False(t, mem.has(signalArnSig, "snapshot#requested"))
			assert.False(t, mem.has(signalArnKO, "snapshot#requested"))
			assert.Equal(t, 0, d.backfills.Len())
		})
	}
}

func TestHandleSnapshotSignalWarnsWhenNotIncremental(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)
	d.conf.snapshot.mode = snapshotModeNone

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1"]}`)))

	assert.False(t, mem.has(signalArnT1, "snapshot#requested"))
	assert.Equal(t, 0, d.backfills.Len())
}

func TestHandleSnapshotSignalRejectsMalformedPayload(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":[]}`)))
	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{nope`)))
	assert.Equal(t, 0, d.backfills.Len())
}

func TestHandleSnapshotSignalReturnsErrorWhenCheckpointFails(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	api := &failingPutAPI{memCheckpointAPI: mem, failPuts: -1}
	d := newSignalTestInput(api, mem)

	require.Error(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1"]}`)))

	assert.False(t, mem.has(signalArnT1, "snapshot#requested"))
	assert.Equal(t, 0, d.backfills.Len(), "nothing is queued before the requested row is durable")
}

func TestHandleSignalRecordsRetriesUntilWriteSucceeds(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	api := &failingPutAPI{memCheckpointAPI: mem, failPuts: 1}
	d := newSignalTestInput(api, mem)

	ok := d.handleSignalRecords(t.Context(), []types.Record{snapshotSignalRecord("s1", `{"tables":["t1"]}`)})

	assert.True(t, ok)
	assert.Equal(t, int64(2), api.puts.Load(), "the failed write is retried once")
	assert.True(t, mem.has(signalArnT1, "snapshot#requested"))
	assert.Equal(t, 1, d.backfills.Len())
}

func TestHandleSignalRecordsReturnsFalseOnCancel(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	api := &failingPutAPI{memCheckpointAPI: mem, failPuts: -1}
	d := newSignalTestInput(api, mem)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan bool, 1)
	go func() {
		done <- d.handleSignalRecords(ctx, []types.Record{snapshotSignalRecord("s1", `{"tables":["t1"]}`)})
	}()
	require.Eventually(t, func() bool { return api.puts.Load() >= 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		assert.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("handleSignalRecords did not return promptly after cancel")
	}
	assert.Equal(t, 0, d.backfills.Len())
}

func TestHandleSignalRecordsSkipsRejectedAndNonSignalRecords(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	ok := d.handleSignalRecords(t.Context(), []types.Record{
		signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{"id": strAttr("bad")}),
		signalRecord(types.OperationTypeModify, map[string]types.AttributeValue{
			"id": strAttr("m"), "type": strAttr("snapshot-execute"), "data": strAttr(`{"tables":["t1"]}`),
		}),
		signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
			"id": strAttr("l"), "type": strAttr("log"), "data": strAttr(`{"message":"hello"}`),
		}),
		signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{
			"id": strAttr("u"), "type": strAttr("mystery"), "data": strAttr(`{}`),
		}),
	})

	assert.True(t, ok)
	assert.Equal(t, 0, d.backfills.Len(), "MODIFY is not a signal, and nothing else asks for a snapshot")

	ok = d.handleSignalRecords(t.Context(), []types.Record{
		signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{"id": strAttr("bad")}),
		snapshotSignalRecord("s1", `{"tables":["t1"]}`),
	})
	assert.True(t, ok)
	assert.Equal(t, 1, d.backfills.Len(), "a rejected record does not stop the rest of the page")
}

func TestHandleSnapshotSignalIsIdempotent(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)

	data := []byte(`{"tables":["t1"]}`)
	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, data))
	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, data))

	assert.True(t, mem.has(signalArnT1, "snapshot#requested"))
	assert.Equal(t, 1, d.backfills.Len(), "t1 is queued once")
}

const snapshotSignalPage = `[{"eventID":"1","eventName":"INSERT","dynamodb":{"SequenceNumber":"00001","Keys":{"id":{"S":"s1"}},` +
	`"NewImage":{"id":{"S":"s1"},"type":{"S":"snapshot-execute"},"data":{"S":"{\"tables\":[\"t1\"]}"}}}}]`

// The signal table's reader must durably record the request and queue the
// table before the signal record's batch can reach the message channel, and
// so before it can be acked.
func TestSignalReaderMarksRequestedBeforeSend(t *testing.T) {
	// msgChanCap 0: the reader blocks on the unbuffered send until this test
	// receives, so the request's effects are observed before the batch can
	// be delivered.
	h := readerHarnesses(snapshotSignalPage, 100, 0)["multi-table"]()
	mem := newMemCheckpointAPI(nil)
	sd := newSignalTestInput(mem, mem)
	h.d.conf.snapshot = sd.conf.snapshot
	h.d.backfills = sd.backfills
	h.d.tableStreams = sd.tableStreams
	h.ts.isSignalTable = true

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go h.start(ctx)

	assert.Eventually(t, func() bool {
		return mem.has(signalArnT1, "snapshot#requested") && h.d.backfills.Len() == 1
	}, 5*time.Second, 5*time.Millisecond, "the reader must record and queue the request before sending")
	assert.Equal(t, 1, h.batcher.TrackedMessageCount(), "the signal batch is tracked and waiting on the send")

	select {
	case m := <-h.d.msgChan:
		assert.Len(t, m.msg, 1)
		require.NoError(t, m.ackFn(t.Context(), nil))
	case <-time.After(5 * time.Second):
		t.Fatal("no batch from reader")
	}
}

// A signal reader cancelled while its checkpoint write keeps failing must
// return without sending and untrack the batch, like a cancelled send.
func TestSignalReaderCancelWhileRetryingUntracksBatch(t *testing.T) {
	h := readerHarnesses(snapshotSignalPage, 100, 0)["multi-table"]()
	mem := newMemCheckpointAPI(nil)
	api := &failingPutAPI{memCheckpointAPI: mem, failPuts: -1}
	sd := newSignalTestInput(api, mem)
	h.d.conf.snapshot = sd.conf.snapshot
	h.d.backfills = sd.backfills
	h.d.tableStreams = sd.tableStreams
	h.ts.isSignalTable = true

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() {
		h.start(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return api.puts.Load() >= 1 }, 5*time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader should exit promptly on cancellation")
	}
	assert.Equal(t, 0, h.batcher.TrackedMessageCount(), "the unsent signal batch is untracked")
	assert.Empty(t, h.d.msgChan)
	assert.Equal(t, 0, h.d.backfills.Len())
}

// A signal handled before the backfill queue exists is ignored, not a nil
// Push that crashes the process.
func TestHandleSnapshotSignalWithoutQueueIsIgnored(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)
	d.backfills = nil

	require.NotPanics(t, func() {
		require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1"]}`)))
	})
	assert.False(t, mem.has(signalArnT1, "snapshot#requested"))
}

// captureLogs points d's logger at a buffer and returns it.
func captureLogs(d *dynamoDBCDCInput) *bytes.Buffer {
	var buf bytes.Buffer
	d.log = service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf
}

// Rewriting an existing signal item streams a MODIFY, which is never acted
// on, so the input warns that the id was reused instead of staying silent.
func TestHandleSignalRecordsWarnsOnReusedSignalID(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)
	logs := captureLogs(d)

	ok := d.handleSignalRecords(t.Context(), []types.Record{
		signalRecord(types.OperationTypeModify, map[string]types.AttributeValue{
			"id": strAttr("1"), "type": strAttr(replication.SnapshotSignalType), "data": strAttr(`{"tables":["t1"]}`),
		}),
	})

	assert.True(t, ok)
	assert.Contains(t, logs.String(), "reusing a signal id has no effect; write each signal with a new id")
	assert.Contains(t, logs.String(), "id=1")
	assert.Equal(t, 0, d.backfills.Len(), "the MODIFY is not acted on")
	assert.False(t, mem.has(signalArnT1, "snapshot#requested"))

	// A MODIFY of an item that is not a signal is ignored quietly.
	logs.Reset()
	require.True(t, d.handleSignalRecords(t.Context(), []types.Record{
		signalRecord(types.OperationTypeModify, map[string]types.AttributeValue{"id": strAttr("2")}),
	}))
	assert.Empty(t, logs.String())
}

// Rejections log through the signal's scoped logger and say "rejected" once.
func TestHandleSignalRecordsLogsRejectionOnce(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	d := newSignalTestInput(mem, mem)
	logs := captureLogs(d)

	require.True(t, d.handleSignalRecords(t.Context(), []types.Record{
		snapshotSignalRecord("s9", `{"tables":["nope"]}`),
		snapshotSignalRecord("s10", `{nope`),
		signalRecord(types.OperationTypeInsert, map[string]types.AttributeValue{"id": strAttr("s11")}),
	}))

	out := logs.String()
	assert.Contains(t, out, "Snapshot signal rejected: cannot snapshot nope (not watched)")
	assert.Contains(t, out, "id=s9")
	assert.Contains(t, out, "type="+replication.SnapshotSignalType)
	assert.Contains(t, out, "Snapshot signal rejected: parsing "+replication.SnapshotSignalType+" payload")
	assert.Contains(t, out, "id=s10")
	assert.Contains(t, out, `Control signal rejected: signal record has no \"type\" attribute`)
	assert.NotContains(t, out, "rejected: rejected")
}

// A table named twice in one signal is recorded and queued once.
func TestHandleSnapshotSignalDeduplicatesTables(t *testing.T) {
	mem := newMemCheckpointAPI(nil)
	api := &failingPutAPI{memCheckpointAPI: mem}
	d := newSignalTestInput(api, mem)
	logs := captureLogs(d)

	require.NoError(t, d.handleSnapshotSignal(t.Context(), d.log, []byte(`{"tables":["t1","t2","t1"]}`)))

	assert.Equal(t, int64(1), api.puts.Load(), "t1's requested row is written once")
	assert.Equal(t, 1, d.backfills.Len())
	assert.Contains(t, logs.String(), "signal asked for [t1 t2] but [t2] are already backfilled, so only [t1] were queued")
}
