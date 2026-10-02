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
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newBackfillTestInput builds a dynamoDBCDCInput whose DynamoDB client is
// served by tr, configured for a single-segment incremental snapshot.
func newBackfillTestInput(t *testing.T, tr *scanStubTransport) *dynamoDBCDCInput {
	t.Helper()
	return &dynamoDBCDCInput{
		conf: dynamoDBCDCConfig{
			batchSize:       10,
			pollInterval:    20 * time.Millisecond,
			throttleBackoff: 20 * time.Millisecond,
			snapshot: snapshotConfig{
				mode:      snapshotModeIncremental,
				segments:  1,
				batchSize: 10,
				throttle:  time.Millisecond,
			},
		},
		log:          service.MockResources().Logger(),
		dynamoClient: newStubDynamoClient(tr),
		metrics:      newDynamoDBCDCMetrics(service.MockResources().Metrics()),
		msgChan:      make(chan asyncMessage, 10),
		shutSig:      shutdown.NewSignaller(),
		snapshot:     &snapshotState{segmentsTotal: 1},
	}
}

// newBackfillCheckpointer builds a Checkpointer against client with the fields
// NewCheckpointer sets, skipping ensureTableExists.
func newBackfillCheckpointer(t *testing.T, client *dynamodb.Client) *Checkpointer {
	t.Helper()
	return &Checkpointer{
		tableName:       "ckpt",
		sourceTable:     "t",
		streamArn:       testStreamArn,
		checkpointLimit: 10,
		svc:             client,
		log:             service.MockResources().Logger(),
	}
}

// settledIncrementalState returns a state tracking one shard s1 whose shard
// list is settled, observed at observed.
func settledIncrementalState(observed time.Time) *incrementalState {
	inc := newIncrementalState(0, time.Minute)
	inc.Register("s1")
	inc.RefreshDone([]string{"s1"})
	inc.progress.Observe("s1", observed)
	return inc
}

// snapshotPKs extracts the newImage pk of every message in batch.
func snapshotPKs(t *testing.T, batch service.MessageBatch) []string {
	t.Helper()
	var pks []string
	for _, msg := range batch {
		s, err := msg.AsStructured()
		require.NoError(t, err)
		img := s.(map[string]any)["dynamodb"].(map[string]any)["newImage"].(map[string]any)
		pks = append(pks, img["pk"].(string))
	}
	return pks
}

func backfillTargetFor(cp *Checkpointer, inc *incrementalState) backfillTarget {
	return backfillTarget{
		table: "t", keySchema: []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk")}},
		checkpointer: cp, state: inc,
	}
}

func TestIncrementalBackfillEmitsUntouchedItemsAndCompletes(t *testing.T) {
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(time.Hour))

	// Touch b while its page is in flight: the page opened in the
	// before-request hook, so the touch lands before Hold.
	tr.onScan = func() { inc.window.Touch(mustStreamKey(t, "b")) }

	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range d.msgChan {
			got = append(got, snapshotPKs(t, m.msg)...)
			assert.NoError(t, m.ackFn(t.Context(), nil))
		}
	}()

	err := d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc))
	require.NoError(t, err)
	close(d.msgChan)
	<-done
	assert.Equal(t, []string{"a"}, got, "b was touched while its page was in flight")
	assert.True(t, tr.putContains("snapshot#complete"), "completion marker written after acks")
}

func TestIncrementalBackfillHoldsUntilCaughtUp(t *testing.T) {
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(-time.Hour))
	tr.onScan = func() { inc.window.Touch(mustStreamKey(t, "b")) }

	batches := make(chan []string, 10)
	go func() {
		for m := range d.msgChan {
			batches <- snapshotPKs(t, m.msg)
			assert.NoError(t, m.ackFn(t.Context(), nil))
		}
	}()

	backfillErr := make(chan error, 1)
	go func() { backfillErr <- d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)) }()

	select {
	case b := <-batches:
		t.Fatalf("page released before the stream caught up: %v", b)
	case <-time.After(3 * releaseTick):
	}

	inc.progress.Observe("s1", time.Now().Add(time.Hour))
	inc.poke()

	select {
	case b := <-batches:
		assert.Equal(t, []string{"a"}, b)
	case <-time.After(5 * time.Second):
		t.Fatal("page not released after the stream caught up")
	}
	select {
	case err := <-backfillErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not complete")
	}
	close(d.msgChan)
	assert.True(t, tr.putContains("snapshot#complete"), "completion marker written after acks")
}

// fastReleaseTick lowers releaseTick for the test and restores it after.
func fastReleaseTick(t *testing.T) {
	t.Helper()
	prev := releaseTick
	releaseTick = 20 * time.Millisecond
	t.Cleanup(func() { releaseTick = prev })
}

func TestIncrementalBackfillCompletesOnlyAfterAcks(t *testing.T) {
	fastReleaseTick(t)
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(time.Hour))

	backfillErr := make(chan error, 1)
	go func() { backfillErr <- d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)) }()

	var m asyncMessage
	select {
	case m = <-d.msgChan:
	case <-time.After(5 * time.Second):
		t.Fatal("batch not released")
	}
	assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))

	assert.Never(t, func() bool { return tr.putContains("snapshot#complete") },
		3*releaseTick, 5*time.Millisecond, "marker must wait for the ack")
	select {
	case err := <-backfillErr:
		t.Fatalf("backfill returned before the ack: %v", err)
	default:
	}

	require.NoError(t, m.ackFn(t.Context(), nil))
	select {
	case err := <-backfillErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not complete after the ack")
	}
	assert.True(t, tr.putContains("snapshot#complete"))
}

func TestIncrementalBackfillTouchAfterHoldDrops(t *testing.T) {
	fastReleaseTick(t)
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(-time.Hour))

	batches := make(chan []string, 10)
	go func() {
		for m := range d.msgChan {
			batches <- snapshotPKs(t, m.msg)
			assert.NoError(t, m.ackFn(t.Context(), nil))
		}
	}()
	backfillErr := make(chan error, 1)
	go func() { backfillErr <- d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)) }()

	require.Eventually(t, func() bool { return inc.window.HeldItems() == 2 }, 5*time.Second, 5*time.Millisecond, "page held")
	assert.Equal(t, 1, inc.window.Touch(mustStreamKey(t, "a")))
	inc.progress.Observe("s1", time.Now().Add(time.Hour))
	inc.poke()

	select {
	case b := <-batches:
		assert.Equal(t, []string{"b"}, b)
	case <-time.After(5 * time.Second):
		t.Fatal("page not released")
	}
	select {
	case err := <-backfillErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not complete")
	}
	close(d.msgChan)
}

func TestIncrementalBackfillKeylessItemFails(t *testing.T) {
	fastReleaseTick(t)
	tr := &scanStubTransport{items: `[{"other":{"S":"x"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(time.Hour))

	err := d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no usable primary key")
	assert.Empty(t, d.msgChan, "no message emitted")
	assert.False(t, tr.putContains("snapshot#complete"))
}

func TestIncrementalBackfillAlreadyCompleteSkipsScan(t *testing.T) {
	tr := &scanStubTransport{
		items:   `[{"pk":{"S":"a"}}]`,
		getItem: `{"Item":{"Complete":{"BOOL":true}}}`,
	}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(time.Hour))

	require.NoError(t, d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)))
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 0, tr.calls, "no Scan request")
	assert.Empty(t, d.msgChan)
}

func TestIncrementalBackfillCancelResetsWindowForRerun(t *testing.T) {
	fastReleaseTick(t)
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`}
	d := newBackfillTestInput(t, tr)
	cp := newBackfillCheckpointer(t, d.dynamoClient)
	inc := settledIncrementalState(time.Now().Add(-time.Hour))

	ctx, cancel := context.WithCancel(t.Context())
	backfillErr := make(chan error, 1)
	go func() { backfillErr <- d.runIncrementalBackfill(ctx, backfillTargetFor(cp, inc)) }()
	require.Eventually(t, func() bool { return inc.window.HeldItems() == 2 }, 5*time.Second, 5*time.Millisecond, "page held")
	cancel()
	select {
	case err := <-backfillErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("backfill did not return on cancel")
	}
	assert.Equal(t, 0, inc.window.HeldItems(), "held page discarded")
	assert.Empty(t, d.msgChan)

	var got []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for m := range d.msgChan {
			got = append(got, snapshotPKs(t, m.msg)...)
			assert.NoError(t, m.ackFn(t.Context(), nil))
		}
	}()
	inc.progress.Observe("s1", time.Now().Add(time.Hour))
	require.NotPanics(t, func() {
		require.NoError(t, d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)))
	})
	close(d.msgChan)
	<-done
	assert.Equal(t, []string{"a", "b"}, got)
	assert.True(t, tr.putContains("snapshot#complete"))
}

// TestIncrementalBackfillResetThenStaleReadsStillBackfills: right after a
// stale-checkpoint reset, eventually consistent reads can still see the
// deleted marker and completed segments. The backfill's progress read must
// not, or the re-backfill is skipped or truncated.
func TestIncrementalBackfillResetThenStaleReadsStillBackfills(t *testing.T) {
	fastReleaseTick(t)
	for _, tc := range []struct {
		name   string
		marker bool
	}{
		{name: "stale marker and segments", marker: true},
		{name: "stale segments only", marker: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newMemCheckpointAPI(nil)
			api.pageSize = 1
			cp := memCheckpointer(t, api, "t", testStreamArn)
			api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#segment#0", true, nil))
			if tc.marker {
				api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#complete", true, nil))
			}
			api.freezeStale()
			require.NoError(t, cp.ResetSnapshotProgress(t.Context()))
			require.False(t, api.has(testStreamArn, "snapshot#segment#0"))

			tr := &scanStubTransport{items: `[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]`}
			d := newBackfillTestInput(t, tr)
			inc := settledIncrementalState(time.Now().Add(time.Hour))

			var got []string
			done := make(chan struct{})
			go func() {
				defer close(done)
				for m := range d.msgChan {
					got = append(got, snapshotPKs(t, m.msg)...)
					assert.NoError(t, m.ackFn(t.Context(), nil))
				}
			}()
			require.NoError(t, d.runIncrementalBackfill(t.Context(), backfillTargetFor(cp, inc)))
			close(d.msgChan)
			<-done

			tr.mu.Lock()
			calls := tr.calls
			tr.mu.Unlock()
			assert.Equal(t, 1, calls, "the reset segment is scanned again")
			assert.Equal(t, []string{"a", "b"}, got)
			assert.True(t, api.has(testStreamArn, "snapshot#complete"), "marker rewritten after the re-backfill")
		})
	}
}

func TestSnapshotProgressReadsEveryQueryPage(t *testing.T) {
	api := newMemCheckpointAPI(nil)
	api.pageSize = 1
	cp := memCheckpointer(t, api, "t", testStreamArn)
	api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#segment#0", true, nil))
	api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#segment#1", false,
		map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: "k"}}))

	progress, err := cp.SnapshotProgress(t.Context())
	require.NoError(t, err)
	require.Len(t, progress.SegmentProgress, 2, "segments on later Query pages are read")
	assert.True(t, progress.SegmentProgress[0].Complete)
	assert.False(t, progress.SegmentProgress[1].Complete)
	assert.NotNil(t, progress.SegmentProgress[1].LastKey)
}
