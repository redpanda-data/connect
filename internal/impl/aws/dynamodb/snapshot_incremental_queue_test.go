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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackfillQueueFIFOAndDedup(t *testing.T) {
	q := newBackfillQueue()
	q.Push("a")
	q.Push("b")
	q.Push("a")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := []string{}
	for range 2 {
		tbl, ok := q.Pop(ctx)
		assert.True(t, ok)
		got = append(got, tbl)
	}
	assert.Equal(t, []string{"a", "b"}, got)
	assert.Equal(t, 0, q.Len())
}

func TestBackfillQueuePopBlocksUntilPushOrCancel(t *testing.T) {
	q := newBackfillQueue()
	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(20 * time.Millisecond); q.Push("late") }()
	tbl, ok := q.Pop(ctx)
	assert.True(t, ok)
	assert.Equal(t, "late", tbl)
	cancel()
	_, ok = q.Pop(ctx)
	assert.False(t, ok)
}

// TestPrepareTableBackfillSkipsKeysOnlyTable: the stream-view check runs
// when a table is prepared, before its coordinator starts, so a KEYS_ONLY
// table is never queued; a NEW_IMAGE table with no completed snapshot is.
func TestPrepareTableBackfillSkipsKeysOnlyTable(t *testing.T) {
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newBackfillTestInput(t, tr)
	keySchema := []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk")}}
	newTS := func(name string, view dynamodbtypes.StreamViewType) *tableStream {
		return &tableStream{
			tableName:    name,
			streamArn:    testStreamArn,
			keySchema:    keySchema,
			checkpointer: newBackfillCheckpointer(t, d.dynamoClient),
			streamSpec:   &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: view},
			incremental:  settledIncrementalState(time.Now().Add(time.Hour)),
		}
	}

	assert.False(t, d.prepareTableBackfill(t.Context(), "t1", newTS("t1", dynamodbtypes.StreamViewTypeKeysOnly)),
		"a KEYS_ONLY table is not queued")
	assert.True(t, d.prepareTableBackfill(t.Context(), "t2", newTS("t2", dynamodbtypes.StreamViewTypeNewImage)),
		"a NEW_IMAGE table without a completed snapshot is queued")
	assert.False(t, d.prepareTableBackfill(t.Context(), "t3", &tableStream{tableName: "t3"}),
		"a table outside incremental mode is not queued")
	tr.mu.Lock()
	assert.Equal(t, 0, tr.calls, "preparing never scans")
	tr.mu.Unlock()
}

func TestRunBackfillQueueMovesPastStoppedCoordinator(t *testing.T) {
	fastReleaseTick(t)
	tr := &scanStubTransport{items: `[{"pk":{"S":"a"}}]`}
	d := newBackfillTestInput(t, tr)
	keySchema := []dynamodbtypes.KeySchemaElement{{AttributeName: aws.String("pk")}}
	newTS := func(name string, inc *incrementalState) *tableStream {
		return &tableStream{
			tableName:       name,
			streamArn:       testStreamArn,
			keySchema:       keySchema,
			checkpointer:    newBackfillCheckpointer(t, d.dynamoClient),
			streamSpec:      &dynamodbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: dynamodbtypes.StreamViewTypeNewImage},
			incremental:     inc,
			coordinatorDone: make(chan struct{}),
		}
	}
	// t1's shard list is never settled, so its page would be held forever.
	stuck := newIncrementalState(0, time.Minute)
	stuck.Register("s1")
	t1 := newTS("t1", stuck)
	t2 := newTS("t2", settledIncrementalState(time.Now().Add(time.Hour)))
	t3 := newTS("t3", settledIncrementalState(time.Now().Add(time.Hour)))
	d.tableStreams = map[string]*tableStream{"t1": t1, "t2": t2, "t3": t3}

	// The first Scan is t1's: its coordinator stops shortly after.
	var once sync.Once
	tr.onScan = func() {
		once.Do(func() {
			time.AfterFunc(50*time.Millisecond, func() { close(t1.coordinatorDone) })
		})
	}

	d.backfills = newBackfillQueue()
	d.backfills.Push("t1")
	d.backfills.Push("t2")
	// t3 proves the queue also moves on after a table that completes
	// normally while its coordinator is still running.
	d.backfills.Push("t3")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runnerDone := make(chan struct{})
	go func() {
		defer close(runnerDone)
		d.runBackfillQueue(ctx)
	}()

	for _, want := range []string{"t2", "t3"} {
		var m asyncMessage
		select {
		case m = <-d.msgChan:
		case <-time.After(5 * time.Second):
			t.Fatalf("queue did not reach table %s", want)
		}
		tbl, ok := m.msg[0].MetaGet("dynamodb_table")
		require.True(t, ok)
		assert.Equal(t, want, tbl, "t1 emits nothing")
		assert.Equal(t, []string{"a"}, snapshotPKs(t, m.msg))
		require.NoError(t, m.ackFn(t.Context(), nil))
	}

	require.Eventually(t, func() bool { return completeMarkers(tr) == 2 },
		5*time.Second, 5*time.Millisecond, "t2 and t3 backfills complete")
	assert.Equal(t, 0, stuck.window.HeldItems(), "t1's held page discarded")
	assert.Empty(t, d.msgChan)
	select {
	case <-runnerDone:
		t.Fatal("runner exited before shutdown")
	default:
	}

	cancel()
	select {
	case <-runnerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not return on shutdown")
	}
}

// completeMarkers counts the recorded PutItem bodies writing a snapshot
// completion marker.
func completeMarkers(tr *scanStubTransport) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	n := 0
	for _, b := range tr.puts {
		if strings.Contains(b, "snapshot#complete") {
			n++
		}
	}
	return n
}
