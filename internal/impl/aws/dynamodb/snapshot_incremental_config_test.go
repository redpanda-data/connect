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
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/aws/dynamodb/incrementalsnapshot"
)

// parseCDCConfig parses and validates yaml the way the input constructor does.
func parseCDCConfig(t *testing.T, yaml string) (dynamoDBCDCConfig, error) {
	t.Helper()
	pConf, err := dynamoDBCDCInputConfig().ParseYAML(yaml, service.NewEnvironment())
	require.NoError(t, err)
	conf, err := dynamoCDCInputConfigFromParsed(pConf)
	if err != nil {
		return conf, err
	}
	return conf, validateDynamoDBCDCConfig(conf)
}

func TestIncrementalConfigAllowedWithMultiTable(t *testing.T) {
	conf, err := parseCDCConfig(t, `
tables: [a, b]
table_discovery_mode: includelist
checkpoint_table: ckpt
snapshot_mode: incremental
`)
	require.NoError(t, err)
	assert.Equal(t, snapshotModeIncremental, conf.snapshot.mode)
	assert.Equal(t, 2*time.Second, conf.snapshot.watermarkMargin)
}

func TestIncrementalConfigOtherModesStillRejectMultiTable(t *testing.T) {
	for _, mode := range []string{snapshotModeOnly, snapshotModeAndCDC} {
		t.Run(mode, func(t *testing.T) {
			_, err := parseCDCConfig(t, `
tables: [a, b]
table_discovery_mode: includelist
checkpoint_table: ckpt
snapshot_mode: `+mode+`
`)
			require.Error(t, err)
		})
	}
}

func TestIncrementalConfigNegativeMarginRejected(t *testing.T) {
	_, err := parseCDCConfig(t, `
tables: [a]
checkpoint_table: ckpt
snapshot_mode: incremental
snapshot_watermark_margin: -1s
`)
	require.Error(t, err)
}

// The stale-read case (a reset followed by eventually consistent reads that
// still see the old rows) is covered by
// TestIncrementalBackfillResetThenStaleReadsStillBackfills.
func TestResetSnapshotProgressDeletesSegmentsThenMarker(t *testing.T) {
	tr := &scanStubTransport{
		getItem: `{"Item":{"ShardID":{"S":"snapshot#complete"},"Complete":{"BOOL":true}}}`,
		query:   `{"Items":[{"ShardID":{"S":"snapshot#segment#0"},"Complete":{"BOOL":true}},{"ShardID":{"S":"snapshot#segment#1"},"LastKey":{"M":{"pk":{"S":"a"}}},"Complete":{"BOOL":false}}]}`,
	}
	cp := newBackfillCheckpointer(t, newStubDynamoClient(tr))

	require.NoError(t, cp.ResetSnapshotProgress(t.Context()))

	deletes := tr.deleteBodies()
	require.Len(t, deletes, 3, "both segment rows and the marker are deleted, the requested row is kept")
	assert.Contains(t, deletes[0], "snapshot#segment#0")
	assert.Contains(t, deletes[1], "snapshot#segment#1")
	assert.Contains(t, deletes[2], "snapshot#complete", "the marker is deleted last")
	for _, b := range deletes {
		assert.True(t, strings.Contains(b, `"TableName":"ckpt"`), b)
	}

	queries := tr.queryBodies()
	require.Len(t, queries, 1)
	var q struct{ ConsistentRead *bool }
	require.NoError(t, json.Unmarshal([]byte(queries[0]), &q))
	require.NotNil(t, q.ConsistentRead)
	assert.True(t, *q.ConsistentRead, "the segment query is strongly consistent")
}

func TestPrepareIncrementalBackfillIncompleteSnapshotRuns(t *testing.T) {
	tr := &scanStubTransport{}
	d := newBackfillTestInput(t, tr)
	tgt := backfillTarget{table: "t", checkpointer: newBackfillCheckpointer(t, d.dynamoClient), state: incsnapshot.NewState(0, time.Minute)}

	skip, reset, err := d.prepareIncrementalBackfill(t.Context(), tgt, aws.String(testStreamArn))
	require.NoError(t, err)
	assert.False(t, skip)
	assert.False(t, reset)
	assert.Empty(t, tr.deleteBodies(), "nothing to reset for an incomplete snapshot")
}

func TestShardCoordinatorWaitsForMsgSendersBeforeClosing(t *testing.T) {
	d := newBackfillTestInput(t, &scanStubTransport{})
	d.shardReaders = map[string]*dynamoDBShardReader{}
	d.shardRefreshCh = make(chan struct{}, 1)
	d.msgSenders.Add(1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := make(chan struct{})
	go func() {
		d.startShardCoordinator(ctx)
		close(done)
	}()

	select {
	case <-d.shutSig.HasStoppedChan():
		t.Fatal("coordinator stopped while a sender was still registered")
	case <-time.After(50 * time.Millisecond):
	}
	d.msgSenders.Done()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coordinator did not stop after the sender finished")
	}
	_, open := <-d.msgChan
	assert.False(t, open, "msgChan is closed once the sender is done")
}

func TestIncrementalConfigIdleShardGrace(t *testing.T) {
	const base = `
tables: [a]
checkpoint_table: ckpt
snapshot_mode: incremental
`
	t.Run("default", func(t *testing.T) {
		conf, err := parseCDCConfig(t, base)
		require.NoError(t, err)
		assert.Equal(t, time.Minute, conf.snapshot.idleShardGrace)
	})
	t.Run("explicit", func(t *testing.T) {
		conf, err := parseCDCConfig(t, base+"snapshot_idle_shard_grace: 30s\n")
		require.NoError(t, err)
		assert.Equal(t, 30*time.Second, conf.snapshot.idleShardGrace)
	})
	for _, v := range []string{"0s", "-1s"} {
		t.Run("rejects "+v, func(t *testing.T) {
			_, err := parseCDCConfig(t, base+"snapshot_idle_shard_grace: "+v+"\n")
			require.ErrorContains(t, err, "snapshot_idle_shard_grace must be greater than 0")
		})
	}
}
