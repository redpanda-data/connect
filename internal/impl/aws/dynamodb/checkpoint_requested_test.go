// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRequestedRoundTrip(t *testing.T) {
	api := newMemCheckpointAPI(nil)
	cp := memCheckpointer(t, api, "t", testStreamArn)

	requested, err := cp.SnapshotRequested(t.Context())
	require.NoError(t, err)
	assert.False(t, requested)

	require.NoError(t, cp.MarkSnapshotRequested(t.Context()))

	requested, err = cp.SnapshotRequested(t.Context())
	require.NoError(t, err)
	assert.True(t, requested)
	assert.True(t, api.has(testStreamArn, "snapshot#requested"))
}

func TestMarkSnapshotCompleteClearsRequested(t *testing.T) {
	api := newMemCheckpointAPI(nil)
	cp := memCheckpointer(t, api, "t", testStreamArn)
	require.NoError(t, cp.MarkSnapshotRequested(t.Context()))

	require.NoError(t, cp.MarkSnapshotComplete(t.Context()))

	assert.True(t, api.has(testStreamArn, "snapshot#complete"))
	assert.False(t, api.has(testStreamArn, "snapshot#requested"))
}

func TestResetSnapshotProgressKeepsRequested(t *testing.T) {
	log := &eventLog{}
	api := newMemCheckpointAPI(log)
	cp := memCheckpointer(t, api, "t", testStreamArn)
	api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#segment#0", true, nil))
	api.put(testStreamArn, snapshotRow(testStreamArn, "snapshot#complete", true, nil))
	require.NoError(t, cp.MarkSnapshotRequested(t.Context()))

	require.NoError(t, cp.ResetSnapshotProgress(t.Context()))

	seg := log.index("delete " + testStreamArn + " snapshot#segment#0")
	marker := log.index("delete " + testStreamArn + " snapshot#complete")
	require.NotEqual(t, -1, seg)
	require.NotEqual(t, -1, marker)
	assert.Less(t, seg, marker, "the marker goes last")
	assert.Equal(t, -1, log.index("delete "+testStreamArn+" snapshot#requested"), "the requested row is not deleted")
	assert.True(t, api.has(testStreamArn, "snapshot#requested"))
}

func TestSnapshotRequestedUsesConsistentRead(t *testing.T) {
	api := newMemCheckpointAPI(nil)
	cp := memCheckpointer(t, api, "t", testStreamArn)
	// Eventually consistent reads see nothing from here on, so only a
	// ConsistentRead GetItem observes the row written next.
	api.freezeStale()
	require.NoError(t, cp.MarkSnapshotRequested(t.Context()))

	requested, err := cp.SnapshotRequested(t.Context())
	require.NoError(t, err)
	assert.True(t, requested)
}
