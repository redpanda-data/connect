// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckpointOffsetMerge(t *testing.T) {
	lsn1 := "1/AAAA"
	lsn2 := "1/BBBB"
	state1 := []byte("state-1")
	state2 := []byte("state-2")

	t.Run("nil fields on other never clobber non-nil fields on the receiver", func(t *testing.T) {
		// This is the exact scenario that used to lose data: a row-less
		// incremental snapshot checkpoint (LSN=nil) resolving out of order
		// must not erase a pending real batch's LSN, and a real batch with
		// no new snapshot state must not erase pending snapshot progress.
		receiver := CheckpointOffset{LSN: &lsn1, IncSnapshotState: state1}

		merged := receiver.Merge(CheckpointOffset{})
		assert.Equal(t, &lsn1, merged.LSN)
		assert.Equal(t, state1, merged.IncSnapshotState)
	})

	t.Run("non-nil fields on other overwrite the receiver", func(t *testing.T) {
		receiver := CheckpointOffset{LSN: &lsn1, IncSnapshotState: state1}

		merged := receiver.Merge(CheckpointOffset{LSN: &lsn2, IncSnapshotState: state2})
		assert.Equal(t, &lsn2, merged.LSN)
		assert.Equal(t, state2, merged.IncSnapshotState)
	})

	t.Run("merge is independent per field", func(t *testing.T) {
		receiver := CheckpointOffset{LSN: &lsn1, IncSnapshotState: nil}

		merged := receiver.Merge(CheckpointOffset{LSN: nil, IncSnapshotState: state2})
		assert.Equal(t, &lsn1, merged.LSN, "LSN must carry forward from the receiver, unaffected by other's state advancing")
		assert.Equal(t, state2, merged.IncSnapshotState)
	})

	t.Run("merging two zero values stays zero", func(t *testing.T) {
		merged := CheckpointOffset{}.Merge(CheckpointOffset{})
		assert.Nil(t, merged.LSN)
		assert.Nil(t, merged.IncSnapshotState)
	})
}
