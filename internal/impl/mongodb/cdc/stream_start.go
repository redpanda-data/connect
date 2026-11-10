// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// streamStart is the position one connection's change stream opens from, and
// the inputs of the blocking snapshot that runs before it. Connect resolves it
// once per connection with resolveStreamStart, and the stream goroutine reads
// it.
type streamStart struct {
	// token is the change stream resume token the stream opens after
	// (SetResumeAfter): the checkpoint token, else the position captured
	// before the snapshot. Nil opens the stream at the operation after opTime.
	// When a blocking snapshot runs, no checkpoint was loaded, so token is the
	// captured position that storeSnapshotCheckpoint stores when the snapshot
	// completes.
	token bson.Raw
	// opTime is lastWrite.majorityOpTime.ts from the hello reply: the timestamp
	// of the newest oplog entry that a majority of the replica set members has
	// written. With a nil token, the stream opens at the operation after it.
	// The blocking snapshot puts it in the operation_time metadata of each
	// document it reads, but does not read at it. It is zero on a sharded
	// cluster, because mongos reports no lastWrite, but there token is never
	// nil.
	opTime bson.Timestamp
}

// resolveStreamStart decides where the change stream opens. Arguments:
//   - checkpointToken is the loaded checkpoint, nil on a first start.
//   - hello is the reply to the hello command. Only
//     lastWrite.majorityOpTime.ts is read. On a sharded cluster, mongos
//     replies without lastWrite: it is a router with no oplog, and each shard
//     keeps its own.
//   - captureToken reads the current change stream position. It is a function
//     so that it runs only when a snapshot is about to run or hello has no
//     timestamp.
func (m *mongoCDC) resolveStreamStart(
	ctx context.Context,
	checkpointToken bson.Raw,
	hello bson.M,
	captureToken func(context.Context) (bson.Raw, error),
) (streamStart, error) {
	ts, hasTS := bsonGetPath(hello, "lastWrite", "majorityOpTime", "ts").(bson.Timestamp)
	start := streamStart{token: checkpointToken, opTime: ts}
	if checkpointToken != nil {
		// Resume from the checkpoint. No snapshot runs.
		return start, nil
	}
	if !m.readsSnapshot() && hasTS {
		// Nothing to checkpoint before the first event: open at the oplog time.
		return start, nil
	}
	// A snapshot runs, or hello has no oplog time (mongos). Only a token can be
	// stored in the checkpoint cache, and on mongos it is the only start
	// position, so capture one.
	token, err := captureToken(ctx)
	switch {
	case err == nil:
		start.token = token
	case !hasTS:
		return streamStart{}, fmt.Errorf("unable to compute stream start position: %w (%s)", err, hello.String())
	default:
		// Replica sets before 4.0.7 may not report a post-batch resume
		// token. The oplog timestamp still positions this run, but there is
		// nothing storable, so the post-snapshot checkpoint is skipped and
		// a failure re-runs the snapshot as before.
		m.logger.Debugf("unable to capture a pre-snapshot resume token, a completed snapshot will not be checkpointed: %v", err)
	}
	return start, nil
}
