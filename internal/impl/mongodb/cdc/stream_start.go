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

// streamStart is where one connection's MongoDB change stream opens, and the
// time that the blocking snapshot puts on the documents it reads.
//
// Connect computes it once per connection with resolveStreamStart. The stream
// goroutine reads it.
type streamStart struct {
	// token is the MongoDB change stream resume token that the stream opens
	// after (SetResumeAfter).
	//
	// It is one of:
	//   - The loaded checkpoint, on a restart.
	//   - The current position of the change stream, captured on a first start.
	//     resolveStreamStart says when it captures one.
	//   - Nil, when there is no checkpoint and no captured token. The stream
	//     then starts from opTime: it returns only the changes after that time.
	token bson.Raw

	// opTime is the time of the stream start, from the hello reply:
	//   - On a replica set, lastWrite.majorityOpTime.ts: the timestamp of the
	//     newest oplog entry that a majority of the members has written.
	//   - On a sharded cluster, operationTime: the cluster time of the hello
	//     command. mongos has no oplog, so its reply has no lastWrite.
	opTime bson.Timestamp
}

// helloReply is the reply to the hello command. A client runs hello to learn
// the role of the server it is connected to and the topology around it: for
// example, whether it is a primary, a secondary, or a mongos router. On a
// replica set member, the reply also has lastWrite, the optimes of the latest
// oplog writes.
//
// See https://www.mongodb.com/docs/manual/reference/command/hello/.
type helloReply bson.M

// majorityOpTime returns lastWrite.majorityOpTime.ts.
//
// It is the timestamp of the newest oplog entry that a majority of the replica
// set members has written.
//
// The bool is false when the reply has no such timestamp. This is the case on a
// sharded cluster: mongos replies without lastWrite, because it is a router
// with no oplog, and each shard keeps its own.
func (h helloReply) majorityOpTime() (bson.Timestamp, bool) {
	ts, ok := bsonGetPath(bson.M(h), "lastWrite", "majorityOpTime", "ts").(bson.Timestamp)
	return ts, ok
}

// resolveStreamStart decides where the change stream opens. Arguments:
//   - checkpointToken is the loaded checkpoint, nil on a first start.
//   - hello is the reply to the hello command. lastWrite.majorityOpTime.ts
//     is read, else operationTime.
//   - captureToken reads the current change stream position. Connect passes
//     getCurrentResumeToken, which opens a short-lived change stream and
//     returns its resume token. It is a function so that the short-lived
//     change stream opens only when a token is needed.
func (m *mongoCDC) resolveStreamStart(
	ctx context.Context,
	checkpointToken bson.Raw,
	hello helloReply,
	captureToken func(context.Context) (bson.Raw, error),
) (streamStart, error) {
	ts, hasTS := hello.majorityOpTime()
	start := streamStart{token: checkpointToken, opTime: ts}
	if !hasTS {
		// mongos has no oplog, but its reply has the cluster time of the
		// command. A change stream through mongos orders the events of all
		// shards on this clock, so it is a valid time for the snapshot. hasTS
		// stays false: the stream still opens at a token, not at this time.
		start.opTime, _ = hello["operationTime"].(bson.Timestamp)
	}
	if checkpointToken != nil {
		// Resume from the checkpoint. No snapshot runs.
		return start, nil
	}

	if m.snapshotParallelism == 0 && hasTS {
		// No blocking snapshot runs, so nothing is checkpointed before the
		// first event, and the oplog time is enough to open the stream. This
		// includes the incremental snapshot: a majority chunk read sees every
		// write up to the oplog time, and the stream returns every write after
		// it. A captured token can be ahead of the oplog time, and the stream
		// would then skip writes that a majority read does not see yet.
		return start, nil
	}

	// Capture a token. Two cases get here:
	//   - A blocking snapshot runs. The checkpoint cache stores only tokens, so
	//     the end of the snapshot needs a token to checkpoint.
	//   - hello has no oplog time (mongos). A token is the only start position.
	token, err := captureToken(ctx)
	switch {
	case err == nil:
		start.token = token
	case !hasTS:
		return streamStart{}, fmt.Errorf("unable to compute stream start position: %w (%s)", err, bson.M(hello).String())
	default:
		// The capture failed, but the oplog time can still open the stream.
		// The capture fails on replica sets before 4.0.7, which report no
		// post-batch resume token, and on a transient error. There is no token
		// to store when the blocking snapshot completes, so a restart before
		// the first stream checkpoint runs the snapshot again.
		m.logger.Debugf("unable to capture a pre-snapshot resume token, a completed snapshot will not be checkpointed: %v", err)
	}
	return start, nil
}
