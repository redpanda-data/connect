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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestResolveStreamStart covers where a connection's change stream opens. A
// checkpoint token wins. On a first start, a blocking snapshot or a sharded
// cluster (no oplog time in the hello reply) needs a captured token, and every
// other first start opens at the majority committed oplog time.
//
// The incremental snapshot stores no token before the first event, so on a
// replica set it opens at the majority committed oplog time too. A captured
// token can be ahead of that time: the stream would then skip a write that the
// majority chunk reads do not see yet.
func TestResolveStreamStart(t *testing.T) {
	checkpointToken, err := bson.Marshal(bson.M{"_data": "checkpoint"})
	require.NoError(t, err)
	capturedToken, err := bson.Marshal(bson.M{"_data": "captured"})
	require.NoError(t, err)
	replicaSetHello := helloReply{"lastWrite": bson.M{"majorityOpTime": bson.M{"ts": bson.Timestamp{T: 100, I: 2}}}}
	shardedHello := helloReply{"msg": "isdbgrid", "operationTime": bson.Timestamp{T: 200, I: 3}}

	tests := []struct {
		name                string
		snapshotParallelism int
		incremental         bool
		checkpointToken     bson.Raw
		hello               helloReply
		captureErr          error
		wantCaptured        bool
		want                streamStart
		wantErr             string
	}{
		{
			name:                "checkpoint token wins over a blocking snapshot",
			snapshotParallelism: 1,
			checkpointToken:     checkpointToken,
			hello:               replicaSetHello,
			want: streamStart{
				token:  checkpointToken,
				opTime: bson.Timestamp{T: 100, I: 2},
			},
		},
		{
			name:            "checkpoint token wins on a sharded cluster",
			checkpointToken: checkpointToken,
			hello:           shardedHello,
			want: streamStart{
				token:  checkpointToken,
				opTime: bson.Timestamp{T: 200, I: 3},
			},
		},
		{
			name:  "first start without a snapshot opens at the oplog time",
			hello: replicaSetHello,
			want: streamStart{
				opTime: bson.Timestamp{T: 100, I: 2},
			},
		},
		{
			name:                "first start with a blocking snapshot opens at a captured token",
			snapshotParallelism: 1,
			hello:               replicaSetHello,
			wantCaptured:        true,
			want: streamStart{
				token:  capturedToken,
				opTime: bson.Timestamp{T: 100, I: 2},
			},
		},
		{
			name:                "first start with a blocking snapshot falls back to the oplog time without a token",
			snapshotParallelism: 1,
			hello:               replicaSetHello,
			captureErr:          errors.New("no post-batch resume token"),
			wantCaptured:        true,
			want: streamStart{
				opTime: bson.Timestamp{T: 100, I: 2},
			},
		},
		{
			name:        "first start with an incremental snapshot opens at the oplog time",
			incremental: true,
			hello:       replicaSetHello,
			want: streamStart{
				opTime: bson.Timestamp{T: 100, I: 2},
			},
		},
		{
			name:         "first start with an incremental snapshot on a sharded cluster opens at a captured token",
			incremental:  true,
			hello:        shardedHello,
			wantCaptured: true,
			want: streamStart{
				token:  capturedToken,
				opTime: bson.Timestamp{T: 200, I: 3},
			},
		},
		{
			name:         "first start on a sharded cluster opens at a captured token",
			hello:        shardedHello,
			wantCaptured: true,
			want: streamStart{
				token:  capturedToken,
				opTime: bson.Timestamp{T: 200, I: 3},
			},
		},
		{
			name:                "first start with a blocking snapshot on a sharded cluster has the cluster time",
			snapshotParallelism: 1,
			hello: helloReply{
				"msg":           "isdbgrid",
				"operationTime": bson.Timestamp{T: 300, I: 4},
			},
			wantCaptured: true,
			want: streamStart{
				token:  capturedToken,
				opTime: bson.Timestamp{T: 300, I: 4},
			},
		},
		{
			name:         "first start on a sharded cluster fails without a token",
			hello:        shardedHello,
			captureErr:   errors.New("no post-batch resume token"),
			wantCaptured: true,
			wantErr:      "unable to compute the stream start position: the hello reply has no lastWrite.majorityOpTime.ts, and capturing a resume token failed: no post-batch resume token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mongoCDC{
				logger:              service.MockResources().Logger(),
				snapshotParallelism: tt.snapshotParallelism,
				incremental:         tt.incremental,
			}
			captured := false
			captureToken := func(context.Context) (bson.Raw, error) {
				captured = true
				if tt.captureErr != nil {
					return nil, tt.captureErr
				}
				return capturedToken, nil
			}

			got, err := m.resolveStreamStart(t.Context(), tt.checkpointToken, tt.hello, captureToken)

			require.Equal(t, tt.wantCaptured, captured)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
