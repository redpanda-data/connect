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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/mongodb"
)

// TestIntegrationMongoCDCRefreshesCredentialsAfterSnapshot pins the refresh this
// input performs that the other components have no need for: after the snapshot
// completes, and before streaming starts, a role-assuming configuration rebuilds
// its client. The snapshot is not checkpointed as it goes, so it can run for a
// large fraction of an STS session; entering the change-stream phase on those
// same credentials risks starting with a session that is already expired.
//
// The credential builder is counted through the exported mongodb.AWSOptFn seam.
// It returns a (nil, nil) credential, which ClientConfig.Connect treats as "apply
// no auth" (`if cred != nil { opt.SetAuth(*cred) }`, verified in common.go), so
// the container is run without authentication - a MONGODB-AWS credential could
// not authenticate against it, and any credential at all would fail the
// handshake.
func TestIntegrationMongoCDCRefreshesCredentialsAfterSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainerWithoutAuth(t)

	var builds atomic.Int64
	prev := mongodb.AWSOptFn
	t.Cleanup(func() { mongodb.AWSOptFn = prev })
	mongodb.AWSOptFn = func(*service.ParsedConfig, *service.Logger) (mongodb.CredentialBuilder, error) {
		return func(context.Context) (*options.Credential, error) {
			builds.Add(1)
			return nil, nil
		}, nil
	}

	db := &databaseHelper{mongoClient.Database("test")}
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "world"})

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(`
mongodb_cdc:
  url: '`+uri+`'
  database: 'test'
  checkpoint_cache: 'filecache'
  stream_snapshot: true
  json_marshal_mode: relaxed
  collections:
    - 'foo'
  aws:
    enabled: true
    region: us-east-1
    role: arn:aws:iam::123456789012:role/foo
`))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+t.TempDir()+`'`))
	output := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
	logs := &logCapture{}
	builder.SetLogger(slog.New(logs))
	stream := &streamHelper{builder: builder}

	wait := stream.RunAsync(t)
	t.Cleanup(wait)

	// Both snapshot documents arriving means the snapshot completed, which is the
	// point the refresh happens at.
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) >= 2
	}, 60*time.Second, 250*time.Millisecond, "the snapshot never delivered the seeded documents")

	// The refresh runs after the post-snapshot checkpoint, so wait for the count
	// rather than reading it immediately: delivery only proves the snapshot's
	// batches were consumed.
	require.Eventually(t, func() bool {
		return builds.Load() >= 2
	}, 60*time.Second, 250*time.Millisecond,
		"expected a credential rebuild after the snapshot, got %d build(s)", builds.Load())
	t.Logf("credential builds: %d", builds.Load())
	require.NotEmpty(t, logs.matching("refreshing IAM credentials before streaming"),
		"the refresh must be the reason for the rebuild, captured logs: %v", logs.matching(""))

	// Streaming works on the refreshed client, so the rebuild produced a usable
	// connection rather than merely being attempted.
	db.InsertOne(t, "foo", bson.M{"_id": 3, "data": "streamed"})
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) >= 3
	}, 60*time.Second, 250*time.Millisecond, "streaming did not continue on the refreshed client")

	stream.StopWithin(t, 30*time.Second)
}
