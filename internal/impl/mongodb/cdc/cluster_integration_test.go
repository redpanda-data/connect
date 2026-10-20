// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// The tests here each start a multi-container MongoDB topology the shared
// single-node replica set cannot stand in for, so like
// TestIntegrationMongoCDCWithSnapshotShardedCluster they only run when named.

// mongoNode is one mongod or mongos container on a test network.
type mongoNode struct {
	t         *testing.T
	container testcontainers.Container
}

func startMongoNode(t *testing.T, nw *testcontainers.DockerNetwork, alias string, cmd ...string) *mongoNode {
	t.Helper()
	ctr, err := testcontainers.Run(t.Context(), "mongo:7",
		network.WithNetwork([]string{alias}, nw),
		testcontainers.WithCmd(cmd...),
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("27017/tcp").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	return &mongoNode{t: t, container: ctr}
}

// eval runs JavaScript in mongosh on the node and returns its output.
func (n *mongoNode) eval(js string) string {
	n.t.Helper()
	code, out, err := n.container.Exec(n.t.Context(), []string{"mongosh", "--quiet", "--port", "27017", "--eval", js}, tcexec.Multiplexed())
	require.NoError(n.t, err)
	b, err := io.ReadAll(out)
	require.NoError(n.t, err)
	require.Zero(n.t, code, "mongosh %q: %s", js, b)
	return strings.TrimSpace(string(b))
}

// awaitEval polls js until it prints want.
func (n *mongoNode) awaitEval(js, want string) {
	n.t.Helper()
	require.Eventually(n.t, func() bool {
		code, out, err := n.container.Exec(n.t.Context(), []string{"mongosh", "--quiet", "--port", "27017", "--eval", js}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			return false
		}
		b, _ := io.ReadAll(out)
		return strings.TrimSpace(string(b)) == want
	}, 2*time.Minute, 500*time.Millisecond, "waiting for %q to print %q", js, want)
}

// uri returns a host-reachable connection string for the node.
func (n *mongoNode) uri(direct bool) string {
	n.t.Helper()
	endpoint, err := n.container.PortEndpoint(n.t.Context(), "27017/tcp", "mongodb")
	require.NoError(n.t, err)
	if direct {
		return endpoint + "/?directConnection=true"
	}
	return endpoint
}

func connectTo(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetTimeout(30 * time.Second))
	require.NoError(t, err)
	t.Cleanup(func() {
		// t.Context() is already cancelled during cleanup.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
	})
	require.NoError(t, client.Ping(t.Context(), nil))
	return client
}

// startShardedCluster starts a config server, two single-node shards and a
// mongos, returning the mongos.
func startShardedCluster(t *testing.T) *mongoNode {
	t.Helper()
	nw, err := network.New(t.Context())
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, nw)

	cfg := startMongoNode(t, nw, "cfg", "mongod", "--configsvr", "--replSet", "cfg", "--port", "27017", "--bind_ip_all")
	cfg.eval(`rs.initiate({_id: "cfg", configsvr: true, members: [{_id: 0, host: "cfg:27017"}]})`)
	shards := []string{"sh1", "sh2"}
	for _, name := range shards {
		shard := startMongoNode(t, nw, name, "mongod", "--shardsvr", "--replSet", name, "--port", "27017", "--bind_ip_all")
		shard.eval(fmt.Sprintf(`rs.initiate({_id: %q, members: [{_id: 0, host: "%s:27017"}]})`, name, name))
		shard.awaitEval(`db.hello().isWritablePrimary`, "true")
	}
	cfg.awaitEval(`db.hello().isWritablePrimary`, "true")

	mongos := startMongoNode(t, nw, "mongos", "mongos", "--configdb", "cfg/cfg:27017", "--port", "27017", "--bind_ip_all")
	for _, name := range shards {
		mongos.eval(fmt.Sprintf(`sh.addShard("%s/%s:27017")`, name, name))
	}
	mongos.awaitEval(`db.adminCommand({listShards: 1}).shards.length`, "2")
	return mongos
}

func TestIntegrationMongoCDCIncrementalSnapshotShardedCluster(t *testing.T) {
	integration.CheckSkipExact(t)
	// Each run starts its own containers on its own network.
	t.Parallel()
	mongos := startShardedCluster(t)
	uri := mongos.uri(false)
	client := connectTo(t, uri)

	h := newIncrementalHarnessOn(t, uri, client)
	h.db.CreateShardedCollection(t, "foo")
	const initial = 1500
	docs := make([]any, 0, initial)
	for i := range initial {
		docs = append(docs, bson.M{"_id": i, "v": 0})
	}
	h.db.InsertMany(t, "foo", docs...)
	h.seed("bar", 40)
	h.db.CreateCollection(t, "signals")

	// A quiet sharded stream only advances on each shard's periodic no-op
	// write, so chunks are released about one per no-op interval: a larger
	// chunk keeps the test short. Refer to the incremental snapshot docs.
	input := strings.NewReplacer("  username: mongoadmin\n", "", "  password: secret\n", "", "chunk_size: 20", "chunk_size: 500").Replace(incrementalInput)
	stream := h.stream(input)
	wait := stream.RunAsync(t)

	// Change both shards' documents while they are read.
	rng := rand.New(rand.NewPCG(3, 4))
	deadline := time.Now().Add(3 * time.Second)
	nextID := initial
	for time.Now().Before(deadline) {
		id := rng.IntN(initial)
		switch rng.IntN(10) {
		case 0:
			h.db.DeleteByID(t, "foo", id)
		case 1:
			h.db.InsertOne(t, "foo", bson.M{"_id": nextID, "v": 0})
			nextID++
		default:
			h.db.UpdateOne(t, "foo", id, bson.M{"$inc": bson.M{"v": 1}})
		}
	}
	want := dbState(t, h.db, "foo")
	// Sharded change streams are slow to emit: refer to
	// TestIntegrationMongoCDCWithSnapshotShardedCluster.
	assert.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(want, replay(h.output).of("foo"))
	}, 3*time.Minute, 500*time.Millisecond)

	// Signals work through mongos too.
	start := time.Now()
	h.waitReads("bar", 40)
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"bar"}}})
	h.waitReads("bar", 80)
	t.Logf("signal re-read took %v", time.Since(start))
	stream.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Empty(t, got.regressed, "documents went backwards")
	assert.Equal(t, want, got.of("foo"))
	assert.Positive(t, got.readByColl["foo"])
	t.Logf("snapshot read %d of %d documents across the shards, %d at the end", got.readByColl["foo"], initial, len(want))
}

// startTwoNodeReplicaSet starts a primary and a secondary that accepts
// failpoints, returning both.
func startTwoNodeReplicaSet(t *testing.T) (primary, secondary *mongoNode) {
	t.Helper()
	nw, err := network.New(t.Context())
	require.NoError(t, err)
	testcontainers.CleanupNetwork(t, nw)

	args := []string{"mongod", "--replSet", "rs0", "--port", "27017", "--bind_ip_all", "--setParameter", "enableTestCommands=1"}
	primary = startMongoNode(t, nw, "rs-a", args...)
	secondary = startMongoNode(t, nw, "rs-b", args...)
	primary.eval(`rs.initiate({_id: "rs0", members: [{_id: 0, host: "rs-a:27017", priority: 2}, {_id: 1, host: "rs-b:27017", priority: 1}]})`)
	primary.awaitEval(`db.hello().isWritablePrimary`, "true")
	secondary.awaitEval(`db.hello().secondary`, "true")
	return primary, secondary
}

// TestIntegrationIncSnapshotDepsMajorityReads shows the chunk reads are
// majority reads: with replication to the only secondary stopped, a write the
// primary has applied but a majority has not must stay invisible to them. A
// local read would hand it to a chunk released before its change streams, or
// before a failover rolls it back.
func TestIntegrationIncSnapshotDepsMajorityReads(t *testing.T) {
	integration.CheckSkipExact(t)
	// Each run starts its own containers on its own network.
	t.Parallel()
	primary, secondary := startTwoNodeReplicaSet(t)
	client := connectTo(t, primary.uri(true))
	db := client.Database("majority")
	ctx := t.Context()

	_, err := db.Collection("c").InsertMany(ctx, []any{
		bson.M{"_id": 1, "v": 0}, bson.M{"_id": 2, "v": 0}, bson.M{"_id": 3, "v": 0},
	})
	require.NoError(t, err)

	secondary.eval(`db.adminCommand({configureFailPoint: "stopReplProducer", mode: "alwaysOn"})`)

	// Applied on the primary only: a majority of two needs the secondary.
	w1 := db.Collection("c", options.Collection().SetWriteConcern(writeconcern.W1()))
	_, err = w1.InsertOne(ctx, bson.M{"_id": 4, "v": 0})
	require.NoError(t, err)
	_, err = w1.UpdateOne(ctx, bson.M{"_id": 2}, bson.M{"$set": bson.M{"v": 1}})
	require.NoError(t, err)

	// Sanity: a local read does see them, so the failpoint held.
	local := db.Collection("c", options.Collection().SetReadConcern(readconcern.Local()))
	n, err := local.CountDocuments(ctx, bson.M{"_id": 4})
	require.NoError(t, err)
	require.EqualValues(t, 1, n, "the unreplicated write is not even locally visible")

	deps := &incSnapshotDeps{
		db:                  db,
		watermarkCollection: "c",
		streamedThrough:     func() bson.Timestamp { return bson.Timestamp{} },
	}
	table := incrementalsnapshot.TableID{Schema: db.Name(), Table: "c"}

	maxKey, err := deps.ResolveMaxKey(ctx, table, nil)
	require.NoError(t, err)
	assert.Equal(t, key(t, 3), maxKey, "the max key saw an unreplicated insert")

	rows, err := deps.FetchChunk(ctx, table, nil, nil, key(t, 4), 10)
	require.NoError(t, err)
	var ids []int32
	for _, r := range rows {
		ids = append(ids, r.Data["_id"].(int32))
		if r.Data["_id"].(int32) == 2 {
			assert.EqualValues(t, 0, r.Data["v"], "the chunk saw an unreplicated update")
		}
	}
	assert.Equal(t, []int32{1, 2, 3}, ids, "the chunk saw an unreplicated insert")

	// Once replication resumes the write becomes majority and visible.
	secondary.eval(`db.adminCommand({configureFailPoint: "stopReplProducer", mode: "off"})`)
	require.Eventually(t, func() bool {
		maxKey, err := deps.ResolveMaxKey(ctx, table, nil)
		return err == nil && assert.ObjectsAreEqual(key(t, 4), maxKey)
	}, time.Minute, 250*time.Millisecond)
}
