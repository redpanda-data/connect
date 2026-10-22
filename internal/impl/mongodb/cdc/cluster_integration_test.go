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
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
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

// The tests here need multi-container MongoDB topologies the shared
// single-node replica set cannot stand in for.

// runMongoNode starts one mongod or mongos container on nw. The container is
// returned even with an error, so the caller can still remove it.
func runMongoNode(ctx context.Context, nw *testcontainers.DockerNetwork, alias string, cmd ...string) (*testcontainers.DockerContainer, error) {
	return testcontainers.Run(ctx, "mongo:7",
		network.WithNetwork([]string{alias}, nw),
		testcontainers.WithCmd(cmd...),
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("27017/tcp").WithStartupTimeout(2*time.Minute)),
	)
}

// evalOn runs JavaScript in mongosh on ctr and returns its output.
func evalOn(ctx context.Context, ctr testcontainers.Container, js string) (string, error) {
	code, out, err := ctr.Exec(ctx, []string{"mongosh", "--quiet", "--port", "27017", "--eval", js}, tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	b, err := io.ReadAll(out)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("mongosh %q exited with %d: %s", js, code, b)
	}
	return strings.TrimSpace(string(b)), nil
}

// awaitEvalOn polls js on ctr until it prints want.
func awaitEvalOn(ctx context.Context, ctr testcontainers.Container, js, want string) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		got, err := evalOn(ctx, ctr, js)
		if err == nil && got == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waiting for %q to print %q: last output %q, error %v", js, want, got, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// mongoNode is one mongod container on a test network, owned by one test.
type mongoNode struct {
	t         *testing.T
	container testcontainers.Container
}

func startMongoNode(t *testing.T, nw *testcontainers.DockerNetwork, alias string, cmd ...string) *mongoNode {
	t.Helper()
	ctr, err := runMongoNode(t.Context(), nw, alias, cmd...)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	return &mongoNode{t: t, container: ctr}
}

// eval runs JavaScript in mongosh on the node and returns its output.
func (n *mongoNode) eval(js string) string {
	n.t.Helper()
	out, err := evalOn(n.t.Context(), n.container, js)
	require.NoError(n.t, err)
	return out
}

// awaitEval polls js until it prints want.
func (n *mongoNode) awaitEval(js, want string) {
	n.t.Helper()
	require.NoError(n.t, awaitEvalOn(n.t.Context(), n.container, js, want))
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

// sharedSharded is the package-wide sharded cluster: a config server, two
// single-node shards and a mongos. It boots on first use and TestMain removes
// it. Tests share it safely because each uses its own database, and the only
// cluster-wide commands they run (enableSharding, shardCollection) are scoped
// to that database.
var sharedSharded sharedShardedCluster

type sharedShardedCluster struct {
	once  sync.Once
	uri   string
	err   error
	nodes []*testcontainers.DockerContainer
	nw    *testcontainers.DockerNetwork
}

// start boots the cluster on first use and returns the mongos URI.
func (s *sharedShardedCluster) start() (string, error) {
	s.once.Do(func() {
		s.uri, s.err = s.boot(context.Background()) // not t.Context(): the cluster outlives individual tests
	})
	return s.uri, s.err
}

func (s *sharedShardedCluster) boot(ctx context.Context) (string, error) {
	nw, err := network.New(ctx)
	if err != nil {
		return "", err
	}
	s.nw = nw
	run := func(alias string, cmd ...string) (*testcontainers.DockerContainer, error) {
		ctr, err := runMongoNode(ctx, nw, alias, cmd...)
		if ctr != nil {
			s.nodes = append(s.nodes, ctr)
		}
		return ctr, err
	}

	cfg, err := run("cfg", "mongod", "--configsvr", "--replSet", "cfg", "--port", "27017", "--bind_ip_all")
	if err != nil {
		return "", err
	}
	if _, err := evalOn(ctx, cfg, `rs.initiate({_id: "cfg", configsvr: true, members: [{_id: 0, host: "cfg:27017"}]})`); err != nil {
		return "", err
	}
	shards := []string{"sh1", "sh2"}
	for _, name := range shards {
		shard, err := run(name, "mongod", "--shardsvr", "--replSet", name, "--port", "27017", "--bind_ip_all")
		if err != nil {
			return "", err
		}
		if _, err := evalOn(ctx, shard, fmt.Sprintf(`rs.initiate({_id: %q, members: [{_id: 0, host: "%s:27017"}]})`, name, name)); err != nil {
			return "", err
		}
		if err := awaitEvalOn(ctx, shard, `db.hello().isWritablePrimary`, "true"); err != nil {
			return "", err
		}
	}
	if err := awaitEvalOn(ctx, cfg, `db.hello().isWritablePrimary`, "true"); err != nil {
		return "", err
	}

	mongos, err := run("mongos", "mongos", "--configdb", "cfg/cfg:27017", "--port", "27017", "--bind_ip_all")
	if err != nil {
		return "", err
	}
	for _, name := range shards {
		if _, err := evalOn(ctx, mongos, fmt.Sprintf(`sh.addShard("%s/%s:27017")`, name, name)); err != nil {
			return "", err
		}
	}
	if err := awaitEvalOn(ctx, mongos, `db.adminCommand({listShards: 1}).shards.length`, "2"); err != nil {
		return "", err
	}
	return mongos.PortEndpoint(ctx, "27017/tcp", "mongodb")
}

// terminate removes the cluster's containers and network, if any were
// started. It is called from TestMain once every test has finished.
func (s *sharedShardedCluster) terminate() error {
	ctx := context.Background()
	var errs []error
	for _, ctr := range s.nodes {
		errs = append(errs, ctr.Terminate(ctx))
	}
	if s.nw != nil {
		errs = append(errs, s.nw.Remove(ctx))
	}
	return errors.Join(errs...)
}

// startShardedCluster returns the URI of the shared sharded cluster's mongos.
func startShardedCluster(t *testing.T) string {
	t.Helper()
	uri, err := sharedSharded.start()
	require.NoError(t, err)
	return uri
}

func TestIntegrationMongoCDCIncrementalSnapshotShardedCluster(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()
	uri := startShardedCluster(t)
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

	// Change both shards' documents while they are read: for at least
	// minWrite, then until a read of foo has been emitted after a streamed
	// change, as in TestIntegrationMongoCDCIncrementalSnapshotConcurrentWrites.
	// bar is read after foo, so only foo's reads count.
	const minWrite, maxWrite = 3 * time.Second, 30 * time.Second
	rng := rand.New(rand.NewPCG(3, 4))
	start := time.Now()
	nextID := initial
	for i := 0; ; i++ {
		if elapsed := time.Since(start); elapsed >= maxWrite ||
			(elapsed >= minWrite && i%20 == 0 && replay(h.output).interleaved["foo"]) {
			break
		}
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
	start = time.Now()
	h.waitReads("bar", 40)
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"bar"}}})
	h.waitReads("bar", 80)
	t.Logf("signal re-read took %v", time.Since(start))
	stream.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.True(t, got.interleaved["foo"], "the snapshot of foo never overlapped streaming, so nothing was deduplicated")
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
	integration.CheckSkip(t)
	// Starts its own containers on its own network.
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
		watermarkCollection: func() string { return "c" },
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
	want := key(t, 4)
	require.Eventually(t, func() bool {
		maxKey, err := deps.ResolveMaxKey(ctx, table, nil)
		return err == nil && assert.ObjectsAreEqual(want, maxKey)
	}, time.Minute, 250*time.Millisecond)
}

// TestIntegrationMongoCDCIncrementalSnapshotShardedWatermarkPerCollection
// reads a collection living on one shard while the first configured
// collection lives, quiet, on the other, and writes to the busy shard only.
// A watermark read against the quiet collection could trail the writes the
// chunk read saw on the busy shard, releasing the chunk before they stream and
// sending documents backwards. Watermarks address the collection being read;
// on mongo:7 a watermark from the quiet collection passes too, as mongos
// reports the gossiped cluster time, so this guards the behaviour rather than
// reproducing a failure.
func TestIntegrationMongoCDCIncrementalSnapshotShardedWatermarkPerCollection(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()
	uri := startShardedCluster(t)
	client := connectTo(t, uri)

	h := newIncrementalHarnessOn(t, uri, client)
	ctx := t.Context()
	admin := client.Database("admin")
	// The quiet collection is unsharded, so it lives on the primary shard.
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "enableSharding", Value: h.db.Name()}, {Key: "primaryShard", Value: "sh2"}}).Err())
	h.db.CreateCollection(t, "quiet")
	h.db.InsertOne(t, "quiet", bson.M{"_id": 0, "v": 0})

	// The busy collection is range sharded with its only chunk on the other
	// shard.
	ns := h.db.Name() + ".busy"
	h.db.CreateCollection(t, "busy")
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "shardCollection", Value: ns}, {Key: "key", Value: bson.M{"_id": 1}}}).Err())
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "moveChunk", Value: ns}, {Key: "find", Value: bson.M{"_id": 0}}, {Key: "to", Value: "sh1"}}).Err())

	const initial = 1000
	docs := make([]any, 0, initial)
	for i := range initial {
		docs = append(docs, bson.M{"_id": i, "v": 0})
	}
	h.db.InsertMany(t, "busy", docs...)

	input := strings.NewReplacer(
		"  username: mongoadmin\n", "",
		"  password: secret\n", "",
		"chunk_size: 20", "chunk_size: 100",
		"    - 'foo'\n    - 'bar'\n", "    - 'quiet'\n    - 'busy'\n",
		"  signal_collection_name: signals\n", "",
	).Replace(incrementalInput)
	stream := h.stream(input)
	wait := stream.RunAsync(t)

	// Update the busy shard only while the snapshot reads it. A quiet shard
	// releases about one chunk per no-op interval (10 s), so this spans a few
	// chunks. Writes are paced so the stream keeps up.
	waitForLog := func(sub string, within time.Duration) {
		t.Helper()
		require.Eventually(t, func() bool { return len(h.logs.matching(sub)) > 0 }, within, 100*time.Millisecond, "waiting for log %q", sub)
	}
	waitForLog("Incremental snapshot reading collection busy", 2*time.Minute)
	rng := rand.New(rand.NewPCG(5, 6))
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		h.db.UpdateOne(t, "busy", rng.IntN(initial), bson.M{"$inc": bson.M{"v": 1}})
		time.Sleep(20 * time.Millisecond)
	}
	want := dbState(t, h.db, "busy")
	waitForLog("Incremental snapshot complete", 5*time.Minute)
	assert.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(want, replay(h.output).of("busy"))
	}, time.Minute, 500*time.Millisecond)
	stream.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Empty(t, got.regressed, "documents went backwards")
	assert.Equal(t, want, got.of("busy"))
	assert.Positive(t, got.readByColl["busy"])
	t.Logf("snapshot read %d documents, %d updates streamed", got.readByColl["busy"], got.ops["update"])
}
