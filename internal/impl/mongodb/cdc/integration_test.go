// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/asyncroutine"
	"github.com/redpanda-data/connect/v4/internal/license"
)

type streamHelper struct {
	builder *service.StreamBuilder

	mu      sync.Mutex
	current *service.Stream
}

func (s *streamHelper) Run(t *testing.T) {
	stream := s.makeStream(t)
	require.NoError(t, stream.Run(t.Context()))
}

func (s *streamHelper) RunAsync(t *testing.T) func() {
	stream := s.makeStream(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		err := stream.Run(t.Context())
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
	})
	return wg.Wait
}

func (s *streamHelper) Stop(t *testing.T) {
	stream := s.getStream(t)
	require.NoError(t, stream.Stop(t.Context()))
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Same(t, s.current, stream)
	s.current = nil
}

func (s *streamHelper) StopWithin(t *testing.T, d time.Duration) {
	stream := s.getStream(t)
	require.NoError(t, stream.StopWithin(d))
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Same(t, s.current, stream)
	s.current = nil
}

func (s *streamHelper) StopNow(t *testing.T) {
	stream := s.getStream(t)
	require.ErrorIs(t, context.DeadlineExceeded, stream.StopWithin(0))
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Same(t, s.current, stream)
	s.current = nil
}

func (s *streamHelper) getStream(t *testing.T) *service.Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	require.NotNil(t, s.current)
	return s.current
}

func (s *streamHelper) makeStream(t *testing.T) *service.Stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Nil(t, s.current)
	stream, err := s.builder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	s.current = stream
	return stream
}

type databaseHelper struct {
	*mongo.Database
}

func (d *databaseHelper) CreateCollection(t *testing.T, collection string, opts ...options.Lister[options.CreateCollectionOptions]) {
	err := d.Database.CreateCollection(t.Context(), collection, opts...)
	require.NoError(t, err)
}

func (d *databaseHelper) CreateShardedCollection(t *testing.T, collection string, opts ...options.Lister[options.CreateCollectionOptions]) {
	require.NoError(t, d.Client().Database("admin").RunCommand(
		t.Context(),
		bson.D{{Key: "enableSharding", Value: d.Database.Name()}},
	).Err())
	err := d.Database.CreateCollection(t.Context(), collection, opts...)
	require.NoError(t, err)
	require.NoError(t, d.Client().Database("admin").RunCommand(
		t.Context(),
		bson.D{
			{Key: "shardCollection", Value: fmt.Sprintf("%s.%s", d.Database.Name(), collection)},
			{Key: "key", Value: bson.M{"_id": "hashed"}},
		},
	).Err())
}

func (d *databaseHelper) FindOne(t *testing.T, collection string, id any) (doc any) {
	r := d.Collection(collection).FindOne(t.Context(), bson.M{"_id": id})
	require.NoError(t, r.Err())
	require.NoError(t, r.Decode(&doc))
	return
}

func (d *databaseHelper) FindOneJSON(t *testing.T, collection string, id any) string {
	doc := d.FindOne(t, collection, id)
	j, err := bson.MarshalExtJSON(doc, false, true)
	require.NoError(t, err)
	return string(j)
}

func (d *databaseHelper) InsertOne(t *testing.T, collection string, doc any) {
	_, err := d.Collection(collection).InsertOne(t.Context(), doc)
	require.NoError(t, err)
}

func (d *databaseHelper) InsertMany(t *testing.T, collection string, docs ...any) {
	_, err := d.Collection(collection).InsertMany(t.Context(), docs)
	require.NoError(t, err)
}

func (d *databaseHelper) ReplaceOne(t *testing.T, collection string, id, doc any) {
	_, err := d.Collection(collection).ReplaceOne(t.Context(), bson.M{"_id": id}, doc)
	require.NoError(t, err)
}

func (d *databaseHelper) UpdateOne(t *testing.T, collection string, id, doc any) {
	_, err := d.Collection(collection).UpdateOne(t.Context(), bson.M{"_id": id}, doc)
	require.NoError(t, err)
}

func (d *databaseHelper) DeleteByID(t *testing.T, collection string, id any) {
	_, err := d.Collection(collection).DeleteOne(t.Context(), bson.M{"_id": id})
	require.NoError(t, err)
}

type outputHelper struct {
	mu      sync.Mutex
	batches []service.MessageBatch
	nack    bool
}

func (o *outputHelper) NackAll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.nack = true
}

func (o *outputHelper) AckAll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.nack = false
}

func (o *outputHelper) AddBatch(_ context.Context, batch service.MessageBatch) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.nack {
		return errors.New("!!!FORCE INJECTED TEST ERROR !!!")
	}
	o.batches = append(o.batches, batch)
	return nil
}

func (o *outputHelper) Messages(t *testing.T) []any {
	t.Helper()
	msgs, err := o.messages()
	require.NoError(t, err)
	return msgs
}

// messages is the non-failing variant of Messages for use inside Eventually
// conditions: require's FailNow runs on testify's tick goroutine there, which
// kills the tick silently instead of failing the test.
func (o *outputHelper) messages() ([]any, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var msgs []any
	for _, b := range o.batches {
		for _, m := range b {
			msg, err := m.AsStructured()
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
	}
	return msgs, nil
}

func (o *outputHelper) MessagesJSON(t *testing.T) string {
	msgs := o.Messages(t)
	b, err := json.Marshal(msgs)
	require.NoError(t, err)
	return string(b)
}

func (o *outputHelper) Metadata(t *testing.T) []map[string]any {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	var metas []map[string]any
	for _, b := range o.batches {
		for _, m := range b {
			meta := map[string]any{}
			err := m.MetaWalkMut(func(k string, v any) error {
				switch k {
				case "operation_time":
					// Make this deterministic
					meta[k] = "$timestamp"
				case "schema":
					// Schema is complex structured metadata, tested separately
				default:
					meta[k] = v
				}
				return nil
			})
			require.NoError(t, err)
			metas = append(metas, meta)
		}
	}
	return metas
}

func (o *outputHelper) MetadataJSON(t *testing.T) string {
	metas := o.Metadata(t)
	b, err := json.Marshal(metas)
	require.NoError(t, err)
	return string(b)
}

// Schemas returns the parsed schema.Common for each message. Messages without
// schema metadata produce a zero-value entry.
func (o *outputHelper) Schemas(t *testing.T) []schema.Common {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	var schemas []schema.Common
	for _, b := range o.batches {
		for _, m := range b {
			var s schema.Common
			var raw any
			_ = m.MetaWalkMut(func(k string, v any) error {
				if k == "schema" {
					raw = v
				}
				return nil
			})
			if raw != nil {
				parsed, err := schema.ParseFromAny(raw)
				require.NoError(t, err)
				s = parsed
			}
			schemas = append(schemas, s)
		}
	}
	return schemas
}

type setupOption = func(client *mongo.Client) error

func enablePreAndPostDocuments() setupOption {
	return func(client *mongo.Client) error {
		r := client.Database("admin").RunCommand(
			context.Background(),
			bson.M{
				"setClusterParameter": bson.M{
					"changeStreamOptions": bson.M{
						"preAndPostImages": bson.M{"expireAfterSeconds": 120},
					},
				},
			},
		)
		return r.Err()
	}
}

// startMongoContainer boots a single node replica set with root credentials and
// returns a direct-connection URI plus a client that has already been pinged
// successfully. Tests that need control over the pieces setup hides - the
// checkpoint cache directory, the logger - build their own stream on top of this.
//
// Callers are responsible for integration.CheckSkip.
func startMongoContainer(t *testing.T, opts ...setupOption) (string, *mongo.Client) {
	t.Helper()
	return runMongoContainer(t, []testcontainers.ContainerCustomizer{
		mongocontainer.WithUsername("mongoadmin"),
		mongocontainer.WithPassword("secret"),
		mongocontainer.WithReplicaSet("rs0"),
	}, opts...)
}

// startMongoContainerWithoutAuth boots the same single node replica set with
// authentication disabled, so its URI carries no userinfo. That is what the
// credential-refresh tests need: they stub the credential builder to return no
// credential at all, and a MONGODB-AWS credential could not authenticate against
// a test container anyway.
func startMongoContainerWithoutAuth(t *testing.T, opts ...setupOption) (string, *mongo.Client) {
	t.Helper()
	return runMongoContainer(t, []testcontainers.ContainerCustomizer{
		mongocontainer.WithReplicaSet("rs0"),
	}, opts...)
}

func runMongoContainer(t *testing.T, customizers []testcontainers.ContainerCustomizer, opts ...setupOption) (string, *mongo.Client) {
	t.Helper()
	container, err := mongocontainer.Run(t.Context(), "mongo:7", customizers...)
	t.Cleanup(func() {
		// t.Context() is already cancelled when cleanup runs
		if err := container.Terminate(context.Background()); err != nil {
			t.Fatal("unable to shutdown container", err)
		}
	})
	require.NoError(t, err)
	connStr, err := container.ConnectionString(t.Context())
	require.NoError(t, err)
	url, err := url.Parse(connStr)
	require.NoError(t, err)
	// Force a directConnection because we don't have the proper networking setup for a
	// proper replica set cluster.
	query := url.Query()
	query.Add("directConnection", "true")
	url.RawQuery = query.Encode()
	uri := url.String()
	t.Log(uri)
	mongoClient, err := mongo.Connect(options.Client().
		SetConnectTimeout(5 * time.Second).
		SetTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		ApplyURI(uri).
		SetDirect(true))
	require.NoError(t, err)
	// The replica set can take a moment after container readiness before it
	// accepts client connections through the mapped port, so retry the ping. A
	// ping succeeds as soon as the server answers, which is before the single
	// node has elected itself primary, so callers would race the election and
	// get `(NotWritablePrimary) not primary` from their first write. Ask the
	// server directly whether it is writable before declaring readiness.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		if !assert.NoError(c, mongoClient.Ping(t.Context(), nil)) {
			return
		}
		hello, err := mongoClient.Database("admin").RunCommand(t.Context(), bson.M{"hello": 1}).Raw()
		if !assert.NoError(c, err) {
			return
		}
		writable, err := hello.LookupErr("isWritablePrimary")
		if !assert.NoError(c, err, "hello reply carried no isWritablePrimary field: %v", hello) {
			return
		}
		isPrimary, ok := writable.BooleanOK()
		assert.True(c, ok, "isWritablePrimary was not a boolean: %v", writable)
		assert.True(c, isPrimary, "the replica set has not elected a primary yet")
	}, 60*time.Second, time.Second)
	for _, opt := range opts {
		require.NoError(t, opt(mongoClient))
	}
	return uri, mongoClient
}

func setup(t *testing.T, template string, opts ...setupOption) (*streamHelper, *databaseHelper, *outputHelper) {
	integration.CheckSkip(t)
	t.Helper()
	uri, mongoClient := startMongoContainer(t, opts...)
	d := &databaseHelper{mongoClient.Database("test")}
	template = strings.NewReplacer(
		"$USERNAME", "mongoadmin",
		"$PASSWORD", "secret",
		"$DATABASE", "test",
		"$CACHE", "filecache",
		"$URI", uri,
	).Replace(template)
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(template))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+t.TempDir()+`'`))
	o := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(o.AddBatch))
	return &streamHelper{builder: builder}, d, o
}

func TestIntegrationMongoCDC(t *testing.T) {
	runTest := func(t *testing.T, mode string) {
		r := strings.NewReplacer("$MODE", mode)
		stream, db, output := setup(t, r.Replace(`
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  document_mode: $MODE
  collections:
    - 'foo'
`), enablePreAndPostDocuments())
		db.CreateCollection(
			t,
			"foo",
			options.CreateCollection().SetChangeStreamPreAndPostImages(bson.M{"enabled": mode == "pre_and_post_images"}),
		)
		wait := stream.RunAsync(t)
		time.Sleep(2 * time.Second) // Wait for stream to start
		db.InsertOne(t, "foo", bson.M{
			"_id":  "1",
			"data": "hello cdc",
		})
		db.ReplaceOne(t, "foo", "1", bson.M{
			"data": "hello cdc!",
		})
		db.UpdateOne(t, "foo", "1", bson.M{
			"$set": bson.M{"foo": "hello!"},
		})
		// Sleep so the update_lookup post-image fetch completes before the
		// delete removes the document, avoiding a null fullDocument race.
		time.Sleep(500 * time.Millisecond)
		db.DeleteByID(t, "foo", "1")
		time.Sleep(3 * time.Second)
		stream.StopWithin(t, 10*time.Second)
		wait()
		switch mode {
		case "pre_and_post_images":
			require.JSONEq(t, `[
          {"_id": "1", "data": "hello cdc"},
          {"_id": "1", "data": "hello cdc!"},
          {"_id": "1", "data": "hello cdc!", "foo": "hello!"},
          {"_id": "1", "data": "hello cdc!", "foo": "hello!"}
      ]`, output.MessagesJSON(t))
		case "update_lookup":
			require.JSONEq(t, `[
          {"_id": "1", "data": "hello cdc"},
          {"_id": "1", "data": "hello cdc!"},
          {"_id": "1", "data": "hello cdc!", "foo": "hello!"},
          {"_id": "1"}
      ]`, output.MessagesJSON(t))
		}
		require.JSONEq(t, `[
      {"operation": "insert", "collection": "foo", "operation_time": "$timestamp"},
    {"operation": "replace", "collection": "foo", "operation_time": "$timestamp"},
    {"operation": "update", "collection": "foo", "operation_time": "$timestamp"},
    {"operation": "delete", "collection": "foo", "operation_time": "$timestamp"}
]`, output.MetadataJSON(t))
	}
	t.Run("Normal", func(t *testing.T) { runTest(t, "update_lookup") })
	t.Run("PreAndPostImages", func(t *testing.T) { runTest(t, "pre_and_post_images") })
}

func TestIntegrationMongoCDCWithSnapshot(t *testing.T) {
	stream, db, output := setup(t, `
read_until:
  idle_timeout: 1s
  input:
    mongodb_cdc:
      url: '$URI'
      database: '$DATABASE'
      checkpoint_cache: '$CACHE'
      stream_snapshot: true
      collections:
        - 'foo'
`)
	db.CreateCollection(t, "foo")
	var id atomic.Int64
	writer := asyncroutine.NewPeriodic(time.Microsecond, func() {
		db.InsertOne(t, "foo", bson.M{"_id": int(id.Add(1)), "data": "hello"})
	})
	writer.Start()
	time.Sleep(time.Second)
	wait := stream.RunAsync(t)
	time.Sleep(time.Second) // pump some data to the stream
	writer.Stop()
	wait()
	stream.Stop(t)
	// Require that we saw all messages at least once, it's possible we get duplicates
	// when replaying the cdc stream after the snapshot completes, but everything should
	// be there. We assert the change stream is ordered in other places, this real goal
	// here is to make sure we're not missing anything.
	actual := output.Messages(t)
	for i := range int(id.Load()) {
		expected := map[string]any{
			"_id":  map[string]any{"$numberInt": strconv.Itoa(i + 1)},
			"data": "hello",
		}
		if !assert.Containsf(t, actual, expected, "actual: %v missing: %v", actual, i+1) {
			return
		}
	}
	// Sanity check to make sure we got past the snapshot phase
	require.Contains(t, output.Metadata(t), map[string]any{
		"operation":      "insert",
		"collection":     "foo",
		"operation_time": "$timestamp",
	})
}

func TestIntegrationMongoCDCWithParallelSnapshot(t *testing.T) {
	runTest := func(t *testing.T, autoBuckets bool) {
		stream, db, output := setup(t, `
read_until:
  # Wait then auto stop, we're just testing the snapshot phase here
  idle_timeout: 3s
  input:
    mongodb_cdc:
      url: '$URI'
      database: '$DATABASE'
      stream_snapshot: true
      checkpoint_cache: '$CACHE'
      snapshot_parallelism: 8
      collections:
        - 'foo'
      snapshot_auto_bucket_sharding: `+strconv.FormatBool(autoBuckets))

		db.CreateCollection(t, "foo")
		// Write 100k messages — enough to exercise parallel snapshot with 8 workers
		// while keeping the total package runtime under the 5-minute go test timeout.
		for batch := range 100 {
			idRangeStart := batch * 1_000
			batch := []any{}
			for id := range 1_000 {
				batch = append(batch, bson.M{"_id": idRangeStart + id + 1, "data": "hello"})
			}
			db.InsertMany(t, "foo", batch...)
		}
		stream.Run(t)
		expected := map[any]bool{}
		for i := range 100_000 {
			expected[strconv.Itoa(i+1)] = true
		}
		seen := map[any]bool{}
		for _, msg := range output.Messages(t) {
			require.IsType(t, map[string]any{}, msg)
			require.Len(t, msg, 2)
			bsonID := msg.(map[string]any)["_id"]
			require.IsType(t, map[string]any{}, bsonID)
			require.Len(t, bsonID, 1)
			id := bsonID.(map[string]any)["$numberInt"]
			require.IsType(t, "", id)
			require.True(t, expected[id], "missing ID %v, seen: %v", id, seen[id])
			seen[id] = true
			delete(expected, id)
		}
		require.Empty(t, expected)
		for _, meta := range output.Metadata(t) {
			require.Equal(t, map[string]any{"operation": "read", "collection": "foo", "operation_time": "$timestamp"}, meta)
		}
	}
	t.Run("AutoBuckets", func(t *testing.T) { runTest(t, true) })
	t.Run("SplitVector", func(t *testing.T) { runTest(t, false) })
}

func TestIntegrationMongoCDCResumeStream(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  snapshot_parallelism: 4
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")

	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	require.Eventually(t, func() bool { return len(output.Messages(t)) > 0 }, time.Second, time.Millisecond)
	stream.StopWithin(t, time.Second)
	wait()
	require.JSONEq(t, `[{"_id":{"$numberInt":"1"}, "data":"hello"}]`, output.MessagesJSON(t))

	wait = stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "world"})
	require.Eventually(t, func() bool { return len(output.Messages(t)) > 1 }, time.Second, time.Millisecond)
	stream.StopWithin(t, time.Second)
	wait()
	require.JSONEq(t, `[{"_id":{"$numberInt":"1"},"data":"hello"},{"_id":{"$numberInt":"2"},"data":"world"}]`, output.MessagesJSON(t))
}

// TestIntegrationMongoCDCResumeStreamWithoutFlusher is TestIntegrationMongoCDCResumeStream
// with checkpoint_interval: 0, which disables the periodic flusher and makes acks
// write to the cache directly. That write-through path shares the epoch guard with
// every other checkpoint write, so it needs its own coverage: a guard that
// dropped these writes would leave nothing to resume from, and the resume would
// silently become a re-read.
func TestIntegrationMongoCDCResumeStreamWithoutFlusher(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  checkpoint_interval: 0s
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")

	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	// output.messages() rather than Messages(t): require inside an Eventually
	// condition FailNows on testify's tick goroutine and silently kills it.
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) > 0
	}, 30*time.Second, 10*time.Millisecond)
	stream.StopWithin(t, 30*time.Second)
	wait()
	require.JSONEq(t, `[{"_id":{"$numberInt":"1"}, "data":"hello"}]`, output.MessagesJSON(t))

	// The ack of the first event wrote its position through to the cache, so this
	// run resumes after it rather than replaying it.
	wait = stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "world"})
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) > 1
	}, 30*time.Second, 10*time.Millisecond)
	stream.StopWithin(t, 30*time.Second)
	wait()
	require.JSONEq(t, `[{"_id":{"$numberInt":"1"},"data":"hello"},{"_id":{"$numberInt":"2"},"data":"world"}]`, output.MessagesJSON(t))
}

func TestIntegrationMongoCDCResumeWithSnapshot(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  snapshot_parallelism: 4
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	output.NackAll()
	wait := stream.RunAsync(t)
	t.Cleanup(wait)
	time.Sleep(time.Second)
	stream.StopNow(t)
	require.Empty(t, output.Messages(t))

	output.AckAll()
	wait = stream.RunAsync(t)
	require.Eventually(t, func() bool { return len(output.Messages(t)) == 1 }, time.Second, time.Millisecond)
	stream.StopWithin(t, time.Second)
	wait()
	require.JSONEq(t, `[{"_id":{"$numberInt":"1"},"data":"hello"}]`, output.MessagesJSON(t))
}

func TestIntegrationMongoCDCRelaxedMarshalling(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "hello"})
	time.Sleep(time.Second)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}]`, output.MessagesJSON(t))
}

func TestIntegrationMongoCDCFilteredStream(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.CreateCollection(t, "bar")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "bar", bson.M{"_id": 2, "data": "world"})
	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 3, "data": "hello"})
	db.InsertOne(t, "bar", bson.M{"_id": 4, "data": "world"})
	time.Sleep(time.Second)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":3,"data":"hello"}]`, output.MessagesJSON(t))
	require.JSONEq(t, `[{"operation":"read","collection":"foo", "operation_time":"$timestamp"}, {"operation":"insert","collection":"foo", "operation_time":"$timestamp"}]`, output.MetadataJSON(t))
}

func TestIntegrationMongoCDCMultipleCollections(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  collections:
    - 'foo'
    - 'bar'
    - 'qux'
`)
	db.CreateCollection(t, "foo")
	db.CreateCollection(t, "bar")
	db.CreateCollection(t, "qux")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "bar", bson.M{"_id": 2, "data": "world"})
	db.InsertOne(t, "qux", bson.M{"_id": 3, "data": "!"})
	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": 4, "data": "hello"})
	db.InsertOne(t, "bar", bson.M{"_id": 5, "data": "world"})
	db.InsertOne(t, "qux", bson.M{"_id": 6, "data": "!"})
	time.Sleep(time.Second)
	stream.Stop(t)
	wait()
	msgs := output.Messages(t)
	metas := output.Metadata(t)
	require.Len(t, msgs, 6)
	require.Len(t, metas, 6)
	// Snapshots can be processed in any order
	require.ElementsMatch(t, []any{
		map[string]any{"_id": json.Number("1"), "data": "hello"},
		map[string]any{"_id": json.Number("2"), "data": "world"},
		map[string]any{"_id": json.Number("3"), "data": "!"},
	}, msgs[0:3])
	require.ElementsMatch(t, []map[string]any{
		{"operation": "read", "collection": "foo", "operation_time": "$timestamp"},
		{"operation": "read", "collection": "bar", "operation_time": "$timestamp"},
		{"operation": "read", "collection": "qux", "operation_time": "$timestamp"},
	}, metas[0:3])
	// Changes must be in order
	require.Equal(t, []any{
		map[string]any{"_id": json.Number("4"), "data": "hello"},
		map[string]any{"_id": json.Number("5"), "data": "world"},
		map[string]any{"_id": json.Number("6"), "data": "!"},
	}, msgs[3:6])
	require.Equal(t, []map[string]any{
		{"operation": "insert", "collection": "foo", "operation_time": "$timestamp"},
		{"operation": "insert", "collection": "bar", "operation_time": "$timestamp"},
		{"operation": "insert", "collection": "qux", "operation_time": "$timestamp"},
	}, metas[3:6])
}

func TestIntegrationMongoPartialUpdates(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  document_mode: partial_update
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{
		"_id":         1,
		"nested.data": "hello",
		"remove_me":   true,
		"arraything": bson.M{
			"here it is": bson.A{1, 2, 3},
			"a.nother":   bson.A{"a", "b", "c"},
		},
		"nested": bson.M{
			"bar": bson.A{bson.M{"a": "a"}},
		},
	})
	wait := stream.RunAsync(t)
	time.Sleep(time.Second)
	db.UpdateOne(t, "foo", 1, bson.A{
		bson.M{
			"$set": bson.M{
				"arraything": bson.M{
					"$setField": bson.M{
						"field": "a.nother",
						"input": "$arraything",
						"value": "world",
					},
				},
			},
		},
		bson.M{
			"$unset": "remove_me",
		},
	})
	db.UpdateOne(t, "foo", 1, bson.A{
		bson.M{
			"$set": bson.M{
				"arraything.here it is": bson.M{
					"$slice": bson.A{"$arraything.here it is", 2},
				},
			},
		},
	})
	db.UpdateOne(t, "foo", 1, bson.M{"$set": bson.M{"nested.bar.0.a": "b"}})
	time.Sleep(time.Second)
	stream.Stop(t)
	wait()
	actual := output.MessagesJSON(t)
	require.JSONEq(t, `[
    {
      "_id": 1,
      "arraything": {"a.nother":["a","b","c"],"here it is":[1,2,3]},
      "nested": {"bar":[{"a":"a"}]},
      "nested.data": "hello",
      "remove_me": true
    },
    {
      "_id":1,
      "operations": [
        {"path": ["arraything", "a.nother"], "type": "set", "value":"world"},
        {"path": ["remove_me"], "type": "unset", "value": null}
      ]
    },
    {
      "_id":1,
      "operations": [
        {"path": ["arraything", "here it is"], "type": "truncatedArray", "value": 2}
      ]
    },
    {
      "_id":1,
      "operations": [
        {"path": ["nested", "bar", "0", "a"], "type": "set", "value":"b"}
      ]
    }
  ]`, actual, "got: %s", actual)
	require.JSONEq(t, `
    {
      "_id": 1,
      "arraything": {"a.nother":"world","here it is":[1,2]},
      "nested": {"bar":[{"a":"b"}]},
      "nested.data": "hello"
    }
  `, db.FindOneJSON(t, "foo", 1))
}

func TestIntegrationMongoResumeAfterSnapshotWithoutChanges(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "hello"})
	wait := stream.RunAsync(t)
	require.Eventually(t, func() bool { return len(output.Messages(t)) == 2 }, 10*time.Second, 100*time.Millisecond)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}]`, output.MessagesJSON(t))
	wait = stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}]`, output.MessagesJSON(t))
}

func TestIntegrationMongoIssue3425(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "hello"})
	wait := stream.RunAsync(t)
	time.Sleep(35 * time.Second) // there is a default connection timeout of 30 seconds in the driver
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}]`, output.MessagesJSON(t))
	db.InsertOne(t, "foo", bson.M{"_id": 3, "data": "hello"})
	time.Sleep(5 * time.Second)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}, {"_id":3,"data":"hello"}]`, output.MessagesJSON(t))
}

func TestIntegrationMongoCDCWithSnapshotShardedCluster(t *testing.T) {
	integration.CheckSkipExact(t)
	// You can setup a sharded cluster with https://github.com/pkdone/sharded-mongodb-docker
	builder := service.NewStreamBuilder()
	require.NoError(t,
		builder.AddInputYAML(`
read_until:
  idle_timeout: 60s # Sharded DBs are *super* slow for some reason to emit changes
  input:
    mongodb_cdc:
      url: 'mongodb://localhost:27017'
      database: 'test'
      checkpoint_cache: 'filecache'
      stream_snapshot: true
      collections:
        - 'foo'
`))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+t.TempDir()+`'`))
	output := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
	stream := &streamHelper{builder: builder}
	mongoClient, err := mongo.Connect(options.Client().
		SetConnectTimeout(5 * time.Second).
		SetTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)
	db := &databaseHelper{mongoClient.Database("test")}
	// Since this is an external database, let's ensure we have a clean slate
	_ = db.Collection("foo").Drop(t.Context())
	db.CreateCollection(t, "foo")
	var id atomic.Int64
	writer := asyncroutine.NewPeriodic(time.Microsecond, func() {
		db.InsertOne(t, "foo", bson.M{"_id": int(id.Add(1)), "data": "hello"})
	})
	writer.Start()
	time.Sleep(time.Second)
	wait := stream.RunAsync(t)
	time.Sleep(time.Second) // pump some data to the stream
	writer.Stop()
	wait()
	stream.Stop(t)
	// Ensure that we got some data via reads and we got some data via change stream
	require.Contains(t, output.Metadata(t), map[string]any{
		"operation":      "insert",
		"collection":     "foo",
		"operation_time": "$timestamp",
	})
	require.Contains(t, output.Metadata(t), map[string]any{
		"operation":      "read",
		"collection":     "foo",
		"operation_time": "$timestamp",
	})
	// Require that we saw all messages at least once, it's possible we get duplicates
	// when replaying the cdc stream after the snapshot completes, but everything should
	// be there. We assert the change stream is ordered in other places, this real goal
	// here is to make sure we're not missing anything.
	actual := output.Messages(t)
	c, err := db.Collection("foo").CountDocuments(t.Context(), bson.D{})
	require.NoError(t, err)
	t.Log("wrote", id.Load(), "documents, read", len(actual), "documents, counting found:", c)
	require.GreaterOrEqual(t, len(actual), int(id.Load()))
	for i := range int(id.Load()) {
		expected := map[string]any{
			"_id":  map[string]any{"$numberInt": strconv.Itoa(i + 1)},
			"data": "hello",
		}
		if !assert.Containsf(t, actual, expected, "actual: %v missing: %v", actual, i+1) {
			return
		}
	}
}

// logCapture is a slog.Handler that keeps every record it is given, so a test
// can assert on what the pipeline logged. The cdc input reports change stream
// failures through ReadBatch, where the framework logs them and reconnects;
// there is no other hook a test can observe them through.
type logCapture struct {
	mu      sync.Mutex
	records []string
}

func (*logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (l *logCapture) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Level.String()+" "+r.Message)
	return nil
}

func (l *logCapture) WithAttrs([]slog.Attr) slog.Handler { return l }

func (l *logCapture) WithGroup(string) slog.Handler { return l }

// matching returns the captured messages containing sub.
func (l *logCapture) matching(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.records {
		if strings.Contains(r, sub) {
			out = append(out, r)
		}
	}
	return out
}

// TestIntegrationMongoCDCUnresumableCheckpointToken is the regression test for
// unresumable-checkpoint recovery. When the cache holds a resume token the server
// cannot resume from, the input has no usable position: the checkpoint's presence
// would skip the snapshot, and the dead token stops the change stream from
// opening at all. Retrying that position can never succeed, so the input must
// recognise the failure, clear the checkpoint and re-run the snapshot - the
// at-least-once recovery (duplicates, never loss) - rather than loop forever
// delivering nothing.
//
// Asserted here: the recovery is announced in the log, both seeded documents
// arrive from the re-run snapshot, and a later restart reads nothing more,
// proving the replacement checkpoint is one the server accepts. Without the
// recovery this test fails on all three: no warning, no documents, and the
// unchanged garbage token still in the cache file.
//
// The stream is built explicitly rather than via setup because the checkpoint
// has to be seeded into the cache directory before the first run, and because
// the recovery is only observable through the logger.
//
// NOTE: before the recovery existed this test produced a fast reconnect loop,
// which is what exposed the checkpointFlusher Start/Stop race across reconnects,
// since fixed by registering TriggerHasStopped first in input.go's stream
// goroutine so the flusher is fully stopped before a waiting Connect resumes.
// The recovery path still crosses one reconnect, so the ordering stays exercised.
func TestIntegrationMongoCDCUnresumableCheckpointToken(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainer(t)
	db := &databaseHelper{mongoClient.Database("test")}
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "world"})

	// Seed the checkpoint before the first run. The file cache stores one file
	// per key whose contents are the raw value bytes, and checkpointCache
	// encodes the token as canonical extended JSON, so writing the exact bytes
	// Store would have written makes Load round-trip it.
	//
	// The _data payload has to be a keystring the server actually rejects. A
	// well-formed keystring that merely points at an ancient position is not
	// enough: a token for timestamp 0 is happily accepted against a replica set
	// whose oplog has never rolled over, and the stream then replays the oplog
	// from its start (verified - it delivered both documents below as `insert`
	// events). "DEADBEEF" is not decodable as a keystring at all, which is what
	// makes the resume fail rather than succeed from an early position.
	cacheDir := t.TempDir()
	rawToken, err := bson.Marshal(bson.M{"_data": "DEADBEEF"})
	require.NoError(t, err)
	encoded, err := bson.MarshalExtJSON(bson.Raw(rawToken), true, false)
	require.NoError(t, err)
	t.Logf("seeded checkpoint: %s", encoded)
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "mongodb_cdc_checkpoint"), encoded, 0o644))

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
`))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+cacheDir+`'`))
	output := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
	logs := &logCapture{}
	builder.SetLogger(slog.New(logs))
	stream := &streamHelper{builder: builder}

	wait := stream.RunAsync(t)

	// The dead token is detected on the first change stream open, its checkpoint
	// cleared, and the reconnect then finds no checkpoint and re-runs the
	// snapshot - which is how the two documents already in the collection get
	// delivered despite the input having started with a checkpoint.
	//
	// output.messages() is used rather than Messages(t) inside the condition:
	// testify runs it on its own goroutine where a require failure would
	// silently kill the tick instead of failing the test.
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) >= 2
	}, 60*time.Second, 250*time.Millisecond, "the re-run snapshot never delivered the seeded documents")

	// logs.matching snapshots under the capture's mutex: the stream is still
	// running here, so reading logs.records directly would race Handle, and
	// assertion message args are evaluated eagerly even on success.
	cleared := logs.matching("no longer resumable, clearing the checkpoint")
	require.NotEmpty(t, cleared, "expected the unresumable position to be reported and cleared, captured logs: %v", logs.matching(""))
	t.Logf("recovery (x%d): %s", len(cleared), cleared[0])
	// The clear is triggered by the failure to open the stream on the dead token,
	// so that failure is still surfaced - it is what makes the framework
	// reconnect into the re-snapshot.
	failures := logs.matching("error watching MongoDB change stream")
	require.NotEmpty(t, failures, "expected the change stream open to fail before the recovery")
	require.Contains(t, failures[0], "error opening change stream")

	// Wait for the replacement checkpoint to actually reach the cache before
	// stopping. Delivery of the documents only proves the snapshot ran; the
	// post-snapshot store happens after the last ack, so stopping on the message
	// count alone would race it and make the restart phase below assert against
	// whichever side won. The file is briefly absent between the clear and the
	// store, which counts as "not yet".
	checkpointFile := filepath.Join(cacheDir, "mongodb_cdc_checkpoint")
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(checkpointFile)
		return err == nil && string(b) != string(encoded)
	}, 60*time.Second, 250*time.Millisecond, "the recovery never stored a replacement checkpoint")

	stream.StopWithin(t, 30*time.Second)
	wait()
	require.Len(t, output.Messages(t), 2, "the re-run snapshot must deliver each document once")

	// The garbage token is gone, replaced by a position the server accepted.
	after, err := os.ReadFile(checkpointFile)
	require.NoError(t, err)
	require.NotEqual(t, string(encoded), string(after), "the unresumable checkpoint must not survive the recovery")
	t.Logf("checkpoint after recovery: %s", after)

	// Count the recoveries only once the first run has fully stopped, so the
	// comparison after the restart cannot be thrown off by a clear that landed
	// between the snapshot above and the shutdown.
	clearedBeforeRestart := len(logs.matching("no longer resumable, clearing the checkpoint"))

	// And that replacement really is resumable: a restart with no new writes
	// resumes the stream instead of re-running the snapshot or recovering again.
	wait = stream.RunAsync(t)
	time.Sleep(5 * time.Second)
	stream.StopWithin(t, 30*time.Second)
	wait()
	require.Len(t, output.Messages(t), 2, "the recovered checkpoint must resume rather than re-snapshot")
	require.Len(t, logs.matching("no longer resumable, clearing the checkpoint"), clearedBeforeRestart,
		"the recovery must not repeat once a valid checkpoint is stored")
}

// TestIntegrationMongoCDCUnresumablePositionWithoutSnapshot covers the recovery
// path that cannot be lossless. With stream_snapshot disabled there is no
// snapshot to re-run, so clearing a dead position restarts streaming from the
// current oplog position and every change since that position is skipped.
// on_unresumable_position decides whether that is acceptable, and it defaults to
// refusing.
//
// The garbage token is seeded the same way as
// TestIntegrationMongoCDCUnresumableCheckpointToken, and for the same reason:
// "DEADBEEF" is not decodable as a keystring, so the server rejects the resume
// rather than accepting an early position and replaying the oplog.
func TestIntegrationMongoCDCUnresumablePositionWithoutSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainer(t)

	rawToken, err := bson.Marshal(bson.M{"_data": "DEADBEEF"})
	require.NoError(t, err)
	encoded, err := bson.MarshalExtJSON(bson.Raw(rawToken), true, false)
	require.NoError(t, err)

	// run boots an input against a fresh database and checkpoint file seeded with
	// the dead token, and returns the pieces the assertions need.
	run := func(t *testing.T, database, mode string) (*databaseHelper, *outputHelper, *logCapture, *streamHelper, string) {
		t.Helper()
		db := &databaseHelper{mongoClient.Database(database)}
		db.CreateCollection(t, "foo")

		cacheDir := t.TempDir()
		checkpointFile := filepath.Join(cacheDir, "mongodb_cdc_checkpoint")
		require.NoError(t, os.WriteFile(checkpointFile, encoded, 0o644))

		conf := `
mongodb_cdc:
  url: '` + uri + `'
  database: '` + database + `'
  checkpoint_cache: 'filecache'
  stream_snapshot: false
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`
		if mode != "" {
			conf += "  on_unresumable_position: " + mode + "\n"
		}
		builder := service.NewStreamBuilder()
		require.NoError(t, builder.AddInputYAML(conf))
		require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+cacheDir+`'`))
		output := &outputHelper{}
		require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
		logs := &logCapture{}
		builder.SetLogger(slog.New(logs))
		return db, output, logs, &streamHelper{builder: builder}, checkpointFile
	}

	t.Run("the default refuses to skip the gap", func(t *testing.T) {
		// No on_unresumable_position at all, so the default is what is under test.
		db, output, logs, stream, checkpointFile := run(t, "faildb", "")
		wait := stream.RunAsync(t)
		t.Cleanup(wait)

		// A write that a `reset` would have streamed, so "nothing was delivered"
		// below means the input really did stop rather than simply having had
		// nothing to read.
		db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})

		require.Eventually(t, func() bool {
			return len(logs.matching("no snapshot to recover with")) > 0
		}, 60*time.Second, 250*time.Millisecond,
			"expected the refusal to be reported, captured logs: %v", logs.matching(""))
		refusal := logs.matching("no snapshot to recover with")
		t.Logf("refusal: %s", refusal[0])
		require.Contains(t, refusal[0], "ERROR", "the refusal must be logged at error level, it needs an operator")
		require.Contains(t, refusal[0], "on_unresumable_position: reset", "the log must name the opt-in")

		// The checkpoint is preserved exactly as it was, for inspection.
		after, err := os.ReadFile(checkpointFile)
		require.NoError(t, err)
		require.Equal(t, string(encoded), string(after), "the checkpoint must be preserved untouched")

		// And nothing was delivered: the input fails rather than skipping ahead.
		require.Empty(t, output.Messages(t), "refusing must not deliver anything")

		stream.StopWithin(t, 30*time.Second)
		after, err = os.ReadFile(checkpointFile)
		require.NoError(t, err)
		require.Equal(t, string(encoded), string(after), "shutdown must not rewrite the preserved checkpoint")
	})

	t.Run("reset opts into skipping the gap", func(t *testing.T) {
		_, output, logs, stream, checkpointFile := run(t, "resetdb", "reset")
		wait := stream.RunAsync(t)
		t.Cleanup(wait)

		require.Eventually(t, func() bool {
			return len(logs.matching("changes since the lost position will be skipped")) > 0
		}, 60*time.Second, 250*time.Millisecond,
			"expected the honest skip warning, captured logs: %v", logs.matching(""))

		// Streaming resumes from the current oplog position, so only writes made
		// after the restart are seen. Which write that is cannot be pinned from
		// outside - the reconnect happens asynchronously - so keep writing until one
		// lands rather than racing a single insert against the reconnect.
		id := 0
		require.Eventually(t, func() bool {
			id++
			if _, err := mongoClient.Database("resetdb").Collection("foo").
				InsertOne(t.Context(), bson.M{"_id": id, "data": "hello"}); err != nil {
				return false
			}
			msgs, err := output.messages()
			return err == nil && len(msgs) > 0
		}, 60*time.Second, 500*time.Millisecond, "streaming never resumed after the reset")

		// The dead token is gone, replaced by a position the server accepted.
		require.Eventually(t, func() bool {
			b, err := os.ReadFile(checkpointFile)
			return err == nil && string(b) != string(encoded)
		}, 60*time.Second, 250*time.Millisecond, "the cleared checkpoint was never replaced")

		stream.StopWithin(t, 30*time.Second)
	})
}

// TestIntegrationMongoCDCCollectionDropAndRename settles what a collection drop
// or rename actually does to this input, which a long-standing TODO on the
// SetResumeAfter call assumed would invalidate the stored resume token.
//
// Measured here, against mongo:7: it does not. The input opens a
// *database*-level change stream (m.db.Watch with an ns.coll $match), and a
// database-level stream is not invalidated by a collection being dropped or
// renamed - the server emits an ordinary `drop`/`rename` event, which the event
// switch skips as an uninteresting operation type, and the stream continues.
// Neither operation costs the input anything: no error, no recovery, no
// re-snapshot, and writes to a recreated collection keep streaming through the
// same cursor.
//
// (For completeness, the two adjacent behaviours were measured the same way and
// are not asserted here because they need a second container each: dropping the
// whole database does end the stream, but its `invalidate` event is filtered out
// by the ns.coll $match, so the cursor simply closes with a nil error and the
// framework's reconnect resumes from the last position successfully. And
// resuming with a token captured before a drop or rename is accepted by the
// server, which replays the drop/rename event. So `ChangeStreamFatalError` (280)
// is not reachable through any of these paths for this input - it is classified
// as position-fatal on the strength of the server's own
// NonResumableChangeStreamError taxonomy, not on a reproduction.)
//
// The assertion that carries the weight is the negative one: no
// unresumable-position recovery is triggered. If a drop did invalidate the
// position, the checkpoint would be cleared and the snapshot re-run, and doc 1
// would be delivered a second time.
func TestIntegrationMongoCDCCollectionDropAndRename(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainer(t)
	db := &databaseHelper{mongoClient.Database("test")}
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "one"})

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

	// awaitCount waits for the delivered message count to reach n. Counting is
	// enough to sequence the phases because every phase adds exactly one document,
	// and the ids are checked at the end.
	awaitCount := func(n int, why string) {
		t.Helper()
		require.Eventually(t, func() bool {
			msgs, err := output.messages()
			return err == nil && len(msgs) >= n
		}, 60*time.Second, 250*time.Millisecond, why)
	}

	// Phase 1: the snapshot delivers doc 1, so the stream phase has begun.
	awaitCount(1, "the snapshot never delivered the seeded document")

	// Phase 2: an ordinary streamed insert, which pins that streaming is live
	// before the collection is disturbed.
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "two"})
	awaitCount(2, "the streamed insert never arrived")

	// Phase 3: drop the watched collection mid-stream, recreate it, and write
	// again. The write must arrive on the same stream. (A drop emits one `drop`
	// event, not a delete per document, so it adds nothing to the delivered
	// count - the skipped event types never reach the output.)
	require.NoError(t, mongoClient.Database("test").Collection("foo").Drop(t.Context()))
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 3, "data": "three"})
	awaitCount(3, "the insert after the collection was dropped and recreated never arrived")

	// Phase 4: rename the watched collection away, recreate it, and write again.
	res := mongoClient.Database("admin").RunCommand(t.Context(), bson.D{
		{Key: "renameCollection", Value: "test.foo"},
		{Key: "to", Value: "test.renamed"},
	})
	require.NoError(t, res.Err())
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 4, "data": "four"})
	awaitCount(4, "the insert after the collection was renamed away never arrived")

	// No recovery was needed for any of it: the position stayed resumable
	// throughout. This is the assertion the stale TODO was really about.
	require.Empty(t, logs.matching("no longer resumable"),
		"a drop or rename must not invalidate the stored position, captured logs: %v", logs.matching(""))
	require.Empty(t, logs.matching("error watching MongoDB change stream"),
		"the change stream must survive a drop and a rename")

	stream.StopWithin(t, 30*time.Second)

	// Every document written was delivered exactly once, in order, through one
	// uninterrupted stream: the snapshot's doc 1, the streamed doc 2, then the
	// writes that followed the drop and the rename. A re-snapshot triggered by
	// either operation would show up here as a repeated id.
	var ids []string
	for _, m := range output.Messages(t) {
		doc, ok := m.(map[string]any)
		require.True(t, ok, "unexpected message shape: %T", m)
		ids = append(ids, fmt.Sprintf("%v", doc["_id"]))
	}
	require.Equal(t, []string{"1", "2", "3", "4"}, ids)
}

// TestIntegrationMongoCDCCorruptCheckpoint covers the sibling of an unresumable
// token: a checkpoint whose bytes are not decodable as a resume token at all.
// That is equally permanent - no retry makes malformed bytes parse - so Connect
// would otherwise fail on every attempt and never start. The input clears it and
// starts over, which for a snapshot-enabled config means the snapshot runs and
// both seeded documents arrive.
func TestIntegrationMongoCDCCorruptCheckpoint(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainer(t)
	db := &databaseHelper{mongoClient.Database("test")}
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})
	db.InsertOne(t, "foo", bson.M{"_id": 2, "data": "world"})

	// Not extended JSON, so Load's unmarshal fails rather than producing a token
	// the server later rejects - a different failure point from the unresumable
	// case above, reached before MongoDB is ever consulted.
	cacheDir := t.TempDir()
	checkpointFile := filepath.Join(cacheDir, "mongodb_cdc_checkpoint")
	require.NoError(t, os.WriteFile(checkpointFile, []byte("}{ not extended json"), 0o644))

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
`))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+cacheDir+`'`))
	output := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
	logs := &logCapture{}
	builder.SetLogger(slog.New(logs))
	stream := &streamHelper{builder: builder}

	wait := stream.RunAsync(t)
	require.Eventually(t, func() bool {
		msgs, err := output.messages()
		return err == nil && len(msgs) >= 2
	}, 60*time.Second, 250*time.Millisecond, "a corrupt checkpoint must not stop the snapshot from running")

	corrupt := logs.matching("Stored checkpoint is corrupt")
	require.NotEmpty(t, corrupt, "expected the corrupt checkpoint to be reported, captured logs: %v", logs.matching(""))
	t.Logf("recovery (x%d): %s", len(corrupt), corrupt[0])

	require.Eventually(t, func() bool {
		b, err := os.ReadFile(checkpointFile)
		return err == nil && string(b) != "}{ not extended json"
	}, 60*time.Second, 250*time.Millisecond, "the corrupt checkpoint was never replaced")

	stream.StopWithin(t, 30*time.Second)
	wait()
	require.Len(t, output.Messages(t), 2, "the snapshot must deliver each document once")

	// The replacement is a real checkpoint, so a restart resumes rather than
	// re-snapshotting or reporting corruption again.
	corruptBeforeRestart := len(logs.matching("Stored checkpoint is corrupt"))
	wait = stream.RunAsync(t)
	time.Sleep(5 * time.Second)
	stream.StopWithin(t, 30*time.Second)
	wait()
	require.Len(t, output.Messages(t), 2, "the replacement checkpoint must resume rather than re-snapshot")
	require.Len(t, logs.matching("Stored checkpoint is corrupt"), corruptBeforeRestart,
		"the corruption must not be reported again once a valid checkpoint is stored")
}

// TestIntegrationMongoCDCCorruptCheckpointWithoutSnapshot pins that a corrupt
// checkpoint goes through the same recovery policy as an unresumable position:
// with stream_snapshot disabled and the default on_unresumable_position: fail,
// clearing it would silently skip every change since the stored position, so the
// input must refuse — keeping the corrupt entry for inspection and failing on
// every reconnect instead of quietly starting over from the oplog end.
func TestIntegrationMongoCDCCorruptCheckpointWithoutSnapshot(t *testing.T) {
	integration.CheckSkip(t)
	uri, mongoClient := startMongoContainer(t)
	db := &databaseHelper{mongoClient.Database("test")}
	db.CreateCollection(t, "foo")
	db.InsertOne(t, "foo", bson.M{"_id": 1, "data": "hello"})

	cacheDir := t.TempDir()
	checkpointFile := filepath.Join(cacheDir, "mongodb_cdc_checkpoint")
	const corruptBytes = "}{ not extended json"
	require.NoError(t, os.WriteFile(checkpointFile, []byte(corruptBytes), 0o644))

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.AddInputYAML(`
mongodb_cdc:
  url: '`+uri+`'
  database: 'test'
  checkpoint_cache: 'filecache'
  stream_snapshot: false
  json_marshal_mode: relaxed
  collections:
    - 'foo'
`))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+cacheDir+`'`))
	output := &outputHelper{}
	require.NoError(t, builder.AddBatchConsumerFunc(output.AddBatch))
	logs := &logCapture{}
	builder.SetLogger(slog.New(logs))
	stream := &streamHelper{builder: builder}

	wait := stream.RunAsync(t)
	require.Eventually(t, func() bool {
		return len(logs.matching("cannot be decoded")) > 0
	}, 30*time.Second, 250*time.Millisecond, "expected the refusal to be reported, captured logs: %v", logs.matching(""))
	stream.StopWithin(t, 30*time.Second)
	wait()

	require.Empty(t, output.Messages(t), "a refused corrupt checkpoint must not deliver anything")
	b, err := os.ReadFile(checkpointFile)
	require.NoError(t, err)
	require.Equal(t, corruptBytes, string(b), "the corrupt entry must be preserved for inspection")
}

// TestIntegrationMongoCDCSnapshotRestartChaos restarts the input repeatedly
// while a snapshot is in flight and requires two things of the result: every
// document is delivered at least once across all runs (duplicates are expected
// and fine, gaps are not), and once a snapshot has run to completion a later
// start does not re-run it.
func TestIntegrationMongoCDCSnapshotRestartChaos(t *testing.T) {
	const docCount = 300
	// read_batch_size is deliberately tiny - it is the snapshot cursor's batch
	// size, so the documents arrive as many small batches - and a sleep processor
	// slows their consumption. Both are needed to make the restarts land inside
	// the snapshot: unthrottled, a local container snapshots 300 (or even 5000)
	// documents in well under the shortest pause below, and every run would
	// complete the snapshot instead of interrupting it.
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  stream_snapshot: true
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  read_batch_size: 5
  collections:
    - 'foo'
`)
	require.NoError(t, stream.builder.AddProcessorYAML(`sleep: {duration: 20ms}`))
	db.CreateCollection(t, "foo")
	docs := make([]any, 0, docCount)
	for id := 1; id <= docCount; id++ {
		docs = append(docs, bson.M{"_id": id, "data": "hello"})
	}
	db.InsertMany(t, "foo", docs...)

	// seenIDs must not fail the test itself: it runs inside require.Eventually
	// conditions, which testify spawns on a separate goroutine where FailNow
	// (runtime.Goexit) silently kills the tick instead of failing the test.
	// Shape problems are returned as an error and asserted on the test
	// goroutine after the wait.
	seenIDs := func() (map[int]bool, error) {
		msgs, err := output.messages()
		if err != nil {
			return nil, err
		}
		ids := map[int]bool{}
		for _, msg := range msgs {
			doc, ok := msg.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("unexpected message shape: %T", msg)
			}
			num, ok := doc["_id"].(json.Number)
			if !ok {
				return nil, fmt.Errorf("unexpected _id shape: %T", doc["_id"])
			}
			id, err := num.Int64()
			if err != nil {
				return nil, err
			}
			ids[int(id)] = true
		}
		return ids, nil
	}
	mustSeenIDs := func(t *testing.T) map[int]bool {
		t.Helper()
		ids, err := seenIDs()
		require.NoError(t, err)
		return ids
	}
	missing := func(ids map[int]bool) []int {
		var out []int
		for id := 1; id <= docCount; id++ {
			if !ids[id] {
				out = append(out, id)
			}
		}
		return out
	}

	// Chaos: start and kill the input at randomised - but deterministic - points,
	// several of which land inside the snapshot.
	for i := range 4 {
		r := rand.New(rand.NewPCG(0x5eed, uint64(i)))
		pause := time.Duration(100+r.IntN(301)) * time.Millisecond
		t.Logf("chaos run %d: stopping after %v", i, pause)
		wait := stream.RunAsync(t)
		time.Sleep(pause)
		stream.StopWithin(t, 30*time.Second)
		wait()
		t.Logf("chaos run %d: %d/%d distinct ids seen so far", i, len(mustSeenIDs(t)), docCount)
	}

	// Final run, allowed to finish.
	wait := stream.RunAsync(t)
	require.Eventually(t, func() bool {
		ids, err := seenIDs()
		return err == nil && len(missing(ids)) == 0
	}, 60*time.Second, 250*time.Millisecond, "documents never fully delivered")
	require.Empty(t, missing(mustSeenIDs(t)))
	stream.StopWithin(t, 30*time.Second)
	wait()

	// The completed snapshot was checkpointed, so a further start with no new
	// writes must not re-read anything.
	before := len(output.Messages(t))
	wait = stream.RunAsync(t)
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 30*time.Second)
	wait()
	require.Len(t, output.Messages(t), before, "a completed snapshot must not be re-run after a restart")
}

// ---------------------------------------------------------------------------
// Schema integration tests
// ---------------------------------------------------------------------------

func TestIntegrationMongoCDCSchemaOnInsert(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": "1", "name": "alice", "age": int32(30)})
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	schemas := output.Schemas(t)
	require.Len(t, schemas, 1)
	s := schemas[0]
	assert.Equal(t, "foo", s.Name)
	assert.Equal(t, schema.Object, s.Type)
	require.Len(t, s.Children, 3)
	// Alphabetically sorted
	assert.Equal(t, "_id", s.Children[0].Name)
	assert.Equal(t, schema.String, s.Children[0].Type)
	assert.Equal(t, "age", s.Children[1].Name)
	assert.Equal(t, schema.Int32, s.Children[1].Type)
	assert.Equal(t, "name", s.Children[2].Name)
	assert.Equal(t, schema.String, s.Children[2].Type)
	for _, c := range s.Children {
		assert.True(t, c.Optional)
	}
}

func TestIntegrationMongoCDCSnapshotSchema(t *testing.T) {
	stream, db, output := setup(t, `
read_until:
  idle_timeout: 3s
  input:
    mongodb_cdc:
      url: '$URI'
      database: '$DATABASE'
      checkpoint_cache: '$CACHE'
      stream_snapshot: true
      collections:
        - 'foo'
`)
	db.CreateCollection(t, "foo")
	for i := range 5 {
		db.InsertOne(t, "foo", bson.M{"_id": i + 1, "name": fmt.Sprintf("user%d", i), "value": "x"})
	}
	stream.Run(t)
	stream.Stop(t)

	schemas := output.Schemas(t)
	require.GreaterOrEqual(t, len(schemas), 5)
	for i, s := range schemas {
		assert.Equal(t, "foo", s.Name, "schema %d", i)
		assert.Equal(t, schema.Object, s.Type, "schema %d", i)
		require.Len(t, s.Children, 3, "schema %d", i)
		assert.Equal(t, "_id", s.Children[0].Name)
		assert.Equal(t, "name", s.Children[1].Name)
		assert.Equal(t, "value", s.Children[2].Name)
	}
}

func TestIntegrationMongoCDCSchemaChange(t *testing.T) {
	stream, db, output := setup(t, `
read_until:
  idle_timeout: 3s
  input:
    mongodb_cdc:
      url: '$URI'
      database: '$DATABASE'
      checkpoint_cache: '$CACHE'
      stream_snapshot: true
      collections:
        - 'foo'
`)
	db.CreateCollection(t, "foo")
	// First doc: 2 fields
	db.InsertOne(t, "foo", bson.M{"_id": 1, "name": "alice"})
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	// Second doc: 3 fields — triggers schema change via key-set fingerprinting
	db.InsertOne(t, "foo", bson.M{"_id": 2, "name": "bob", "email": "bob@test.com"})
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	schemas := output.Schemas(t)
	require.GreaterOrEqual(t, len(schemas), 2)
	// First message (snapshot): [_id, name]
	assert.Len(t, schemas[0].Children, 2)
	assert.Equal(t, "_id", schemas[0].Children[0].Name)
	assert.Equal(t, "name", schemas[0].Children[1].Name)
	// Last message (insert with email): [_id, email, name]
	last := schemas[len(schemas)-1]
	assert.Len(t, last.Children, 3)
	assert.Equal(t, "_id", last.Children[0].Name)
	assert.Equal(t, "email", last.Children[1].Name)
	assert.Equal(t, "name", last.Children[2].Name)
}

func TestIntegrationMongoCDCSchemaOrdering(t *testing.T) {
	stream, db, output := setup(t, `
read_until:
  idle_timeout: 3s
  input:
    mongodb_cdc:
      url: '$URI'
      database: '$DATABASE'
      checkpoint_cache: '$CACHE'
      stream_snapshot: true
      collections:
        - 'foo'
`)
	db.CreateCollection(t, "foo")
	for i := range 20 {
		db.InsertOne(t, "foo", bson.M{
			"_id":   i + 1,
			"zulu":  "z",
			"alpha": "a",
			"mike":  "m",
		})
	}
	stream.Run(t)
	stream.Stop(t)

	schemas := output.Schemas(t)
	require.GreaterOrEqual(t, len(schemas), 20)
	expected := []string{"_id", "alpha", "mike", "zulu"}
	for i, s := range schemas {
		names := make([]string, len(s.Children))
		for j, c := range s.Children {
			names[j] = c.Name
		}
		assert.Equal(t, expected, names, "schema %d has wrong field order", i)
	}
}

func TestIntegrationMongoCDCMultiCollectionSchema(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  collections:
    - 'users'
    - 'events'
`)
	db.CreateCollection(t, "users")
	db.CreateCollection(t, "events")
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	db.InsertOne(t, "users", bson.M{"_id": "1", "name": "alice", "age": int32(30)})
	db.InsertOne(t, "events", bson.M{"_id": "1", "type": "login", "ts": bson.DateTime(time.Now().UnixMilli())})
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	schemas := output.Schemas(t)
	require.Len(t, schemas, 2)

	// Find schemas by collection name
	schemaByName := map[string]schema.Common{}
	for _, s := range schemas {
		schemaByName[s.Name] = s
	}

	users := schemaByName["users"]
	require.Len(t, users.Children, 3)
	assert.Equal(t, "_id", users.Children[0].Name)
	assert.Equal(t, schema.String, users.Children[0].Type)
	assert.Equal(t, "age", users.Children[1].Name)
	assert.Equal(t, schema.Int32, users.Children[1].Type)
	assert.Equal(t, "name", users.Children[2].Name)
	assert.Equal(t, schema.String, users.Children[2].Type)

	events := schemaByName["events"]
	require.Len(t, events.Children, 3)
	assert.Equal(t, "_id", events.Children[0].Name)
	assert.Equal(t, schema.String, events.Children[0].Type)
	assert.Equal(t, "ts", events.Children[1].Name)
	assert.Equal(t, schema.Timestamp, events.Children[1].Type)
	assert.Equal(t, "type", events.Children[2].Name)
	assert.Equal(t, schema.String, events.Children[2].Type)
}

func TestIntegrationMongoCDCDeleteUsesCache(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": "1", "name": "alice"})
	time.Sleep(time.Second)
	db.DeleteByID(t, "foo", "1")
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	schemas := output.Schemas(t)
	require.Len(t, schemas, 2)
	// Insert schema
	assert.Equal(t, "foo", schemas[0].Name)
	assert.Len(t, schemas[0].Children, 2)
	// Delete should use cached schema (same as insert)
	assert.Equal(t, "foo", schemas[1].Name)
	assert.Len(t, schemas[1].Children, 2)
	assert.Equal(t, schemas[0].Children[0].Name, schemas[1].Children[0].Name)
	assert.Equal(t, schemas[0].Children[1].Name, schemas[1].Children[1].Name)
}

func TestIntegrationMongoCDCSchemaValidator(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo", options.CreateCollection().SetValidator(bson.M{
		"$jsonSchema": bson.M{
			"bsonType": "object",
			"required": bson.A{"name"},
			"properties": bson.M{
				"name":   bson.M{"bsonType": "string"},
				"age":    bson.M{"bsonType": "int"},
				"active": bson.M{"bsonType": "bool"},
			},
		},
	}))
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	// Insert a document that matches the validator and also has _id (not in the validator).
	db.InsertOne(t, "foo", bson.M{"_id": "1", "name": "alice", "age": int32(30), "active": true})
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	schemas := output.Schemas(t)
	require.Len(t, schemas, 1)
	s := schemas[0]
	assert.Equal(t, "foo", s.Name)
	assert.Equal(t, schema.Object, s.Type)
	// The $jsonSchema validator has 3 properties (name, age, active). The _id field
	// is auto-injected into the Tier 1 schema so the key-set fingerprint matches the
	// document's 4 fields (_id, active, age, name). The Tier 1 schema is preserved,
	// keeping the required/optional classification from the validator.
	require.Len(t, s.Children, 4)
	assert.Equal(t, "_id", s.Children[0].Name)
	assert.Equal(t, schema.String, s.Children[0].Type)
	assert.True(t, s.Children[0].Optional) // auto-injected

	assert.Equal(t, "active", s.Children[1].Name)
	assert.Equal(t, schema.Boolean, s.Children[1].Type)
	assert.True(t, s.Children[1].Optional) // not in required

	assert.Equal(t, "age", s.Children[2].Name)
	assert.Equal(t, schema.Int32, s.Children[2].Type)
	assert.True(t, s.Children[2].Optional) // not in required

	assert.Equal(t, "name", s.Children[3].Name)
	assert.Equal(t, schema.String, s.Children[3].Type)
	assert.False(t, s.Children[3].Optional) // in required — Tier 1 preserved
}

func TestIntegrationMongoCDCPartialUpdateSchema(t *testing.T) {
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  document_mode: partial_update
  collections:
    - 'foo'
`)
	db.CreateCollection(t, "foo")
	wait := stream.RunAsync(t)
	time.Sleep(2 * time.Second)
	db.InsertOne(t, "foo", bson.M{"_id": "1", "name": "alice", "age": int32(30)})
	time.Sleep(time.Second)
	db.UpdateOne(t, "foo", "1", bson.M{"$set": bson.M{"age": int32(31)}})
	time.Sleep(3 * time.Second)
	stream.StopWithin(t, 10*time.Second)
	wait()

	msgs := output.Messages(t)
	require.Len(t, msgs, 2)
	schemas := output.Schemas(t)
	require.Len(t, schemas, 2)

	// Insert: full document schema — [_id: String, age: Int32, name: String]
	assert.Equal(t, "foo", schemas[0].Name)
	require.Len(t, schemas[0].Children, 3)
	assert.Equal(t, "_id", schemas[0].Children[0].Name)
	assert.Equal(t, "age", schemas[0].Children[1].Name)
	assert.Equal(t, schema.Int32, schemas[0].Children[1].Type)
	assert.Equal(t, "name", schemas[0].Children[2].Name)

	// Partial update: should use the CACHED schema from the insert, NOT infer
	// from the synthetic {_id, operations} structure.
	assert.Equal(t, "foo", schemas[1].Name)
	require.Len(t, schemas[1].Children, 3, "partial update should use cached 3-field schema, not synthetic doc")
	assert.Equal(t, "_id", schemas[1].Children[0].Name)
	assert.Equal(t, "age", schemas[1].Children[1].Name)
	assert.Equal(t, "name", schemas[1].Children[2].Name)
}
