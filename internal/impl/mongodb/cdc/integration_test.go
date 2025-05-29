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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	mongocontainer "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, stream.Run(t.Context()))
	}()
	return wg.Wait
}

func (s *streamHelper) RunAsyncWithErrors(t *testing.T) func() {
	stream := s.makeStream(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Error(t, stream.Run(t.Context()))
	}()
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
	o.mu.Lock()
	defer o.mu.Unlock()
	var msgs []any
	for _, b := range o.batches {
		for _, m := range b {
			msg, err := m.AsStructured()
			require.NoError(t, err)
			msgs = append(msgs, msg)
		}
	}
	return msgs
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

func setup(t *testing.T, template string, opts ...setupOption) (*streamHelper, *databaseHelper, *outputHelper) {
	integration.CheckSkip(t)
	t.Helper()
	container, err := mongocontainer.Run(
		t.Context(),
		"mongo:7",
		mongocontainer.WithUsername("mongoadmin"),
		mongocontainer.WithPassword("secret"),
		mongocontainer.WithReplicaSet("rs0"),
	)
	t.Cleanup(func() {
		//nolint:usetesting // t.Context() is already cancelled when cleanup runs
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
	require.NoError(t, mongoClient.Ping(t.Context(), nil))
	for _, opt := range opts {
		require.NoError(t, opt(mongoClient))
	}
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
		// Write a million messages
		for batch := range 1_000 {
			idRangeStart := batch * 1_000
			batch := []any{}
			for id := range 1_000 {
				batch = append(batch, bson.M{"_id": idRangeStart + id + 1, "data": "hello"})
			}
			db.InsertMany(t, "foo", batch...)
		}
		stream.Run(t)
		expected := map[any]bool{}
		for i := range 1_000_000 {
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
	// For some reason the stream's Run doesn't exit until the context is cancelled.
	// I'm not sure why that doesn't work, but for this test we can just cancel and
	// let the cancelation happen after the test is done.
	//
	// Ideally wait would return immediately after StopNow is called...
	wait := stream.RunAsyncWithErrors(t)
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
	time.Sleep(5 * time.Second)
	stream.Stop(t)
	wait()
	require.JSONEq(t, `[{"_id":1,"data":"hello"}, {"_id":2,"data":"hello"}]`, output.MessagesJSON(t))
	wait = stream.RunAsync(t)
	time.Sleep(5 * time.Second)
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
