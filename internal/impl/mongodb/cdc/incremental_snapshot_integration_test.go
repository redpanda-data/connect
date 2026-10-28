// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cdc

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestIntegrationIncSnapshotDeps(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	_, client := startMongoContainer(t)
	db := client.Database(testDatabaseName(t))
	ctx := t.Context()

	// One document per BSON type an _id commonly takes. A $gt/$lte chunk
	// query on any one of these types would match no other.
	oid := bson.NewObjectID()
	ids := []any{
		int32(1), int64(2), 3.5, "a", "b",
		bson.D{{Key: "k", Value: 1}},
		bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")},
		oid, true,
		bson.NewDateTimeFromTime(time.UnixMilli(1760000000000)),
	}
	var docs []any
	for i, id := range ids {
		docs = append(docs, bson.D{{Key: "_id", Value: id}, {Key: "n", Value: i}})
	}
	_, err := db.Collection("mixed").InsertMany(ctx, docs)
	require.NoError(t, err)

	deps := &incSnapshotDeps{
		db:                  db,
		watermarkCollection: func() string { return "mixed" },
		streamedThrough:     func() bson.Timestamp { return bson.Timestamp{} },
	}
	table := incrementalsnapshot.TableID{Schema: db.Name(), Table: "mixed"}

	t.Run("key", func(t *testing.T) {
		cols, err := deps.ResolvePrimaryKey(ctx, table)
		require.NoError(t, err)
		assert.Equal(t, []string{"_id"}, cols)
	})

	t.Run("unusable", func(t *testing.T) {
		_, err := deps.ResolvePrimaryKey(ctx, incrementalsnapshot.TableID{Schema: db.Name(), Table: "missing"})
		require.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable)

		require.NoError(t, db.CreateView(ctx, "mixed_view", "mixed", []bson.D{}))
		_, err = deps.ResolvePrimaryKey(ctx, incrementalsnapshot.TableID{Schema: db.Name(), Table: "mixed_view"})
		require.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable)

		// Like a view, a time series collection rejects min/max reads and
		// its writes never reach a change stream.
		timeSeries := options.CreateCollection().SetTimeSeriesOptions(options.TimeSeries().SetTimeField("t"))
		require.NoError(t, db.CreateCollection(ctx, "mixed_ts", timeSeries))
		_, err = deps.ResolvePrimaryKey(ctx, incrementalsnapshot.TableID{Schema: db.Name(), Table: "mixed_ts"})
		require.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable)
	})

	t.Run("max key across types", func(t *testing.T) {
		// Dates sort last among these in BSON order.
		maxKey, err := deps.ResolveMaxKey(ctx, table, nil)
		require.NoError(t, err)
		want, err := documentKey(rawID(t, ids[len(ids)-1]))
		require.NoError(t, err)
		assert.Equal(t, want, maxKey)

		empty, err := deps.ResolveMaxKey(ctx, incrementalsnapshot.TableID{Schema: db.Name(), Table: "missing"}, nil)
		require.NoError(t, err)
		assert.Nil(t, empty)
	})

	t.Run("walk covers every type", func(t *testing.T) {
		upper, err := deps.ResolveMaxKey(ctx, table, nil)
		require.NoError(t, err)

		var seen []int32
		var lower incrementalsnapshot.PrimaryKey
		for range len(ids) + 2 {
			rows, err := deps.FetchChunk(ctx, table, nil, lower, upper, 3)
			require.NoError(t, err)
			for _, r := range rows {
				seen = append(seen, r.Data["n"].(int32))
			}
			if len(rows) < 3 {
				break
			}
			lower = rows[len(rows)-1].PK
		}
		require.Len(t, seen, len(ids), "seen %v", seen)
		assert.ElementsMatch(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, seen)
	})

	t.Run("walk past a NaN key", func(t *testing.T) {
		// The NaN that JavaScript drivers such as mongosh write. Go's
		// math.NaN() has other bits.
		jsNaN := math.Float64frombits(0x7ff8000000000000)
		_, err := db.Collection("nan").InsertMany(ctx, []any{
			bson.D{{Key: "_id", Value: jsNaN}},
			bson.D{{Key: "_id", Value: int32(1)}},
		})
		require.NoError(t, err)
		table := incrementalsnapshot.TableID{Schema: db.Name(), Table: "nan"}
		upper, err := deps.ResolveMaxKey(ctx, table, nil)
		require.NoError(t, err)

		// NaN sorts before every other number, so a chunk of one ends on it.
		first, err := deps.FetchChunk(ctx, table, nil, nil, upper, 1)
		require.NoError(t, err)
		require.True(t, math.IsNaN(first[0].Data["_id"].(float64)))

		next, err := deps.FetchChunk(ctx, table, nil, first[0].PK, upper, 1)
		require.NoError(t, err)
		assert.Equal(t, int32(1), next[0].Data["_id"])
	})

	t.Run("watermark", func(t *testing.T) {
		first, err := deps.ResolveWatermark(ctx)
		require.NoError(t, err)
		assert.NotZero(t, first.T)
		assert.False(t, first.Quiesced(), "the stream has consumed nothing")

		_, err = db.Collection("mixed").InsertOne(ctx, bson.D{{Key: "_id", Value: "later"}})
		require.NoError(t, err)
		second, err := deps.ResolveWatermark(ctx)
		require.NoError(t, err)
		assert.True(t, first.T.Before(second.T), "a write must advance the watermark: %v then %v", first.T, second.T)

		deps.streamedThrough = func() bson.Timestamp { return second.T }
		third, err := deps.ResolveWatermark(ctx)
		require.NoError(t, err)
		// The periodic no-op writer can move the cluster time on between
		// the two reads, so judge against what was actually read.
		assert.Equal(t, !second.T.Before(third.T), third.Quiesced(), "stream through %v, watermark %v", second.T, third.T)
	})
}

// replayed is the state a consumer upserting by _id would hold after the
// output so far, plus every ordering violation seen on the way.
type replayed struct {
	docs       map[string]float64 // collection/_id -> v
	regressed  []string
	readCount  int
	readByColl map[string]int
	// ops counts messages by operation.
	ops map[string]int
	// reads counts how often each _id was read by the snapshot.
	reads map[string]int
	// interleaved records, per collection, a snapshot read of it emitted
	// after a streamed change.
	interleaved map[string]bool
	// lastReadTime is the operation_time of the latest snapshot read.
	lastReadTime bson.Timestamp
	// signals counts signal inserts emitted from the signal collection,
	// which are otherwise left out of the replay.
	signals int
	// problems records messages that could not be replayed. replay runs
	// inside Eventually conditions, off the test goroutine, so it must not
	// fail the test itself: replayChecked does that.
	problems []string
}

// replay applies the output in order. Documents carry a counter v that every
// update increments, so a document going backwards is a stale chunk emitted
// after a newer change. It is safe to call from an Eventually condition.
func replay(o *outputHelper) replayed {
	o.mu.Lock()
	defer o.mu.Unlock()
	r := replayed{docs: map[string]float64{}, readByColl: map[string]int{}, ops: map[string]int{}, reads: map[string]int{}, interleaved: map[string]bool{}}
	streamed := false
	for _, b := range o.batches {
		for _, m := range b {
			op, _ := m.MetaGet("operation")
			coll, _ := m.MetaGet("collection")
			if coll == "signals" {
				if op == "insert" {
					r.signals++
				} else {
					r.problems = append(r.problems, fmt.Sprintf("unexpected %s message from the signal collection", op))
				}
				continue
			}
			r.ops[op]++
			r.checkOperationTime(m, op, coll)
			body, err := m.AsStructured()
			if err != nil {
				r.problems = append(r.problems, fmt.Sprintf("decoding a %s message from %s: %v", op, coll, err))
				continue
			}
			doc, ok := body.(map[string]any)
			if !ok {
				r.problems = append(r.problems, fmt.Sprintf("a %s message from %s is a %T, not a document", op, coll, body))
				continue
			}
			// Keyed by collection too: tests seed several with the same _ids.
			id := coll + "/" + fmt.Sprint(doc["_id"])
			if op == "read" {
				r.readCount++
				r.readByColl[coll]++
				r.reads[id]++
				r.interleaved[coll] = r.interleaved[coll] || streamed
			} else {
				streamed = true
			}
			switch op {
			case "delete":
				delete(r.docs, id)
				continue
			case "read", "insert", "update", "replace":
			default:
				r.problems = append(r.problems, fmt.Sprintf("unexpected operation %q on %s", op, id))
				continue
			}
			num, ok := doc["v"].(json.Number)
			if !ok {
				// update_lookup found the document already deleted; the
				// delete follows.
				continue
			}
			v, err := num.Float64()
			if err != nil {
				r.problems = append(r.problems, fmt.Sprintf("%s: counter %v: %v", id, num, err))
				continue
			}
			if prev, seen := r.docs[id]; seen && v < prev {
				r.regressed = append(r.regressed, fmt.Sprintf("%s: %v after %v (%s)", id, v, prev, op))
			}
			r.docs[id] = v
		}
	}
	return r
}

// checkOperationTime records a problem when a message's operation_time breaks
// the snapshot's ordering. A chunk is read at its high watermark and only
// released once the stream is through it, so reads never go back in time and
// a change emitted after a read happened at or after it.
func (r *replayed) checkOperationTime(m *service.Message, op, coll string) {
	raw, _ := m.MetaGet("operation_time")
	var parsed struct {
		Timestamp struct {
			T uint32 `json:"t"`
			I uint32 `json:"i"`
		} `json:"$timestamp"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		r.problems = append(r.problems, fmt.Sprintf("a %s message from %s has operation_time %q: %v", op, coll, raw, err))
		return
	}
	ts := bson.Timestamp{T: parsed.Timestamp.T, I: parsed.Timestamp.I}
	if ts.IsZero() {
		r.problems = append(r.problems, fmt.Sprintf("a %s message from %s has no operation_time", op, coll))
		return
	}
	if ts.Before(r.lastReadTime) {
		r.problems = append(r.problems, fmt.Sprintf("a %s message from %s at %v was emitted after a read at %v", op, coll, ts, r.lastReadTime))
	}
	if op == "read" {
		r.lastReadTime = ts
	}
}

// replayChecked is replay for the test goroutine: it fails the test if any
// message could not be replayed.
func replayChecked(t *testing.T, o *outputHelper) replayed {
	t.Helper()
	r := replay(o)
	require.Empty(t, r.problems, "messages that could not be replayed")
	return r
}

// of returns the documents of one collection.
func (r replayed) of(coll string) map[string]float64 {
	docs := map[string]float64{}
	for id, v := range r.docs {
		if strings.HasPrefix(id, coll+"/") {
			docs[id] = v
		}
	}
	return docs
}

// freshDatabase drops what an earlier run left on the shared container, so a
// test can run with -count.
func freshDatabase(t *testing.T, db *databaseHelper) {
	t.Helper()
	require.NoError(t, db.Drop(t.Context()))
}

func dbState(t *testing.T, db *databaseHelper, coll string) map[string]float64 {
	t.Helper()
	cursor, err := db.Collection(coll).Find(t.Context(), bson.D{})
	require.NoError(t, err)
	var docs []bson.M
	require.NoError(t, cursor.All(t.Context(), &docs))
	state := map[string]float64{}
	for _, d := range docs {
		var v float64
		switch n := d["v"].(type) {
		case int32:
			v = float64(n)
		case int64:
			v = float64(n)
		case float64:
			v = n
		}
		state[coll+"/"+fmt.Sprint(d["_id"])] = v
	}
	return state
}

func TestIntegrationMongoCDCIncrementalSnapshotConcurrentWrites(t *testing.T) {
	t.Parallel()
	stream, db, output := setup(t, `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  incremental_snapshot:
    enabled: true
    chunk_size: 50
  collections:
    - 'foo'
`)
	freshDatabase(t, db)
	db.CreateCollection(t, "foo")
	const initial = 1500
	docs := make([]any, 0, initial)
	for i := range initial {
		docs = append(docs, bson.M{"_id": i, "v": 0})
	}
	db.InsertMany(t, "foo", docs...)

	wait := stream.RunAsync(t)

	// Change the collection while it is being read: updates and deletes land
	// in chunks not yet read, in the chunk being read, and in ones already
	// emitted; inserts land both below and above the captured max _id.
	//
	// Writes go on for at least minWrite, and then until a snapshot read has
	// been emitted after a streamed change: on a loaded runner the input can
	// connect so late that a fixed window ends before streaming overlaps the
	// snapshot. maxWrite bounds a run where they never overlap.
	const minWrite, maxWrite = 3 * time.Second, 30 * time.Second
	rng := rand.New(rand.NewPCG(1, 2))
	start := time.Now()
	nextID := initial
	for i := 0; ; i++ {
		if elapsed := time.Since(start); elapsed >= maxWrite ||
			(elapsed >= minWrite && i%20 == 0 && replay(output).interleaved["foo"]) {
			break
		}
		id := rng.IntN(initial)
		switch rng.IntN(10) {
		case 0:
			db.DeleteByID(t, "foo", id)
		case 1:
			db.InsertOne(t, "foo", bson.M{"_id": nextID, "v": 0})
			nextID++
		default:
			db.UpdateOne(t, "foo", id, bson.M{"$inc": bson.M{"v": 1}})
		}
	}
	want := dbState(t, db, "foo")

	assert.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(want, replay(output).docs)
	}, time.Minute, 250*time.Millisecond)

	got := replayChecked(t, output)
	assert.True(t, got.interleaved["foo"], "the snapshot never overlapped streaming, so nothing was deduplicated")
	assert.Empty(t, got.regressed, "documents went backwards")
	assert.Equal(t, want, got.docs)
	assert.Positive(t, got.readCount, "nothing came from the snapshot")
	t.Logf("snapshot read %d documents of %d, %d documents at the end", got.readCount, initial, len(want))

	stream.Stop(t)
	wait()
}

// incrementalHarness runs successive pipelines against one database and one
// checkpoint cache, so a test can restart or reconfigure the input between
// runs.
type incrementalHarness struct {
	t        *testing.T
	uri      string
	db       *databaseHelper
	cacheDir string
	output   *outputHelper
	logs     *logCapture
}

func newIncrementalHarness(t *testing.T) *incrementalHarness {
	t.Helper()
	integration.CheckSkip(t)
	uri, client := startMongoContainer(t)
	return newIncrementalHarnessOn(t, uri, client)
}

// newIncrementalHarnessOn is newIncrementalHarness against a deployment the
// caller started.
func newIncrementalHarnessOn(t *testing.T, uri string, client *mongo.Client) *incrementalHarness {
	t.Helper()
	db := &databaseHelper{client.Database(testDatabaseName(t))}
	freshDatabase(t, db)
	return &incrementalHarness{t: t, uri: uri, db: db, cacheDir: t.TempDir(), output: &outputHelper{}, logs: &logCapture{}}
}

// stream builds a pipeline from input YAML; $URI, $DATABASE and $CACHE are
// replaced.
func (h *incrementalHarness) stream(input string) *streamHelper {
	t := h.t
	t.Helper()
	input = strings.NewReplacer(
		"$DATABASE", h.db.Name(),
		"$CACHE", "filecache",
		"$URI", h.uri,
	).Replace(input)
	builder := service.NewStreamBuilder()
	builder.SetLogger(slog.New(h.logs))
	require.NoError(t, builder.AddInputYAML(input))
	require.NoError(t, builder.AddCacheYAML(`
label: filecache
file:
  directory: '`+h.cacheDir+`'`))
	require.NoError(t, builder.AddBatchConsumerFunc(h.output.AddBatch))
	return &streamHelper{builder: builder}
}

func (h *incrementalHarness) seed(coll string, n int) {
	h.t.Helper()
	h.db.CreateCollection(h.t, coll)
	docs := make([]any, 0, n)
	for i := range n {
		docs = append(docs, bson.M{"_id": i, "v": 0})
	}
	h.db.InsertMany(h.t, coll, docs...)
}

func (h *incrementalHarness) waitReads(coll string, atLeast int) {
	h.t.Helper()
	if !assert.Eventually(h.t, func() bool {
		return replay(h.output).readByColl[coll] >= atLeast
	}, 90*time.Second, 100*time.Millisecond, "waiting for %d reads of %s", atLeast, coll) {
		for _, r := range h.logs.matching("") {
			if !strings.HasPrefix(r, "DEBUG") {
				h.t.Logf("LOG %s", r)
			}
		}
		h.t.Logf("reads so far: %v", replay(h.output).readByColl)
		h.t.FailNow()
	}
}

const incrementalInput = `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  username: mongoadmin
  password: secret
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  incremental_snapshot:
    enabled: true
    chunk_size: 20
  signal_collection_name: signals
  collections:
    - 'foo'
    - 'bar'
`

func TestIntegrationMongoCDCIncrementalSnapshotResumesAfterRestart(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	const n = 6000
	h.seed("foo", n)
	h.seed("bar", 10)

	first := h.stream(incrementalInput)
	wait := first.RunAsync(t)
	h.waitReads("foo", 1000)
	first.Stop(t)
	wait()
	stoppedAt := replayChecked(t, h.output).readByColl["foo"]
	require.Less(t, stoppedAt, n, "the snapshot finished before the restart, so nothing was resumed")

	second := h.stream(incrementalInput)
	wait = second.RunAsync(t)
	h.waitReads("foo", n)
	h.waitReads("bar", 10)
	second.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	for i := range n {
		require.Positive(t, got.reads[fmt.Sprintf("foo/%d", i)], "document %d was never read", i)
	}
	// A resume re-reads at most the chunks in flight at the stop.
	assert.Less(t, got.readByColl["foo"], n+stoppedAt, "the restart started over: %d reads, %d before the stop", got.readByColl["foo"], stoppedAt)
	t.Logf("%d reads before the stop, %d in total for %d documents", stoppedAt, got.readByColl["foo"], n)
}

func TestIntegrationMongoCDCIncrementalSnapshotSignals(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.seed("foo", 30)
	h.seed("bar", 50)
	h.db.CreateCollection(t, "signals")

	// A blocking snapshot stores a stream position.
	blocking := h.stream(`
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  username: mongoadmin
  password: secret
  checkpoint_cache: '$CACHE'
  stream_snapshot: true
  json_marshal_mode: relaxed
  collections:
    - 'foo'
    - 'bar'
`)
	wait := blocking.RunAsync(t)
	h.waitReads("bar", 50)
	h.waitReads("foo", 30)
	blocking.Stop(t)
	wait()
	afterBlocking := replayChecked(t, h.output).readCount

	// Switching to incremental with that position re-reads nothing.
	incremental := h.stream(incrementalInput)
	wait = incremental.RunAsync(t)
	h.db.InsertOne(t, "foo", bson.M{"_id": 1000, "v": 0})
	require.Eventually(t, func() bool {
		_, ok := replay(h.output).docs["foo/1000"]
		return ok
	}, time.Minute, 100*time.Millisecond, "streaming did not start")
	assert.Equal(t, afterBlocking, replayChecked(t, h.output).readCount, "switching modes re-read collections")

	// A signal reads a collection again, alongside streaming.
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"bar"}}})
	h.waitReads("bar", 100)
	h.db.InsertOne(t, "foo", bson.M{"_id": 1001, "v": 0})

	// A repeat re-reads, once however often it names the collection, and
	// takes the shared "tables" spelling and a JSON string payload.
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": `{"tables":["bar","bar"]}`})
	h.waitReads("bar", 150)

	// Unknown and log signals read nothing.
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"nope"}}})
	h.db.InsertOne(t, "signals", bson.M{"type": "log", "data": bson.M{"message": "signal test done"}})
	require.Eventually(t, func() bool {
		return len(h.logs.matching("signal test done")) > 0
	}, time.Minute, 100*time.Millisecond, "the log signal was not seen")
	incremental.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Equal(t, 150, got.readByColl["bar"], "bar read by the blocking snapshot and once per signal")
	assert.Equal(t, 30, got.readByColl["foo"])
	assert.NotEmpty(t, h.logs.matching("are not in collections"))
	// Every signal insert is emitted, the rejected one included.
	assert.Equal(t, 4, got.signals, "signal inserts emitted")
}

// signalsOnlyInput reads on demand: no snapshot of its own, incremental or
// blocking, so only a snapshot-execute signal reads a collection.
const signalsOnlyInput = `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  username: mongoadmin
  password: secret
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  incremental_snapshot:
    chunk_size: 20
  signal_collection_name: signals
  collections:
    - 'foo'
    - 'bar'
    - 'markers'
`

// streamSnapshotSignalsInput is signalsOnlyInput with the blocking snapshot
// on the first start.
const streamSnapshotSignalsInput = `
mongodb_cdc:
  url: '$URI'
  database: '$DATABASE'
  username: mongoadmin
  password: secret
  checkpoint_cache: '$CACHE'
  json_marshal_mode: relaxed
  stream_snapshot: true
  incremental_snapshot:
    chunk_size: 20
  signal_collection_name: signals
  collections:
    - 'foo'
    - 'bar'
    - 'markers'
`

// waitStreamed inserts marker documents into the markers collection until one
// is streamed, which proves the input is past any snapshot and streaming. A
// first start without a stored position streams from the moment it connects,
// so a marker written before that is never delivered and another is written.
func (h *incrementalHarness) waitStreamed(prefix string) {
	h.t.Helper()
	written := 0
	require.Eventually(h.t, func() bool {
		written++
		if _, err := h.db.Collection("markers").InsertOne(h.t.Context(), bson.M{"_id": fmt.Sprintf("%s-%d", prefix, written), "v": 0}); err != nil {
			h.t.Logf("marker insert failed: %v", err)
			return false
		}
		r := replay(h.output)
		for i := 1; i <= written; i++ {
			if _, ok := r.docs[fmt.Sprintf("markers/%s-%d", prefix, i)]; ok {
				return true
			}
		}
		return false
	}, time.Minute, 250*time.Millisecond, "streaming did not deliver a %s marker", prefix)
}

func TestIntegrationMongoCDCSignalsWithStreamSnapshot(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.seed("foo", 30)
	h.seed("bar", 50)
	h.db.CreateCollection(t, "signals")
	h.db.CreateCollection(t, "markers")

	// The blocking snapshot reads everything once; a signal then re-reads
	// bar alongside streaming, with no incremental_snapshot.enabled.
	first := h.stream(streamSnapshotSignalsInput)
	wait := first.RunAsync(t)
	h.waitReads("foo", 30)
	h.waitReads("bar", 50)
	h.waitStreamed("first")
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"bar"}}})
	h.waitReads("bar", 100)
	first.Stop(t)
	wait()
	afterFirst := replayChecked(t, h.output).readCount

	// A restart resumes the stream: no snapshot runs again, and a signal
	// still reads on demand.
	second := h.stream(streamSnapshotSignalsInput)
	wait = second.RunAsync(t)
	h.waitStreamed("second")
	assert.Equal(t, afterFirst, replayChecked(t, h.output).readCount, "the restart re-read collections")
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"foo"}}})
	h.waitReads("foo", 60)
	second.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Equal(t, 100, got.readByColl["bar"], "bar read by the blocking snapshot and once by the signal")
	assert.Equal(t, 60, got.readByColl["foo"], "foo read by the blocking snapshot and once by the signal")
	assert.Equal(t, 2, got.signals, "signal inserts emitted")
	assert.Empty(t, h.logs.matching("no backfill was queued"), "a signal was dropped")
}

func TestIntegrationMongoCDCSignalsWithoutSnapshot(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.seed("foo", 30)
	h.seed("bar", 50)
	h.db.CreateCollection(t, "signals")
	h.db.CreateCollection(t, "markers")

	stream := h.stream(signalsOnlyInput)
	wait := stream.RunAsync(t)
	h.waitStreamed("marker")
	require.Zero(t, replayChecked(t, h.output).readCount, "nothing is read before a signal asks")

	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"foo"}}})
	h.waitReads("foo", 30)
	stream.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Equal(t, 30, got.readByColl["foo"])
	assert.Zero(t, got.readByColl["bar"], "only the signalled collection is read")
	for i := range 30 {
		assert.Equal(t, 1, got.reads[fmt.Sprintf("foo/%d", i)], "document %d read once", i)
	}
}

func TestIntegrationMongoCDCSignalsResumeAfterRestart(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	const n = 6000
	h.seed("foo", n)
	h.db.CreateCollection(t, "bar")
	h.db.CreateCollection(t, "signals")
	h.db.CreateCollection(t, "markers")

	// Stop part-way through the requested read.
	first := h.stream(signalsOnlyInput)
	wait := first.RunAsync(t)
	h.waitStreamed("marker")
	h.db.InsertOne(t, "signals", bson.M{"type": "snapshot-execute", "data": bson.M{"collections": bson.A{"foo"}}})
	h.waitReads("foo", 1000)
	first.Stop(t)
	wait()
	stoppedAt := replayChecked(t, h.output).readByColl["foo"]
	require.Less(t, stoppedAt, n, "the read finished before the restart, so nothing was resumed")

	// The request outlives the restart: the signal insert is behind the
	// stored position, so only the stored progress can carry it on.
	second := h.stream(signalsOnlyInput)
	wait = second.RunAsync(t)
	h.waitReads("foo", n)
	second.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	for i := range n {
		require.Positive(t, got.reads[fmt.Sprintf("foo/%d", i)], "document %d was never read", i)
	}
	assert.Less(t, got.readByColl["foo"], n+stoppedAt, "the restart started over: %d reads, %d before the stop", got.readByColl["foo"], stoppedAt)
	t.Logf("%d reads before the stop, %d in total for %d documents", stoppedAt, got.readByColl["foo"], n)
}

func TestIntegrationMongoCDCIncrementalSnapshotMixedIDTypes(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.db.CreateCollection(t, "foo")
	h.db.CreateCollection(t, "bar")
	var docs []any
	for i := range 25 {
		docs = append(docs,
			bson.M{"_id": i, "v": 0},
			bson.M{"_id": fmt.Sprintf("s%02d", i), "v": 0},
			bson.M{"_id": bson.NewObjectID(), "v": 0},
			bson.M{"_id": float64(i) + 0.5, "v": 0},
			bson.M{"_id": bson.D{{Key: "k", Value: i}}, "v": 0},
		)
	}
	h.db.InsertMany(t, "foo", docs...)

	stream := h.stream(incrementalInput)
	wait := stream.RunAsync(t)
	h.waitReads("foo", len(docs))
	// Changes to keys of every type stream through the dedup path too.
	h.db.UpdateOne(t, "foo", "s03", bson.M{"$inc": bson.M{"v": 1}})
	h.db.UpdateOne(t, "foo", bson.D{{Key: "k", Value: 4}}, bson.M{"$inc": bson.M{"v": 1}})
	require.Eventually(t, func() bool {
		return replay(h.output).ops["update"] == 2
	}, time.Minute, 100*time.Millisecond, "updates to string and document keys did not stream")
	stream.Stop(t)
	wait()

	got := replayChecked(t, h.output)
	assert.Equal(t, len(docs), got.readByColl["foo"], "every document read exactly once")
	assert.Empty(t, got.regressed)
}

func TestIntegrationMongoCDCIncrementalSnapshotUnresumableRequeues(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.seed("foo", 100)
	h.seed("bar", 10)

	first := h.stream(incrementalInput)
	wait := first.RunAsync(t)
	h.waitReads("foo", 100)
	h.waitReads("bar", 10)
	first.Stop(t)
	wait()
	stateFile := filepath.Join(h.cacheDir, "mongodb_cdc_checkpoint"+incSnapshotKeySuffix)
	_, err := os.Stat(stateFile)
	require.NoError(t, err, "no incremental snapshot progress was stored")

	// A position the server cannot resume from, with the progress left in
	// place: refer to TestIntegrationMongoCDCUnresumableCheckpointToken.
	rawToken, err := bson.Marshal(bson.M{"_data": "DEADBEEF"})
	require.NoError(t, err)
	encoded, err := bson.MarshalExtJSON(bson.Raw(rawToken), true, false)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(h.cacheDir, "mongodb_cdc_checkpoint"), encoded, 0o644))

	second := h.stream(incrementalInput)
	wait = second.RunAsync(t)
	// Recovery starts the stream afresh, so the stale progress must not mark
	// the collections read: both are read again, incrementally.
	h.waitReads("foo", 200)
	h.waitReads("bar", 20)
	second.Stop(t)
	wait()
	assert.NotEmpty(t, h.logs.matching("no longer resumable, clearing the checkpoint"))
}

func TestIntegrationMongoCDCSignalCollectionWithPrePostImages(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	h.db.CreateCollection(t, "foo", options.CreateCollection().SetChangeStreamPreAndPostImages(bson.M{"enabled": true}))
	h.db.CreateCollection(t, "bar", options.CreateCollection().SetChangeStreamPreAndPostImages(bson.M{"enabled": true}))
	// The signal collection deliberately has no images: nothing requires
	// operators to configure them for it.
	h.db.CreateCollection(t, "signals")

	stream := h.stream(strings.Replace(incrementalInput, "  signal_collection_name: signals\n", "  signal_collection_name: signals\n  document_mode: pre_and_post_images\n", 1))
	wait := stream.RunAsync(t)
	h.db.InsertOne(t, "foo", bson.M{"_id": 0, "v": 0})
	require.Eventually(t, func() bool {
		_, ok := replay(h.output).docs["foo/0"]
		return ok
	}, time.Minute, 100*time.Millisecond, "streaming did not start")

	h.db.InsertOne(t, "signals", bson.M{"_id": "s1", "type": "log", "data": bson.M{"message": "first"}})
	require.Eventually(t, func() bool { return len(h.logs.matching("first")) > 0 }, 20*time.Second, 100*time.Millisecond, "log signal not seen: %v", h.logs.matching("ERROR"))

	// Cleaning up signals must not need images the collection lacks.
	h.db.UpdateOne(t, "signals", "s1", bson.M{"$set": bson.M{"done": true}})
	h.db.DeleteByID(t, "signals", "s1")
	h.db.InsertOne(t, "foo", bson.M{"_id": 1, "v": 0})
	require.Eventually(t, func() bool {
		_, ok := replay(h.output).docs["foo/1"]
		return ok
	}, time.Minute, 100*time.Millisecond, "the stream stopped after a signal update or delete: %v", h.logs.matching("ERROR"))
	stream.Stop(t)
	wait()
	assert.Empty(t, h.logs.matching("error watching MongoDB change stream"))
	// The insert is emitted; the update and delete of it are not.
	assert.Equal(t, 1, replayChecked(t, h.output).signals)
}

func TestIntegrationMongoCDCIncrementalSnapshotModeRoundTrip(t *testing.T) {
	t.Parallel()
	h := newIncrementalHarness(t)
	const n = 6000
	h.seed("foo", n)
	h.seed("bar", 10)

	// Stop part-way through foo, so progress points mid-read.
	first := h.stream(incrementalInput)
	wait := first.RunAsync(t)
	h.waitReads("foo", 500)
	first.Stop(t)
	wait()
	stoppedAt := replayChecked(t, h.output).readByColl["foo"]
	require.Less(t, stoppedAt, n)
	// A spell with stream_snapshot (blocking) and a stream position stored: no snapshot, and
	// the progress it cannot keep current is discarded. The signal collection
	// goes too: with one, the on-demand run would keep the progress current
	// and carry the read on instead.
	blocking := h.stream(strings.NewReplacer(
		"  incremental_snapshot:\n    enabled: true\n", "  stream_snapshot: true\n  incremental_snapshot:\n    enabled: false\n",
		"  signal_collection_name: signals\n", "",
	).Replace(incrementalInput))
	wait = blocking.RunAsync(t)
	h.db.InsertOne(t, "bar", bson.M{"_id": "blocking-marker", "v": 0})
	require.Eventually(t, func() bool {
		_, ok := replay(h.output).docs["bar/blocking-marker"]
		return ok
	}, time.Minute, 100*time.Millisecond, "streaming did not start")
	blocking.Stop(t)
	wait()
	require.NotEmpty(t, h.logs.matching("Discarding the incremental snapshot progress"))
	_, err := os.Stat(filepath.Join(h.cacheDir, "mongodb_cdc_checkpoint"+incSnapshotKeySuffix))
	require.ErrorIs(t, err, os.ErrNotExist)

	// Back to incremental: the stale mid-read progress must not be resumed.
	completeBefore := len(h.logs.matching("Incremental snapshot complete"))
	again := h.stream(incrementalInput)
	wait = again.RunAsync(t)
	h.db.InsertOne(t, "bar", bson.M{"_id": "incremental-marker", "v": 0})
	require.Eventually(t, func() bool {
		_, ok := replay(h.output).docs["bar/incremental-marker"]
		return ok
	}, time.Minute, 100*time.Millisecond, "streaming did not start")
	// Logged either way: at once when the progress is gone, or after foo is
	// read to the end when stale progress was resumed.
	require.Eventually(t, func() bool {
		return len(h.logs.matching("Incremental snapshot complete")) > completeBefore
	}, time.Minute, 100*time.Millisecond, "the incremental snapshot did not complete")
	again.Stop(t)
	wait()
	assert.Equal(t, stoppedAt, replayChecked(t, h.output).readByColl["foo"], "stale progress was resumed")
}
