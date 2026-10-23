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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestRetryable(t *testing.T) {
	plain := errors.New("boom")
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "nil", err: nil},
		{name: "plain", err: plain},
		{name: "cancelled", err: fmt.Errorf("reading: %w", context.Canceled)},
		{name: "deadline", err: fmt.Errorf("reading: %w", context.DeadlineExceeded), retryable: true},
		{name: "network label", err: mongo.CommandError{Code: 1, Labels: []string{"NetworkError"}}, retryable: true},
		{name: "server selection", err: fmt.Errorf("reading: %w", topology.ServerSelectionError{Wrapped: plain}), retryable: true},
		{name: "stepped down", err: mongo.CommandError{Code: 189, Name: "PrimarySteppedDown"}, retryable: true},
		{name: "repl state change", err: fmt.Errorf("reading: %w", mongo.CommandError{Code: 11602, Name: "InterruptedDueToReplStateChange"}), retryable: true},
		{name: "not primary", err: mongo.CommandError{Code: 10107, Name: "NotWritablePrimary"}, retryable: true},
		{name: "shutting down", err: mongo.CommandError{Code: 91, Name: "ShutdownInProgress"}, retryable: true},
		{name: "majority not available", err: mongo.CommandError{Code: 134, Name: "ReadConcernMajorityNotAvailableYet"}, retryable: true},
		{name: "unauthorized", err: mongo.CommandError{Code: 13, Name: "Unauthorized"}},
		{name: "bad value", err: mongo.CommandError{Code: 2, Name: "BadValue"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := retryable(tc.err)
			if tc.err == nil {
				assert.NoError(t, got)
				return
			}
			assert.ErrorContains(t, got, tc.err.Error())
			assert.Equal(t, tc.retryable, errors.Is(got, incrementalsnapshot.ErrRetryable))
		})
	}
}

// TestDefaultChunkSizeLeavesRoomInFlight pins why chunk_size defaults below
// checkpoint_limit: the capped tracker admits a batch larger than its free
// capacity only once nothing else is in flight, so an oversized chunk
// serializes with every streamed batch.
func TestDefaultChunkSizeLeavesRoomInFlight(t *testing.T) {
	parsed, err := spec().ParseYAML(incSnapshotTestConfig, nil)
	require.NoError(t, err)
	limit, err := parsed.FieldInt(fieldCheckpointLimit)
	require.NoError(t, err)
	chunkSize, err := parsed.Namespace(fieldIncSnapshot).FieldInt(fieldIncSnapshotChunkSize)
	require.NoError(t, err)

	track := func(cp *checkpoint.Capped[int], n int) error {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := cp.Track(ctx, 0, int64(n))
		return err
	}

	cp := checkpoint.NewCapped[int](int64(limit))
	require.NoError(t, track(cp, chunkSize), "the first chunk")
	require.NoError(t, track(cp, chunkSize), "a second chunk must fit while the first is unacknowledged")

	cp = checkpoint.NewCapped[int](int64(limit))
	require.NoError(t, track(cp, 1), "a streamed change")
	require.NoError(t, track(cp, chunkSize), "a chunk must fit alongside a streamed change")

	// The old default of 1024 waited for everything in flight.
	cp = checkpoint.NewCapped[int](int64(limit))
	require.NoError(t, track(cp, 1))
	require.ErrorIs(t, track(cp, limit+1), context.DeadlineExceeded)
}

// fakeIncDeps serves int32 _ids per collection, always quiesced at one cluster
// time, so every advance releases the buffered chunk.
type fakeIncDeps struct {
	t       *testing.T
	ids     map[string][]int32
	failing int // retryable failures left before reads succeed
}

func (*fakeIncDeps) ResolvePrimaryKey(context.Context, incrementalsnapshot.TableID) ([]string, error) {
	return []string{"_id"}, nil
}

func (f *fakeIncDeps) ResolveMaxKey(_ context.Context, table incrementalsnapshot.TableID, _ []string) (incrementalsnapshot.PrimaryKey, error) {
	ids := f.ids[table.Table]
	if len(ids) == 0 {
		return nil, nil
	}
	return key(f.t, slices.Max(ids)), nil
}

func (*fakeIncDeps) ResolveWatermark(context.Context) (clusterTimeWatermark, error) {
	return newClusterTimeWatermark(bson.Timestamp{T: 1}, bson.Timestamp{T: 1}), nil
}

func (*fakeIncDeps) Prepare(context.Context) error { return nil }

func (f *fakeIncDeps) FetchChunk(ctx context.Context, table incrementalsnapshot.TableID, _ []string, lower, upper incrementalsnapshot.PrimaryKey, limit int) ([]incrementalsnapshot.Row, error) {
	if f.failing > 0 {
		f.failing--
		return nil, retryable(mongo.CommandError{Code: 11602, Name: "InterruptedDueToReplStateChange"})
	}
	return fetchChunk(ctx, &fakeChunkReader{t: f.t, ids: f.ids[table.Table]}, table, lower, upper, limit)
}

// recordingMetrics is a metrics exporter recording counter totals and gauge
// values by name and label values.
type recordingMetrics struct {
	mu     sync.Mutex
	values map[string]int64
}

type recordingMetric struct {
	r   *recordingMetrics
	key string
}

func (m recordingMetric) Incr(n int64)                { m.r.update(m.key, func(v int64) int64 { return v + n }) }
func (m recordingMetric) IncrFloat64(n float64)       { m.Incr(int64(n)) }
func (m recordingMetric) Set(n int64)                 { m.r.update(m.key, func(int64) int64 { return n }) }
func (m recordingMetric) SetFloat64(n float64)        { m.Set(int64(n)) }
func (m recordingMetric) DecrFloat64(n float64)       { m.Incr(-int64(n)) }
func (recordingMetric) Timing(int64)                  {}
func (*recordingMetrics) Close(context.Context) error { return nil }

func metricKey(name string, labels []string) string {
	return strings.Join(append([]string{name}, labels...), "|")
}

func (r *recordingMetrics) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(labels ...string) service.MetricsExporterCounter {
		return recordingMetric{r: r, key: metricKey(name, labels)}
	}
}

func (r *recordingMetrics) NewTimerCtor(name string, _ ...string) service.MetricsExporterTimerCtor {
	return func(labels ...string) service.MetricsExporterTimer {
		return recordingMetric{r: r, key: metricKey(name, labels)}
	}
}

func (r *recordingMetrics) NewGaugeCtor(name string, _ ...string) service.MetricsExporterGaugeCtor {
	return func(labels ...string) service.MetricsExporterGauge {
		return recordingMetric{r: r, key: metricKey(name, labels)}
	}
}

func (r *recordingMetrics) update(key string, fn func(int64) int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[key] = fn(r.values[key])
}

func (r *recordingMetrics) get(name string, labels ...string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.values[metricKey(name, labels)]
}

func newRecordingResources(t *testing.T, logs *logCapture) (*service.Resources, *recordingMetrics) {
	t.Helper()
	rec := &recordingMetrics{values: map[string]int64{}}
	env := service.NewEmptyEnvironment()
	require.NoError(t, env.RegisterMetricsExporter("recording", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return rec, nil }))
	// The resource builder defaults to the "none" tracer, which lives in a
	// plugin package this test binary does not import.
	require.NoError(t, env.RegisterOtelTracerProvider("none", service.NewConfigSpec(),
		func(*service.ParsedConfig) (trace.TracerProvider, error) { return noop.NewTracerProvider(), nil }))
	builder := env.NewResourceBuilder()
	require.NoError(t, builder.SetMetricsYAML("recording: {}"))
	builder.SetLogger(slog.New(logs))
	res, closeFn, err := builder.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn(context.Background()) })
	return res, rec
}

func TestIncSnapshotRunMetricsAndLogs(t *testing.T) {
	logs := &logCapture{}
	res, rec := newRecordingResources(t, logs)
	deps := &fakeIncDeps{t: t, ids: map[string][]int32{"a": {1, 2, 3}, "b": {1, 2}}, failing: 1}

	run := newIncSnapshotRunBase("db", res.Logger(), res.Metrics())
	require.NoError(t, run.setUp(deps, []string{"a", "b"}, 2, 0, nil))
	require.NoError(t, run.start(t.Context()))
	assert.EqualValues(t, 1, rec.get(metricIncSnapshotDeferred), "the first read failed transiently")
	assert.EqualValues(t, 2, rec.get(metricIncSnapshotRemaining))

	var released []int32
	emit := func(rows []incrementalsnapshot.Row) error {
		released = append(released, rowIDs(rows)...)
		return nil
	}
	for i := 0; !run.coord.Idle(); i++ {
		require.Less(t, i, 20, "the snapshot never finished")
		require.NoError(t, run.advance(t.Context(), bson.Timestamp{T: uint32(2 + i)}, emit))
	}

	assert.Len(t, released, 5)
	assert.EqualValues(t, 3, rec.get(metricIncSnapshotDocuments, "a"))
	assert.EqualValues(t, 2, rec.get(metricIncSnapshotDocuments, "b"))
	assert.EqualValues(t, 2, rec.get(metricIncSnapshotChunks, "a"))
	assert.EqualValues(t, 1, rec.get(metricIncSnapshotChunks, "b"))
	assert.EqualValues(t, 1, rec.get(metricIncSnapshotDeferred))
	assert.EqualValues(t, 0, rec.get(metricIncSnapshotRemaining))

	assert.Len(t, logs.matching("Incremental snapshot reading collection a"), 1)
	assert.Len(t, logs.matching("Incremental snapshot finished reading collection a, 3 documents"), 1)
	assert.Len(t, logs.matching("Incremental snapshot reading collection b"), 1)
	assert.Len(t, logs.matching("Incremental snapshot finished reading collection b, 2 documents"), 1)
}

// loadFixture is a mongoCDC with a stream checkpoint and snapshot progress in
// one cache, as Connect sees them.
type loadFixture struct {
	m     *mongoCDC
	token bson.Raw
	res   *service.Resources
}

func newLoadFixture(t *testing.T) *loadFixture {
	t.Helper()
	const cacheName = "checkpoints"
	res := service.MockResources(service.MockResourcesOptAddCache(cacheName))
	// Connect does not dial: only the database name is used.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	raw, err := bson.Marshal(bson.M{"_data": "position"})
	require.NoError(t, err)
	return &loadFixture{
		m: &mongoCDC{
			logger:      res.Logger(),
			db:          client.Database("db"),
			collections: []string{"foo", "bar"},
			incremental: true,
			checkpoint:  &checkpointCache{cacheEntry{resources: res, cacheName: cacheName, cacheKey: "key"}},
			incStateStore: &incStateCache{cacheEntry{
				resources: res, cacheName: cacheName, cacheKey: "key" + incSnapshotKeySuffix,
			}},
		},
		token: bson.Raw(raw),
		res:   res,
	}
}

func (f *loadFixture) storeProgress(t *testing.T, b []byte) {
	t.Helper()
	require.NoError(t, f.m.incStateStore.Store(t.Context(), b))
}

func (f *loadFixture) storedToken(t *testing.T) bson.Raw {
	t.Helper()
	token, err := f.m.checkpoint.Load(t.Context())
	require.NoError(t, err)
	return token
}

func TestLoadIncSnapshotState(t *testing.T) {
	tables := func(names ...string) []incrementalsnapshot.TableID {
		var out []incrementalsnapshot.TableID
		for _, n := range names {
			out = append(out, incrementalsnapshot.TableID{Schema: "db", Table: n})
		}
		return out
	}

	t.Run("no stream position ignores stored progress", func(t *testing.T) {
		f := newLoadFixture(t)
		f.storeProgress(t, []byte(`{`))
		state, token, err := f.m.loadIncSnapshotState(t.Context(), nil)
		require.NoError(t, err)
		assert.Nil(t, state)
		assert.Nil(t, token)
	})

	t.Run("position without progress marks every collection read", func(t *testing.T) {
		f := newLoadFixture(t)
		state, token, err := f.m.loadIncSnapshotState(t.Context(), f.token)
		require.NoError(t, err)
		assert.Equal(t, f.token, token)
		require.NotNil(t, state)
		assert.Equal(t, tables("foo", "bar"), state.Tables)
		assert.Empty(t, state.RemainingTables)
		assert.Nil(t, state.CurrentTable)
	})

	t.Run("corrupt progress keeps the position and queues every collection", func(t *testing.T) {
		f := newLoadFixture(t)
		require.NoError(t, f.m.checkpoint.Store(t.Context(), f.token))
		f.storeProgress(t, []byte(`{`))

		state, token, err := f.m.loadIncSnapshotState(t.Context(), f.token)
		require.NoError(t, err)
		assert.Nil(t, state, "no progress, so every collection is read again")
		assert.Equal(t, f.token, token, "the stream must resume from its position, or deletes since it are lost")
		assert.Equal(t, f.token, f.storedToken(t), "the stored position must survive")
		stored, err := f.m.incStateStore.Load(t.Context())
		require.NoError(t, err)
		assert.Nil(t, stored, "the corrupt progress is cleared")

		run := newIncSnapshotRunBase("db", f.res.Logger(), f.res.Metrics())
		require.NoError(t, run.setUp(&fakeIncDeps{t: t}, f.m.collections, 10, 0, state))
		assert.Equal(t, tables("foo", "bar"), run.coord.State().RemainingTables)
	})

	t.Run("valid progress resumes", func(t *testing.T) {
		f := newLoadFixture(t)
		want := &incrementalsnapshot.State{
			Version:         incrementalsnapshot.CurrentStateVersion,
			CurrentTable:    &tables("foo")[0],
			LastSentPK:      key(t, 4),
			MaxPK:           key(t, 9),
			RemainingTables: tables("bar"),
			Tables:          tables("foo", "bar"),
		}
		b, err := json.Marshal(want)
		require.NoError(t, err)
		f.storeProgress(t, b)

		state, token, err := f.m.loadIncSnapshotState(t.Context(), f.token)
		require.NoError(t, err)
		assert.Equal(t, f.token, token)
		assert.Equal(t, want, state)
	})

	t.Run("unreadable cache fails", func(t *testing.T) {
		f := newLoadFixture(t)
		f.m.incStateStore.cacheName = "missing"
		_, _, err := f.m.loadIncSnapshotState(t.Context(), f.token)
		require.ErrorContains(t, err, "unable to load incremental snapshot progress")
	})
}

// TestCommitPositionStoresProgressBeforeToken pins the write order: a token
// stored ahead of its progress could checkpoint past a request the stored
// progress does not hold yet, so a failed progress write must keep the token
// back.
func TestCommitPositionStoresProgressBeforeToken(t *testing.T) {
	f := newLoadFixture(t)
	epoch := f.m.beginTokenEpoch(nil)
	f.m.incStateStore.cacheName = "missing" // every progress write fails

	err := f.m.commitPosition(t.Context(), epoch, streamPosition{token: f.token, incState: []byte(`{"version":1}`)})
	require.ErrorContains(t, err, "storing incremental snapshot progress")
	assert.Nil(t, f.storedToken(t), "the token must not be stored ahead of its progress")

	f.m.incStateStore.cacheName = "checkpoints"
	require.NoError(t, f.m.commitPosition(t.Context(), epoch, streamPosition{token: f.token, incState: []byte(`{"version":1}`)}))
	assert.Equal(t, f.token, f.storedToken(t))
	stored, err := f.m.incStateStore.get(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, `{"version":1}`, string(stored))
}
