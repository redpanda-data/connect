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
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestPrevTimestamp(t *testing.T) {
	assert.Equal(t, bson.Timestamp{T: 10, I: 4}, prevTimestamp(bson.Timestamp{T: 10, I: 5}))
	assert.Equal(t, bson.Timestamp{T: 9, I: math.MaxUint32}, prevTimestamp(bson.Timestamp{T: 10, I: 0}))
	assert.Equal(t, bson.Timestamp{}, prevTimestamp(bson.Timestamp{}))
}

func TestClusterTimeWatermark(t *testing.T) {
	wm := clusterTimeWatermark{T: bson.Timestamp{T: 100, I: 3}}

	// A position is "streamed through, inclusive", so the window bound is
	// reached once the stream is through T itself.
	assert.False(t, wm.ClosesAt(bson.Timestamp{T: 100, I: 2}))
	assert.True(t, wm.ClosesAt(bson.Timestamp{T: 100, I: 3}))
	assert.True(t, wm.ClosesAt(bson.Timestamp{T: 101, I: 0}))
	assert.False(t, wm.OpensAt(bson.Timestamp{T: 99, I: 9}))
	assert.True(t, wm.OpensAt(bson.Timestamp{T: 100, I: 3}))

	assert.False(t, wm.Quiesced())
	assert.True(t, clusterTimeWatermark{T: wm.T, quiesced: true}.Quiesced())

	// Events of one transaction share a clusterTime, so an event at T only
	// proves the stream is through the tick before T: the window must not
	// close until the stream moves past the transaction.
	evt := bson.Timestamp{T: 100, I: 3}
	assert.False(t, wm.ClosesAt(prevTimestamp(evt)))

	// The zero value must not panic and is closed by any position.
	var zero clusterTimeWatermark
	assert.True(t, zero.ClosesAt(bson.Timestamp{}))
}

func TestNewClusterTimeWatermarkQuiesced(t *testing.T) {
	at := bson.Timestamp{T: 50, I: 1}
	assert.True(t, newClusterTimeWatermark(at, bson.Timestamp{T: 50, I: 1}).Quiesced())
	assert.True(t, newClusterTimeWatermark(at, bson.Timestamp{T: 51}).Quiesced())
	assert.False(t, newClusterTimeWatermark(at, bson.Timestamp{T: 50}).Quiesced())
}

func hexToken(t *testing.T, ts bson.Timestamp, suffix string) bson.Raw {
	t.Helper()
	data := "82" + hex.EncodeToString([]byte{
		byte(ts.T >> 24), byte(ts.T >> 16), byte(ts.T >> 8), byte(ts.T),
		byte(ts.I >> 24), byte(ts.I >> 16), byte(ts.I >> 8), byte(ts.I),
	}) + suffix
	raw, err := bson.Marshal(bson.D{{Key: "_data", Value: data}})
	require.NoError(t, err)
	return raw
}

func TestResumeTokenTimestamp(t *testing.T) {
	ts := bson.Timestamp{T: 1760000000, I: 7}

	t.Run("hex string", func(t *testing.T) {
		// A real post-batch token has more KeyString after the timestamp;
		// upper-case hex is what servers emit.
		got, err := resumeTokenTimestamp(hexToken(t, ts, "2B0429296E1404"))
		require.NoError(t, err)
		assert.Equal(t, ts, got)
	})

	t.Run("binary", func(t *testing.T) {
		data := []byte{0x82, 0x68, 0xE7, 0x78, 0x00, 0x00, 0x00, 0x00, 0x07, 0x2B}
		raw, err := bson.Marshal(bson.D{{Key: "_data", Value: bson.Binary{Data: data}}})
		require.NoError(t, err)
		got, err := resumeTokenTimestamp(raw)
		require.NoError(t, err)
		assert.Equal(t, ts, got)
	})

	for name, token := range map[string]any{
		"wrong ctype": bson.D{{Key: "_data", Value: "8000000001"}},
		"short":       bson.D{{Key: "_data", Value: "8200"}},
		"not hex":     bson.D{{Key: "_data", Value: "82zz"}},
		"no data":     bson.D{{Key: "other", Value: 1}},
		"wrong type":  bson.D{{Key: "_data", Value: int32(1)}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := bson.Marshal(token)
			require.NoError(t, err)
			_, err = resumeTokenTimestamp(raw)
			assert.Error(t, err)
		})
	}

	t.Run("nil", func(t *testing.T) {
		_, err := resumeTokenTimestamp(nil)
		assert.Error(t, err)
	})
}

func rawID(t *testing.T, v any) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: v}})
	require.NoError(t, err)
	return bson.Raw(raw).Lookup("_id")
}

func TestDocumentKeyRoundTrip(t *testing.T) {
	oid := bson.NewObjectID()
	dec, err := bson.ParseDecimal128("12345.6789")
	require.NoError(t, err)
	for name, v := range map[string]any{
		"objectid": oid,
		"string":   "abc",
		"int32":    int32(7),
		"int64":    int64(1) << 60,
		"double":   2.5,
		"decimal":  dec,
		"uuid":     bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")},
		"date":     bson.NewDateTimeFromTime(time.UnixMilli(1760000000123)),
		"document": bson.D{{Key: "b", Value: 1}, {Key: "a", Value: "x"}},
	} {
		t.Run(name, func(t *testing.T) {
			key, err := documentKey(rawID(t, v))
			require.NoError(t, err)

			// The key must survive the coordinator's State JSON round trip,
			// which is how it reaches the next run's FetchChunk bounds.
			state := incrementalsnapshot.State{Version: incrementalsnapshot.CurrentStateVersion, LastSentPK: key}
			b, err := json.Marshal(state)
			require.NoError(t, err)
			var resumed incrementalsnapshot.State
			require.NoError(t, json.Unmarshal(b, &resumed))
			assert.Equal(t, key, resumed.LastSentPK)

			back, err := documentKeyValue(resumed.LastSentPK)
			require.NoError(t, err)
			assert.True(t, rawID(t, v).Equal(back), "got %s", back)
		})
	}
}

func TestDocumentKeyDistinguishesTypes(t *testing.T) {
	str, err := documentKey(rawID(t, "1"))
	require.NoError(t, err)
	num, err := documentKey(rawID(t, int32(1)))
	require.NoError(t, err)
	assert.NotEqual(t, str, num)
}

func TestDocumentKeyValueRejectsBadKeys(t *testing.T) {
	for name, pk := range map[string]incrementalsnapshot.PrimaryKey{
		"empty":     {},
		"two parts": {"a", "b"},
		"not text":  {int64(1)},
		"bad json":  {"{"},
		"no _id":    {`{"x":1}`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := documentKeyValue(pk)
			assert.Error(t, err)
		})
	}
}

// fakeChunkReader serves int32 _ids with the server's index-bound semantics:
// min inclusive, max exclusive.
type fakeChunkReader struct {
	t         *testing.T
	ids       []int32
	rangeCall int
	byIDCall  int
}

func fakeID(v bson.RawValue) (int32, bool) {
	if v.Type == bson.Type(0) {
		return math.MinInt32, false
	}
	return v.Int32(), true
}

func (f *fakeChunkReader) doc(id int32) chunkDoc {
	return chunkDoc{id: rawID(f.t, id), doc: bson.M{"_id": id}}
}

func (f *fakeChunkReader) findRange(_ context.Context, _ string, lower, upper bson.RawValue, limit int) ([]chunkDoc, error) {
	f.rangeCall++
	lo, _ := fakeID(lower)
	hi, _ := fakeID(upper)
	if lo >= hi {
		// As the server does: error 51175, min must come before max.
		return nil, errors.New("min() does not come before max()")
	}
	var out []chunkDoc
	for _, id := range f.ids {
		if id >= lo && id < hi && len(out) < limit {
			out = append(out, f.doc(id))
		}
	}
	return out, nil
}

func (f *fakeChunkReader) findByID(_ context.Context, _ string, id bson.RawValue) (*chunkDoc, error) {
	f.byIDCall++
	want, _ := fakeID(id)
	for _, have := range f.ids {
		if have == want {
			d := f.doc(have)
			return &d, nil
		}
	}
	return nil, nil
}

func key(t *testing.T, id int32) incrementalsnapshot.PrimaryKey {
	t.Helper()
	pk, err := documentKey(rawID(t, id))
	require.NoError(t, err)
	return pk
}

func rowIDs(rows []incrementalsnapshot.Row) []int32 {
	var ids []int32
	for _, r := range rows {
		ids = append(ids, r.Data["_id"].(int32))
	}
	return ids
}

func TestFetchChunkWalksWholeRangeIncludingUpper(t *testing.T) {
	r := &fakeChunkReader{t: t, ids: []int32{1, 2, 3, 4, 5, 6, 7}}
	table := incrementalsnapshot.TableID{Schema: "db", Table: "c"}
	upper := key(t, 7)

	var got []int32
	var lower incrementalsnapshot.PrimaryKey
	for range 10 {
		rows, err := fetchChunk(t.Context(), r, table, lower, upper, 3)
		require.NoError(t, err)
		got = append(got, rowIDs(rows)...)
		if len(rows) < 3 {
			break
		}
		lower = rows[len(rows)-1].PK
	}
	assert.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7}, got)
}

func TestFetchChunkExcludesLowerBound(t *testing.T) {
	r := &fakeChunkReader{t: t, ids: []int32{1, 2, 3, 4}}
	table := incrementalsnapshot.TableID{Schema: "db", Table: "c"}

	rows, err := fetchChunk(t.Context(), r, table, key(t, 2), key(t, 4), 10)
	require.NoError(t, err)
	assert.Equal(t, []int32{3, 4}, rowIDs(rows))
	assert.Equal(t, table, rows[0].Table)
	assert.Equal(t, key(t, 3), rows[0].PK)
}

func TestFetchChunkLeavesUpperForNextChunkWhenFull(t *testing.T) {
	r := &fakeChunkReader{t: t, ids: []int32{1, 2, 3}}
	table := incrementalsnapshot.TableID{Schema: "db", Table: "c"}

	rows, err := fetchChunk(t.Context(), r, table, nil, key(t, 3), 2)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2}, rowIDs(rows))
	assert.Zero(t, r.byIDCall, "a full chunk must not look up the upper key")
}

func TestFetchChunkAfterUpperIsEmpty(t *testing.T) {
	r := &fakeChunkReader{t: t, ids: []int32{1, 2, 3}}
	table := incrementalsnapshot.TableID{Schema: "db", Table: "c"}

	// What a resume asks for when the checkpoint was taken right after a
	// collection's last chunk: the server rejects that range outright.
	rows, err := fetchChunk(t.Context(), r, table, key(t, 3), key(t, 3), 2)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.Zero(t, r.rangeCall)
	assert.Zero(t, r.byIDCall)
}

func TestFetchChunkUpperDeletedMeanwhile(t *testing.T) {
	// The max key was captured, then that document was deleted: the chunk
	// just ends without it.
	r := &fakeChunkReader{t: t, ids: []int32{1, 2}}
	table := incrementalsnapshot.TableID{Schema: "db", Table: "c"}

	rows, err := fetchChunk(t.Context(), r, table, nil, key(t, 9), 5)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2}, rowIDs(rows))
}

func TestPruneResumeDropsUnconfiguredCollections(t *testing.T) {
	orders := incrementalsnapshot.TableID{Schema: "db", Table: "orders"}
	legacy := incrementalsnapshot.TableID{Schema: "db", Table: "legacy"}
	users := incrementalsnapshot.TableID{Schema: "db", Table: "users"}
	logger := service.MockResources().Logger()

	t.Run("current and queued", func(t *testing.T) {
		stored := &incrementalsnapshot.State{
			Version:         incrementalsnapshot.CurrentStateVersion,
			CurrentTable:    &legacy,
			LastSentPK:      incrementalsnapshot.PrimaryKey{"a"},
			MaxPK:           incrementalsnapshot.PrimaryKey{"z"},
			RemainingTables: []incrementalsnapshot.TableID{users, legacy, orders},
			Tables:          []incrementalsnapshot.TableID{legacy, users, orders},
		}
		got := pruneResume(stored, []incrementalsnapshot.TableID{orders, users}, logger)
		assert.Nil(t, got.CurrentTable)
		assert.Nil(t, got.LastSentPK)
		assert.Nil(t, got.MaxPK)
		assert.Equal(t, []incrementalsnapshot.TableID{users, orders}, got.RemainingTables)
		assert.Equal(t, &legacy, stored.CurrentTable, "the caller's state must not change")
		assert.Len(t, stored.RemainingTables, 3)
	})

	t.Run("all configured", func(t *testing.T) {
		stored := &incrementalsnapshot.State{
			Version:         incrementalsnapshot.CurrentStateVersion,
			CurrentTable:    &orders,
			LastSentPK:      incrementalsnapshot.PrimaryKey{"a"},
			RemainingTables: []incrementalsnapshot.TableID{users},
		}
		assert.Equal(t, stored, pruneResume(stored, []incrementalsnapshot.TableID{orders, users}, logger))
	})

	t.Run("nil", func(t *testing.T) {
		assert.Nil(t, pruneResume(nil, nil, logger))
	})
}

func TestIncStateCacheVersionMismatchIsNotCorrupt(t *testing.T) {
	res := service.MockResources(service.MockResourcesOptAddCache("c"))
	cache := &incStateCache{resources: res, cacheName: "c", cacheKey: "k"}

	require.NoError(t, cache.Store(t.Context(), []byte(`{"version":999}`)))
	_, err := cache.Load(t.Context())
	require.ErrorIs(t, err, incrementalsnapshot.ErrUnsupportedStateVersion)
	assert.NotErrorIs(t, err, errCorruptCheckpoint, "a newer build's progress must not be cleared")

	require.NoError(t, cache.Store(t.Context(), []byte(`{`)))
	_, err = cache.Load(t.Context())
	require.ErrorIs(t, err, errCorruptCheckpoint)

	require.NoError(t, cache.Delete(t.Context()))
	state, err := cache.Load(t.Context())
	require.NoError(t, err)
	assert.Nil(t, state)
}
