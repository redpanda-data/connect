// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cdc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/connect/v4/internal/replication"
)

func marshalDoc(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	require.NoError(t, err)
	return raw
}

func TestDecodeMongoSignal(t *testing.T) {
	oid := bson.NewObjectID()

	t.Run("document data", func(t *testing.T) {
		sig, data, err := decodeMongoSignal(marshalDoc(t, bson.D{
			{Key: "_id", Value: oid},
			{Key: "type", Value: replication.SnapshotSignalType},
			{Key: "data", Value: bson.D{{Key: "collections", Value: bson.A{"a", "b"}}}},
		}))
		require.NoError(t, err)
		assert.Equal(t, oid.Hex(), sig.ID)
		assert.Equal(t, replication.SnapshotSignalType, sig.Type())
		assert.JSONEq(t, `{"collections":["a","b"]}`, string(data))
	})

	t.Run("string data", func(t *testing.T) {
		sig, _, err := decodeMongoSignal(marshalDoc(t, bson.D{
			{Key: "_id", Value: "sig-1"},
			{Key: "type", Value: replication.LogSignalType},
			{Key: "data", Value: `{"message":"hello"}`},
		}))
		require.NoError(t, err)
		assert.Equal(t, "sig-1", sig.ID)
		assert.Equal(t, "hello", sig.Message)
	})

	t.Run("unknown type passes through", func(t *testing.T) {
		sig, _, err := decodeMongoSignal(marshalDoc(t, bson.D{
			{Key: "_id", Value: int32(4)},
			{Key: "type", Value: "pause-snapshot"},
		}))
		require.NoError(t, err)
		assert.Equal(t, "4", sig.ID)
		assert.False(t, replication.IsKnownSignalType(sig.Type()))
	})

	for name, doc := range map[string]bson.D{
		"no type":       {{Key: "_id", Value: 1}},
		"non-text type": {{Key: "_id", Value: 1}, {Key: "type", Value: 1}},
		"bad data":      {{Key: "_id", Value: 1}, {Key: "type", Value: "log"}, {Key: "data", Value: int32(1)}},
		"bad log data":  {{Key: "_id", Value: 1}, {Key: "type", Value: "log"}, {Key: "data", Value: "{"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := decodeMongoSignal(marshalDoc(t, doc))
			require.ErrorIs(t, err, replication.ErrSignalRejected)
		})
	}
}

func TestParseSnapshotCollections(t *testing.T) {
	got, err := parseSnapshotCollections([]byte(`{"collections":["a","b"]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, got)

	got, err = parseSnapshotCollections([]byte(`{"tables":["c"]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, got)

	got, err = parseSnapshotCollections([]byte(`{"collections":["a"],"tables":["a","c"]}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "c"}, got)

	for _, bad := range []string{`{}`, `{"collections":[]}`, `[`, `{"collections":"a"}`} {
		_, err := parseSnapshotCollections([]byte(bad))
		require.ErrorIs(t, err, replication.ErrSignalRejected, bad)
	}
}
