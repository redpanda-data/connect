// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
)

func TestSpannerPartitionBatcherMaybeFlushWith(t *testing.T) {
	s, err := service.NewStreamBuilder().Build()
	require.NoError(t, err)
	batcher, err := service.BatchPolicy{
		Count: 2,
	}.NewBatcher(s.Resources())
	require.NoError(t, err)

	pb := &spannerPartitionBatcher{
		batcher: batcher,
	}

	mod := &changestreams.Mod{
		Keys: spanner.NullJSON{
			Value: "foo",
		},
	}

	tsn := func(i int) time.Time {
		return time.Unix(int64(i), 0).UTC()
	}

	{
		// Given a DataChangeRecord with a single mod
		dcr := &changestreams.DataChangeRecord{
			CommitTimestamp: tsn(1),
			TableName:       "test_table",
			Mods: []*changestreams.Mod{
				mod,
			},
			ModType: "INSERT",
		}

		// When MaybeFlushWith is called
		iter := pb.MaybeFlushWith(dcr)

		var count int
		for range iter.Iter(t.Context()) {
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 0, count)
	}

	{
		// Given a DataChangeRecord with 5 mods
		dcr := &changestreams.DataChangeRecord{
			CommitTimestamp: tsn(2),
			TableName:       "test_table",
			Mods: []*changestreams.Mod{
				mod,
				mod,
				mod,
				mod,
				mod,
			},
		}

		// When MaybeFlushWith is called
		iter := pb.MaybeFlushWith(dcr)
		var got []time.Time
		for mb, ts := range iter.Iter(t.Context()) {
			assert.Len(t, mb, 2)
			got = append(got, ts)
		}
		require.NoError(t, iter.Err())

		// Then 3 batches are returned, each with 2 mods
		want := []time.Time{
			tsn(1),
			{},
			tsn(2),
		}
		assert.Equal(t, want, got)

		// When Flush is called
		mb, ts, err := pb.Flush(t.Context())
		require.NoError(t, err)

		// Then no batch is returned
		require.Nil(t, mb)
		require.Zero(t, ts)
	}
}
