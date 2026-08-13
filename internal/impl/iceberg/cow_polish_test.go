// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestCOWAmplificationWarning pins item 3.4: the one-time startup guidance about
// copy-on-write's write-amplification characteristic must fire for a mutating
// copy-on-write config and stay silent otherwise (merge-on-read, or an
// append-only insert config regardless of merge_strategy). It is deliberately
// separate from the max_in_flight ordering warning, which is about correctness.
func TestCOWAmplificationWarning(t *testing.T) {
	cases := []struct {
		name     string
		cfg      RowOpConfig
		wantWarn bool
	}{
		{
			name:     "cow static upsert warns",
			cfg:      RowOpConfig{Operation: mustInterp(t, "upsert"), IdentifierFields: []string{"id"}, MergeStrategy: mergeStrategyCOW},
			wantWarn: true,
		},
		{
			name:     "cow static delete warns",
			cfg:      RowOpConfig{Operation: mustInterp(t, "delete"), IdentifierFields: []string{"id"}, MergeStrategy: mergeStrategyCOW},
			wantWarn: true,
		},
		{
			name:     "cow dynamic operation warns",
			cfg:      RowOpConfig{Operation: mustInterp(t, `${! metadata("op") }`), IdentifierFields: []string{"id"}, MergeStrategy: mergeStrategyCOW},
			wantWarn: true,
		},
		{
			name:     "cow static insert stays silent",
			cfg:      RowOpConfig{Operation: mustInterp(t, "insert"), MergeStrategy: mergeStrategyCOW},
			wantWarn: false,
		},
		{
			name:     "merge-on-read mutating stays silent",
			cfg:      RowOpConfig{Operation: mustInterp(t, "upsert"), IdentifierFields: []string{"id"}, MergeStrategy: mergeStrategyMOR},
			wantWarn: false,
		},
		{
			name:     "unset operation stays silent",
			cfg:      RowOpConfig{MergeStrategy: mergeStrategyCOW},
			wantWarn: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, ok := tc.cfg.cowAmplificationWarning()
			assert.Equal(t, tc.wantWarn, ok)
			if tc.wantWarn {
				assert.Contains(t, msg, "copy-on-write")
				assert.Contains(t, msg, "sort the table by the identifier key", "the message must name the sort mitigation")
				assert.Contains(t, msg, "large batches", "the message must name the batching mitigation")
			} else {
				assert.Empty(t, msg)
			}
		})
	}
}

// TestSplitByOperationCOWCountsFeedMetrics pins item 3.2 at the reachable seam:
// the per-operation counts that drive iceberg_row_operations_total{operation=...}
// are computed by splitByOperation. Because the emitted counter *values* are not
// readable from a unit test (see the metrics note in output_iceberg.go), this
// guards the numbers that would be handed to incrInserted/incrUpserted/
// incrDeleted instead — including the last-writer-wins per-key collapse, so a
// counter can never over-count a repeatedly-mutated key.
func TestSplitByOperationCOWCountsFeedMetrics(t *testing.T) {
	tbl, _ := newTestTable(t) // schema: id int64
	w := cowWriter(t, tbl, "id")

	t.Run("distinct ops counted after collapse", func(t *testing.T) {
		inserts, deletes, counts, err := w.splitByOperation(service.MessageBatch{
			cowMsg(t, "insert", map[string]any{"id": 10}),
			cowMsg(t, "insert", map[string]any{"id": 11}),
			cowMsg(t, "upsert", map[string]any{"id": 2}),
			cowMsg(t, "delete", map[string]any{"id": 3}),
			cowMsg(t, "upsert", map[string]any{"id": 4}),
		})
		require.NoError(t, err)
		// inserted counts insert-op rows; upserted/deleted count keyed ops after
		// per-key collapse. These are exactly the arguments passed to the incr*
		// methods in cow.go's writeCOW.
		assert.EqualValues(t, 2, counts.inserted, "two insert-op rows")
		assert.EqualValues(t, 2, counts.upserted, "two distinct upsert keys")
		assert.EqualValues(t, 1, counts.deleted, "one delete key")
		// Sanity on the batch split the same counts describe: inserts carry the
		// insert rows plus the upsert rows to (re)write; deletes carry one message
		// per keyed op.
		assert.Len(t, inserts, 4, "2 inserts + 2 upserts to write")
		assert.Len(t, deletes, 3, "2 upsert keys + 1 delete key to remove")
	})

	t.Run("repeated key collapses to one op so the counter cannot over-count", func(t *testing.T) {
		// Three mutations of id=2 in one batch collapse to the last (a delete), so
		// the metrics reflect one committed operation for the key, not three.
		_, _, counts, err := w.splitByOperation(service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": 2}),
			cowMsg(t, "upsert", map[string]any{"id": 2}),
			cowMsg(t, "delete", map[string]any{"id": 2}),
		})
		require.NoError(t, err)
		assert.EqualValues(t, 0, counts.inserted)
		assert.EqualValues(t, 0, counts.upserted, "the trailing delete wins, so no upsert is counted")
		assert.EqualValues(t, 1, counts.deleted, "the key collapses to a single delete")
	})
}
