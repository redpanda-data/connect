// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// TestIntegrationSnowflakeStreamingPipe runs against a real account:
//   - Tier1: rows land with decimal precision intact, and a fresh stream
//     re-sending the same offset tokens inserts nothing new.
//   - Tier2 / Tier2b: stream-static-join enrichment and WHERE-clause
//     filtering, skipped when the account lacks the private-preview
//     features (see isTier2Unavailable).
func TestIntegrationSnowflakeStreamingPipe(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	dimTable := env.Table + "_DIM"
	enrichedTable := env.Table + "_ENRICHED"
	filteredTable := env.Table + "_FILTERED"

	// Fixtures are applied lazily so a fixture failure only fails the
	// subtests that depend on it.
	var tier1Once, tier2Once sync.Once
	var tier1Err error
	var tier2Available bool
	var tier2Err error

	ensureTier1 := func() error {
		tier1Once.Do(func() {
			for _, stmt := range ssv2Tier1FixtureSQL(env.Table, env.Pipe) {
				if _, err := runSSv2SQL(t, ctx, sql, env, stmt); err != nil {
					tier1Err = fmt.Errorf("tier 1 fixture statement failed: %w\nstatement: %s", err, stmt)
					return
				}
			}
		})
		return tier1Err
	}
	ensureTier2 := func() (available bool, err error) {
		tier2Once.Do(func() {
			for _, stmt := range ssv2Tier2FixtureSQL(dimTable, enrichedTable, enrichedTable) {
				if _, sqlErr := runSSv2SQL(t, ctx, sql, env, stmt); sqlErr != nil {
					if isTier2Unavailable(sqlErr) {
						return
					}
					tier2Err = fmt.Errorf("tier 2 fixture statement failed: %w\nstatement: %s", sqlErr, stmt)
					return
				}
			}
			for _, stmt := range ssv2Tier2bFixtureSQL(filteredTable, filteredTable) {
				if _, sqlErr := runSSv2SQL(t, ctx, sql, env, stmt); sqlErr != nil {
					if isTier2Unavailable(sqlErr) {
						return
					}
					tier2Err = fmt.Errorf("tier 2b fixture statement failed: %w\nstatement: %s", sqlErr, stmt)
					return
				}
			}
			tier2Available = true
		})
		return tier2Available, tier2Err
	}

	t.Run("Tier1_ValuesLandAndDedupHolds", func(t *testing.T) {
		require.NoError(t, ensureTier1())

		// A decimal that would silently round to 13 if the table/pipe used a
		// bare `number` instead of number(38,2).
		rowJSON := []string{
			`{"stake": 12.50, "bet_id": "b-1"}`,
			`{"stake": 3.00, "bet_id": "b-2"}`,
			`{"stake": 7.75, "bet_id": "b-3"}`,
		}
		buildBatch := func() []*service.Message {
			batch := make([]*service.Message, len(rowJSON))
			for i, j := range rowJSON {
				batch[i] = ssv2Row(j, 0, i+1)
			}
			return batch
		}

		// Rows land in the destination table.
		sendSSv2Rows(t, env, env.Pipe, 1, buildBatch())
		waitForSSv2RowCount(t, ctx, sql, env, env.Table, 3, 90*time.Second)

		// Decimal precision survives (12.50, not 13).
		assertSSv2Scalar(t, ctx, sql, env, "decimal precision",
			fmt.Sprintf("select stake from %s where bet_id = 'b-1'", env.Table), "12.50")

		// A fresh stream reopens the same channel (deterministic name) and
		// re-presents the same tokens; dedup against Snowflake's committed
		// state must insert nothing.
		sendSSv2Rows(t, env, env.Pipe, 1, buildBatch())
		waitForSSv2RowCount(t, ctx, sql, env, env.Table, 3, 30*time.Second)
	})

	t.Run("Tier2_EnrichmentAndNullPreservation", func(t *testing.T) {
		available, err := ensureTier2()
		require.NoError(t, err)
		if !available {
			t.Skip("ENABLE_STREAMING_STATIC_JOIN / ENABLE_SNOWPIPE_STREAMING_WHERE_CLAUSE not enabled on this account; tier 2 fixture setup reported the feature unavailable")
		}

		// b-2 is absent from the dimension table (see
		// ssv2Tier2FixtureSQL) so it exercises the "unmatched row preserved
		// with NULL" assertion instead of being dropped by the join.
		batch := []*service.Message{
			ssv2Row(`{"stake": 1.10, "bet_id": "b-1"}`, 0, 1),
			ssv2Row(`{"stake": 2.20, "bet_id": "b-2"}`, 0, 2),
			ssv2Row(`{"stake": 3.30, "bet_id": "b-3"}`, 0, 3),
		}
		sendSSv2Rows(t, env, enrichedTable, 1, batch)
		waitForSSv2RowCount(t, ctx, sql, env, enrichedTable, 3, 90*time.Second)

		assertSSv2Scalar(t, ctx, sql, env, "enrichment for b-1",
			fmt.Sprintf("select customer from %s where bet_id = 'b-1'", enrichedTable), "alice")
		assertSSv2Scalar(t, ctx, sql, env, "enrichment for b-3",
			fmt.Sprintf("select customer from %s where bet_id = 'b-3'", enrichedTable), "carol")
		assertSSv2Null(t, ctx, sql, env, "unmatched row preserved with NULL",
			fmt.Sprintf("select customer from %s where bet_id = 'b-2'", enrichedTable))
	})

	t.Run("Tier2b_WhereClauseFiltering", func(t *testing.T) {
		available, err := ensureTier2()
		require.NoError(t, err)
		if !available {
			t.Skip("ENABLE_SNOWPIPE_STREAMING_WHERE_CLAUSE not enabled on this account; tier 2 fixture setup reported the feature unavailable")
		}

		// The fixture's pipe keeps only stake >= 2.00. f-1 is below the
		// threshold and must never reach storage -- not merely be excluded
		// from a later query for it.
		batch := []*service.Message{
			ssv2Row(`{"stake": 1.50, "bet_id": "f-1"}`, 0, 1),
			ssv2Row(`{"stake": 2.00, "bet_id": "f-2"}`, 0, 2),
			ssv2Row(`{"stake": 5.00, "bet_id": "f-3"}`, 0, 3),
		}
		sendSSv2Rows(t, env, filteredTable, 1, batch)

		// Exactly the two rows meeting the threshold landed.
		waitForSSv2RowCount(t, ctx, sql, env, filteredTable, 2, 90*time.Second)
		assertSSv2Scalar(t, ctx, sql, env, "below-threshold row never reached storage",
			fmt.Sprintf("select count(*) from %s where bet_id = 'f-1'", filteredTable), "0")
		assertSSv2Scalar(t, ctx, sql, env, "at-threshold row (>=) is kept",
			fmt.Sprintf("select stake from %s where bet_id = 'f-2'", filteredTable), "2.00")
		assertSSv2Scalar(t, ctx, sql, env, "above-threshold row is kept",
			fmt.Sprintf("select stake from %s where bet_id = 'f-3'", filteredTable), "5.00")
	})
}
