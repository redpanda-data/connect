// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

// Concurrent, mutually unordered batches on one channel cannot be made safe
// (a channel has one committed token), so the requirement is that the
// failure is loud: checkSubmissionOrder must fail the batch rather than let
// the dedup filter silently drop rows as duplicates.

// provisionSSv2SeqObjects creates a table with a single seq number(38,0)
// column and a Snowpipe-Streaming-backed pipe copying into it, mirroring
// ssv2Tier1FixtureSQL's shape (explicit precision/scale on the numeric cast).
func provisionSSv2SeqObjects(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, table, pipe string) {
	t.Helper()
	mustRunSSv2SQL(t, ctx, sql, env, fmt.Sprintf("create or replace table %s (seq number(38,0))", table))
	mustRunSSv2SQL(t, ctx, sql, env, fmt.Sprintf(
		"create or replace pipe %s as copy into %s from (select $1:seq::number(38,0) from table(data_source(type => 'STREAMING')))",
		pipe, table))
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := runSSv2SQL(t, cctx, sql, env, fmt.Sprintf("drop pipe if exists %s", pipe)); err != nil {
			t.Logf("cleanup: drop pipe %s: %v", pipe, err)
		}
		if _, err := runSSv2SQL(t, cctx, sql, env, fmt.Sprintf("drop table if exists %s", table)); err != nil {
			t.Logf("cleanup: drop table %s: %v", table, err)
		}
	})
}

// analyzeSSv2Gaps reports which of {1..n} are absent from got and whether
// the gap is one contiguous range from 1 (the silent-dedup signature).
func analyzeSSv2Gaps(got []int, n int) (missing []int, lowEndContiguous bool) {
	present := make(map[int]bool, len(got))
	for _, v := range got {
		present[v] = true
	}
	for i := 1; i <= n; i++ {
		if !present[i] {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return missing, false
	}
	sort.Ints(missing)
	lowEndContiguous = missing[0] == 1
	for i := 1; i < len(missing) && lowEndContiguous; i++ {
		if missing[i] != missing[i-1]+1 {
			lowEndContiguous = false
		}
	}
	return missing, lowEndContiguous
}

// TestIntegrationSnowflakeStreamingPipeMaxInFlightReordering sends n
// single-row batches concurrently to one channel at max_in_flight=4. The
// race is not controlled, so the assertion is: either every row lands, or
// the failure is checkSubmissionOrder's error. Silent partial loss is the
// one outcome that must never occur.
func TestIntegrationSnowflakeStreamingPipeMaxInFlightReordering(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	const n = 30
	table := env.Table + "_MAXINFLIGHT_SEQ"
	pipe := env.Pipe + "_MAXINFLIGHT"
	provisionSSv2SeqObjects(t, ctx, sql, env, table, pipe)

	produce, stream := buildSSv2Stream(t, env, pipe, 4, defaultSSv2OutputOpts())
	runSSv2StreamInBackground(t, stream)

	var eg errgroup.Group
	for i := 1; i <= n; i++ {
		i := i
		eg.Go(func() error {
			row := ssv2Row(fmt.Sprintf(`{"seq": %d}`, i), 0, i)
			return produce(t.Context(), service.MessageBatch{row})
		})
	}

	if err := eg.Wait(); err != nil {
		require.Contains(t, err.Error(), "out of order",
			"a WriteBatch call failed, but not with checkSubmissionOrder's ordering-violation guard -- got: %v", err)
		t.Logf("reproduced the documented hazard (concurrent, mutually-unordered writers on one channel) -- "+
			"checkSubmissionOrder caught it loudly instead of silently dropping rows: %v", err)
		return
	}

	// No violation this run: then every row must have landed.
	waitForSSv2RowCount(t, ctx, sql, env, table, n, 3*time.Minute)

	resp := mustRunSSv2SQL(t, ctx, sql, env, fmt.Sprintf("select seq from %s order by seq", table))
	got := make([]int, 0, len(resp.Data))
	for _, row := range resp.Data {
		v, err := strconv.Atoi(row[0])
		require.NoError(t, err)
		got = append(got, v)
	}

	missing, lowEndContiguous := analyzeSSv2Gaps(got, n)
	require.Empty(t, missing, "rows silently dropped despite every WriteBatch call reporting success (low_end_contiguous=%v, landed=%d/%d)", lowEndContiguous, len(got), n)
}
