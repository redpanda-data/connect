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

// ---------------------------------------------------------------------------
// max_in_flight batch-reordering hazard.
//
// This is ported from the SSv2 demo repo's maxinflight_reorder_it_test.go,
// which drove this scenario through a real Kafka broker and an out-of-process
// connector binary. That repo's own commentary concluded the hazard was
// "structurally unreachable" there specifically because
// internal/impl/kafka/franz_reader_ordered.go's partitionCache never hands a
// partition's next batch to the pipeline until the previous one's Ack/Nack
// has fully resolved. That's still true here, and worth being precise about:
// a genuinely per-partition-ordered reader (Kafka/Redpanda's own included)
// never has more than one batch in flight for a single partition, no matter
// what max_in_flight is set to -- max_in_flight only ever creates concurrency
// *across* partitions for that kind of reader, never within one. So the
// scenario this test drives (many single-row batches, all resolving to one
// channel, dispatched with zero ordering relationship to each other) is not
// something any real ordered Kafka/Redpanda reader writing to a
// per-partition channel_name would ever produce.
//
// It's still worth testing, for two reasons. First, offset_token is a
// general-purpose field, not Kafka-specific -- an input whose delivery isn't
// ack-gated per token (dispatching eagerly, trusting max_in_flight alone to
// bound concurrency) can genuinely produce concurrent, same-channel batches
// from what's still logically one ordered source. Second, and the more
// commonly reachable case: this connector's own default channel_name (see
// defaultChannelName) is a single constant channel shared across every
// partition. Feed it a multi-partition Kafka input with max_in_flight > 1
// and left at that default, and the reader's own per-partition ordering
// guarantee doesn't help at all -- partition 0's and partition 1's batches
// are dispatched concurrently (correctly, by that reader's own contract) and
// both land on the *same* channel, with no ordering relationship between
// them, since their tokens come from mutually independent sequences in the
// first place. That misconfiguration is already called out in channel_name's
// own Description ("the always-safe fallback is this field's own default
// ... with max_in_flight: 1 set explicitly"), and this test's real value is
// checking what happens when someone hits it anyway: not "does it work" (it
// structurally can't -- two independent, unordered sources sharing one
// channel have no common token order to preserve), but "does it fail loudly
// or corrupt data silently".
//
// Before checkSubmissionOrder existed (output_snowflake_streaming_pipe.go),
// it did the latter: whichever concurrent submission's goroutine happened to
// reach the channel first would commit, silently advancing
// channel.LatestOffsetToken past every other in-flight batch's own tokens --
// each of which then looked, to preprocessForExactlyOncePipe, exactly like an
// ordinary already-landed duplicate, and was dropped with no error, warning,
// or metric. checkSubmissionOrder closes that specific gap: it can't make
// concurrent unordered writers to one channel safe (nothing can -- that's an
// inherent limit of a single monotonic per-channel token, not a bug), but it
// guarantees the failure mode is now a loud, attributable error instead of a
// gap nobody notices. This test's assertion reflects that: either every row
// lands (no violation was actually triggered this run -- the scenario is
// racy, not deterministic), or WriteBatch fails with checkSubmissionOrder's
// error. What must never happen, and is the one thing genuinely asserted
// here, is silent partial data loss.
// ---------------------------------------------------------------------------

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

// analyzeSSv2Gaps reports which of the expected {1..n} seq values are absent
// from got, and whether those missing values form a single contiguous range
// starting at 1 -- the signature the reordering+silent-dedup hazard predicts
// (see this file's header comment): before checkSubmissionOrder existed, if
// a higher-offset batch's commit advanced channel.LatestOffsetToken before a
// lower-offset batch's own AppendRows call was compared against it, every
// row in that lower-offset batch was dropped as "already landed". Kept for
// the failure message's diagnostic value on the no-error path, where every
// row landing with no gaps is a real assertion, not just a log line.
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
// single-row batches, all on the same channel (partition 0), concurrently
// through the output at max_in_flight=4, with no ordering relationship
// enforced between the n goroutines -- deliberately reconstructing the
// documented-unsafe "one shared channel, max_in_flight > 1" configuration
// (see this file's header comment). n=30 and a single run (rather than the
// source repo's 5 runs at max_in_flight=4 plus 5 at max_in_flight=1 as a
// control) trims this from a statistical investigation to a regression
// test: the scenario is preserved, repeated-run confidence-building is not.
//
// The scenario is genuinely racy -- whether checkSubmissionOrder's guard
// actually triggers this run depends on the order goroutines happen to
// reach the channel, which this test does nothing to control. So the
// assertion is deliberately not "every row lands": it's "either every row
// lands, or the failure is checkSubmissionOrder's own loud error" -- ruling
// out the one outcome that must never happen, silent partial data loss,
// without asserting on an outcome (which of the two safe results occurs)
// that isn't actually deterministic.
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

	// No violation triggered this run: the 30 sends happened to reach the
	// channel in an order preprocessForExactlyOncePipe's dedup filter
	// tolerated. Every row landing with no gaps is a real assertion in that
	// case, not a race-dependent maybe.
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
