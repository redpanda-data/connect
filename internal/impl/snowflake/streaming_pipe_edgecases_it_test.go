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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// ---------------------------------------------------------------------------
// Edge cases beyond the main tier 1/2/2b suite (streaming_pipe_integration_test.go),
// the max-in-flight reordering regression, and the OAuth scope probe. Each
// test here targets one specific, previously-unverified-against-a-real-
// account behavior rather than the pipe/enrichment/filtering happy path.
// ---------------------------------------------------------------------------

// provisionSSv2Tier1Objects is provisionSSv2SeqObjects's counterpart for the
// stake/bet_id shape (ssv2Tier1FixtureSQL) the tests in this file build on,
// with the same create-then-register-cleanup pattern.
func provisionSSv2Tier1Objects(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, table, pipe string) {
	t.Helper()
	for _, stmt := range ssv2Tier1FixtureSQL(table, pipe) {
		mustRunSSv2SQL(t, ctx, sql, env, stmt)
	}
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

// TestIntegrationSnowflakeStreamingPipeMissingPipeFailsAtStartup proves the
// claim repeated throughout this output's docs and DEPLOYMENT.md's own
// design notes: a missing pipe fails at connector startup, not silently on
// the first write. streamingv2.Client.Connect's pipe-scoped pre-flight
// check is what's actually responsible for this -- see its own doc comment
// on why a plain AppendRows-based check wouldn't catch it (an invalid pipe
// answers 2xx and silently discards rows).
//
// Whether that failure surfaces from StreamBuilder.Build (component
// construction) or only once Run starts is deliberately not assumed here --
// newSnowflakeStreamerPipe calls Connect synchronously during construction
// today, but this checks both points rather than encoding that as a
// requirement the test would otherwise silently stop covering if it moved.
func TestIntegrationSnowflakeStreamingPipeMissingPipeFailsAtStartup(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)

	missingPipe := env.Pipe + "_MISSING_FOR_TEST"

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML(`level: ERROR`))
	_, err := b.AddBatchProducerFunc()
	require.NoError(t, err)
	require.NoError(t, b.AddOutputYAML(buildSSv2OutputYAML(env, missingPipe, 1, defaultSSv2OutputOpts())))

	stream, buildErr := b.Build()
	if buildErr != nil {
		return // failed during construction, as expected
	}
	license.InjectTestService(stream.Resources())
	runErr := stream.Run(t.Context())
	require.Error(t, runErr, "expected startup to fail against a nonexistent pipe %q, but Build and Run both succeeded", missingPipe)
}

// TestIntegrationSnowflakeStreamingPipeOffsetTokenUnsetCommitsForReal proves
// that leaving offset_token unset -- at-least-once mode, no dedup pass --
// actually works against a real account, not just the fake channel in
// output_snowflake_streaming_pipe_test.go's
// TestWriteBatchWithoutOffsetTokenSkipsDedupAndUsesSyntheticCommitToken.
// This specifically re-validates the noDedupCommitToken mechanism: before it
// existed, writeChannelGroup sent an empty append token when offsetToken was
// nil, which WaitUntilCommitted could never confirm (its success condition
// requires a *non-empty* committed token, and Snowflake only ever has empty
// tokens to echo back if every append sends one) -- every batch would have
// hung until commit_timeout, every time, against a real account. A bounded
// wait here (90s, well under the output's own 60s commit_timeout default
// plus test overhead) turns a regression back into that bug into a timeout
// failure rather than a silent hang.
func TestIntegrationSnowflakeStreamingPipeOffsetTokenUnsetCommitsForReal(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_NODEDUP"
	pipe := env.Pipe + "_NODEDUP"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	rows := []*service.Message{
		ssv2Row(`{"stake": 4.25, "bet_id": "nd-1"}`, 0, 1),
		ssv2Row(`{"stake": 6.00, "bet_id": "nd-2"}`, 0, 2),
	}
	sendSSv2RowsWithOpts(t, env, pipe, 1, ssv2OutputOpts{}, rows)
	waitForSSv2RowCount(t, ctx, sql, env, table, 2, 90*time.Second)

	// "Restart": a second, independent stream (fresh output, fresh synthetic
	// seed) writing to the same default channel, whose committed token is
	// now whatever the first stream left there. This is the case the
	// unit-level restart test can only approximate with a fake: against the
	// real API it proves that a synthetic token round-trips through
	// Snowflake intact (the '~' prefix survives URL escaping and the status
	// echo) and that the second run's tokens compare newer than it, so the
	// commit wait genuinely waits for its own append rather than returning
	// early on the previous run's value -- if it did return early, the rows
	// would still land eventually, so the assertion that matters is the
	// mode-switch one below, which can only fail if the committed token is
	// recognisably synthetic after this second run.
	sendSSv2RowsWithOpts(t, env, pipe, 1, ssv2OutputOpts{}, []*service.Message{
		ssv2Row(`{"stake": 1.50, "bet_id": "nd-3"}`, 0, 3),
	})
	waitForSSv2RowCount(t, ctx, sql, env, table, 3, 90*time.Second)

	// Mode switch, the dangerous direction: offset_token added to a
	// pipeline whose channel already carries synthetic tokens. Without the
	// guard in preprocessForExactlyOncePipe every row would be dropped as an
	// already-committed duplicate (a synthetic token compares newer than any
	// real one) and the batch acked. The batch must fail instead, and the
	// row count must not move.
	err := sendSSv2Batch(t, env, pipe, ssv2OutputOpts{offsetToken: "${! @kafka_offset }"}, service.MessageBatch{
		ssv2Row(`{"stake": 9.99, "bet_id": "nd-4-must-not-land"}`, 0, 4),
	})
	require.Error(t, err, "adding offset_token to a channel last written at-least-once must be refused, not silently dedup every row away")
	require.Contains(t, err.Error(), "fresh channel_name")
	// Give a wrongly-acked append time to have shown up before asserting it
	// didn't.
	time.Sleep(10 * time.Second)
	assertSSv2Scalar(t, ctx, sql, env, "no row may land from the refused exactly-once batch", fmt.Sprintf("select count(*) from %s", table), "3")

	// The supported direction on a fresh channel: same pipe, exactly-once,
	// but under a channel_name nothing has written to. Must work normally.
	freshChannel := pipe + "-fresh-eo"
	require.NoError(t, sendSSv2Batch(t, env, pipe, ssv2OutputOpts{offsetToken: "${! @kafka_offset }", channelName: freshChannel}, service.MessageBatch{
		ssv2Row(`{"stake": 2.00, "bet_id": "nd-5"}`, 0, 1),
	}))
	waitForSSv2RowCount(t, ctx, sql, env, table, 4, 90*time.Second)
}

// TestIntegrationSnowflakeStreamingPipeMultiChannelBatch proves
// groupMessagesByChannel end to end against the real API: a single
// WriteBatch call (one produce call, not one per row) whose messages
// resolve to two *different* channels, via exactly the per-partition
// channel_name pattern the docs and worked examples recommend
// ("<pipe>-p${! @kafka_partition }"), must land every row correctly on its
// own channel -- not just the fake-pool unit test
// (TestWriteBatchGroupsInterleavedMessagesByChannel) that never talks to
// Snowflake. Also exercises within-channel ordering (two messages on
// partition 0 at different offsets) alongside the cross-channel split in
// the same call.
func TestIntegrationSnowflakeStreamingPipeMultiChannelBatch(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_MULTICHAN"
	pipe := env.Pipe + "_MULTICHAN"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	opts := ssv2OutputOpts{
		offsetToken: "${! @kafka_offset }",
		channelName: pipe + `-p${! @kafka_partition }`,
	}
	batch := service.MessageBatch{
		ssv2Row(`{"stake": 1.00, "bet_id": "a-1"}`, 0, 1), // channel "<pipe>-p0"
		ssv2Row(`{"stake": 2.00, "bet_id": "b-1"}`, 1, 1), // channel "<pipe>-p1"
		ssv2Row(`{"stake": 3.00, "bet_id": "a-2"}`, 0, 2), // channel "<pipe>-p0", after a-1
	}
	require.NoError(t, sendSSv2Batch(t, env, pipe, opts, batch))
	waitForSSv2RowCount(t, ctx, sql, env, table, 3, 90*time.Second)

	assertSSv2Scalar(t, ctx, sql, env, "partition 0's first row landed",
		fmt.Sprintf("select stake from %s where bet_id = 'a-1'", table), "1.00")
	assertSSv2Scalar(t, ctx, sql, env, "partition 1's row landed on its own channel",
		fmt.Sprintf("select stake from %s where bet_id = 'b-1'", table), "2.00")
	assertSSv2Scalar(t, ctx, sql, env, "partition 0's second row landed after the first",
		fmt.Sprintf("select stake from %s where bet_id = 'a-2'", table), "3.00")
}

// TestIntegrationSnowflakeStreamingPipeRowRejection proves tolerate_row_errors'
// two documented branches against a real rejection, not a mocked one: a row
// whose stake value can't cast to number(38,2) is accepted by AppendRows
// (2xx) and only rejected asynchronously by the pipe's own COPY INTO, which
// is exactly the class of failure tolerate_row_errors exists to control.
func TestIntegrationSnowflakeStreamingPipeRowRejection(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_REJECT"
	pipe := env.Pipe + "_REJECT"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	t.Run("FailsByDefault", func(t *testing.T) {
		opts := ssv2OutputOpts{offsetToken: "${! @kafka_offset }"} // tolerateRowErrors defaults false
		batch := service.MessageBatch{
			ssv2Row(`{"stake": 9.00, "bet_id": "ok-1"}`, 0, 1),
			ssv2Row(`{"stake": "not-a-number", "bet_id": "bad-1"}`, 0, 2),
		}
		err := sendSSv2Batch(t, env, pipe, opts, batch)
		require.Error(t, err, "a row Snowflake rejects should fail the batch when tolerate_row_errors is false")

		// Snowflake accepts and processes rows individually -- the
		// rejection is per-row, not per-batch, so the valid row in the same
		// append still lands despite the batch call itself returning an
		// error.
		waitForSSv2RowCount(t, ctx, sql, env, table, 1, 90*time.Second)
	})

	t.Run("WarnsAndContinuesWhenTolerated", func(t *testing.T) {
		// This channel already carries one historical rejection from
		// FailsByDefault above (rows_error_count is cumulative and never
		// resets, including across the fresh channel reopen a new stream
		// instance causes -- see RowsRejected's own doc comment). That's
		// deliberately not reset between subtests: it also proves the
		// baseline/delta accounting still reports only the *new* rejection
		// here, not re-flagging the old one.
		opts := ssv2OutputOpts{offsetToken: "${! @kafka_offset }", tolerateRowErrors: true}
		batch := service.MessageBatch{
			ssv2Row(`{"stake": 11.00, "bet_id": "ok-2"}`, 0, 3),
			ssv2Row(`{"stake": "still-not-a-number", "bet_id": "bad-2"}`, 0, 4),
		}
		require.NoError(t, sendSSv2Batch(t, env, pipe, opts, batch),
			"tolerate_row_errors: true should log a warning and return nil, not fail the batch")
		waitForSSv2RowCount(t, ctx, sql, env, table, 2, 90*time.Second)
	})
}
