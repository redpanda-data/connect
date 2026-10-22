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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// Edge cases beyond the tier 1/2/2b happy path in
// streaming_pipe_integration_test.go.

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

// TestIntegrationSnowflakeStreamingPipeMissingPipeFailsAtConnect: a missing
// pipe is reported by Connect's pre-flight, before any row is written (an
// invalid pipe would otherwise accept appends with 2xx and discard them).
func TestIntegrationSnowflakeStreamingPipeMissingPipeFailsAtConnect(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)

	missingPipe := env.Pipe + "_MISSING_FOR_TEST"

	// The framework retries Connect errors rather than failing Run, so drive
	// the output directly. buildSSv2OutputYAML emits the component-level
	// form; ParseYAML wants the inner mapping.
	yaml := strings.TrimPrefix(strings.TrimLeft(buildSSv2OutputYAML(env, missingPipe, 1, defaultSSv2OutputOpts()), "\n"), "snowflake_streaming_pipe:\n")
	conf, err := snowpipeStreamingPipeOutputConfig().ParseYAML(yaml, nil)
	require.NoError(t, err)
	res := service.MockResources()
	license.InjectTestService(res)
	out, _, _, err := newSnowpipeStreamingPipeOutput(conf, res)
	require.NoError(t, err, "construction must not touch the network, so a missing pipe cannot fail here")

	err = out.Connect(t.Context())
	require.Error(t, err, "expected Connect to fail against a nonexistent pipe %q", missingPipe)
	require.Contains(t, err.Error(), "pipe pre-flight")
	require.Contains(t, err.Error(), missingPipe)
}

// TestIntegrationSnowflakeStreamingPipeOffsetTokenUnsetCommitsForReal:
// at-least-once mode (no offset_token) commits against a real account, a
// second "process" resumes on the same channel, and adding offset_token to
// that channel afterwards is refused rather than deduplicating every row.
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

	// A second, independent stream on the same channel: the synthetic token
	// must round-trip through Snowflake intact (the '~' prefix included) so
	// the mode-switch check below can recognise it.
	sendSSv2RowsWithOpts(t, env, pipe, 1, ssv2OutputOpts{}, []*service.Message{
		ssv2Row(`{"stake": 1.50, "bet_id": "nd-3"}`, 0, 3),
	})
	waitForSSv2RowCount(t, ctx, sql, env, table, 3, 90*time.Second)

	// Adding offset_token to a channel that carries synthetic tokens must be
	// refused with nothing landing. A non-Kafka token (@seq) is used so the
	// config linter doesn't reject the config before the runtime guard runs.
	switched := ssv2Row(`{"stake": 9.99, "bet_id": "nd-4-must-not-land"}`, 0, 4)
	switched.MetaSetMut("seq", 4)
	err := sendSSv2Batch(t, env, pipe, ssv2OutputOpts{offsetToken: "${! @seq }"}, service.MessageBatch{switched})
	require.Error(t, err, "adding offset_token to a channel last written at-least-once must be refused, not silently dedup every row away")
	require.Contains(t, err.Error(), "fresh channel_name")
	// Give a wrongly-acked append time to have shown up before asserting it
	// didn't.
	time.Sleep(10 * time.Second)
	assertSSv2Scalar(t, ctx, sql, env, "no row may land from the refused exactly-once batch", fmt.Sprintf("select count(*) from %s", table), "3")

	// The supported direction on a fresh channel: same pipe, exactly-once,
	// but under a channel_name nothing has written to. Must work normally.
	freshChannel := pipe + "-fresh-eo-p${! @kafka_partition }"
	require.NoError(t, sendSSv2Batch(t, env, pipe, ssv2OutputOpts{offsetToken: "${! @kafka_offset }", channelName: freshChannel}, service.MessageBatch{
		ssv2Row(`{"stake": 2.00, "bet_id": "nd-5"}`, 0, 1),
	}))
	waitForSSv2RowCount(t, ctx, sql, env, table, 4, 90*time.Second)
}

// TestIntegrationSnowflakeStreamingPipeMultiChannelBatch: one WriteBatch
// whose messages resolve to two channels lands every row on the right
// channel, in order.
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

// TestIntegrationSnowflakeStreamingPipeRowRejection: both tolerate_row_errors
// branches against a real asynchronous rejection (a stake that can't cast to
// number(38,2) is accepted by the append and rejected by the pipe).
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
		opts := defaultSSv2OutputOpts() // exactly-once, per-partition channel; tolerateRowErrors defaults false
		batch := service.MessageBatch{
			ssv2Row(`{"stake": 9.00, "bet_id": "ok-1"}`, 0, 1),
			ssv2Row(`{"stake": "not-a-number", "bet_id": "bad-1"}`, 0, 2),
		}
		err := sendSSv2Batch(t, env, pipe, opts, batch)
		require.Error(t, err, "a row Snowflake rejects should fail the batch when tolerate_row_errors is false")

		// Rejection is per-row: the valid row in the same append still lands.
		waitForSSv2RowCount(t, ctx, sql, env, table, 1, 90*time.Second)
	})

	t.Run("WarnsAndContinuesWhenTolerated", func(t *testing.T) {
		// The channel already carries one rejection from the subtest above
		// (rows_error_count never resets); only the new one may be reported.
		opts := defaultSSv2OutputOpts()
		opts.tolerateRowErrors = true
		batch := service.MessageBatch{
			ssv2Row(`{"stake": 11.00, "bet_id": "ok-2"}`, 0, 3),
			ssv2Row(`{"stake": "still-not-a-number", "bet_id": "bad-2"}`, 0, 4),
		}
		require.NoError(t, sendSSv2Batch(t, env, pipe, opts, batch),
			"tolerate_row_errors: true should log a warning and return nil, not fail the batch")
		waitForSSv2RowCount(t, ctx, sql, env, table, 2, 90*time.Second)
	})
}
