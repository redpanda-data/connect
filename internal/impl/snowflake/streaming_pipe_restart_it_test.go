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
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// TestIntegrationSnowflakeStreamingPipeRestartRedeliversPartialOverlapWithoutDuplication:
// an independent second stream redelivering a range that partially overlaps
// what the first committed (as a consumer group does after a rebalance)
// lands each row exactly once.
func TestIntegrationSnowflakeStreamingPipeRestartRedeliversPartialOverlapWithoutDuplication(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	sql := newSSv2SQLClient(t, env, pk)
	ctx := t.Context()

	table := env.Table + "_RESTART"
	pipe := env.Pipe + "_RESTART"
	provisionSSv2Tier1Objects(t, ctx, sql, env, table, pipe)

	// "Instance 1": offsets 1-3 land normally.
	sendSSv2Rows(t, env, pipe, 1, []*service.Message{
		ssv2Row(`{"stake": 1.00, "bet_id": "r-1"}`, 0, 1),
		ssv2Row(`{"stake": 2.00, "bet_id": "r-2"}`, 0, 2),
		ssv2Row(`{"stake": 3.00, "bet_id": "r-3"}`, 0, 3),
	})
	waitForSSv2RowCount(t, ctx, sql, env, table, 3, 90*time.Second)

	// Instance 2 redelivers 2-5: 2 and 3 already landed, 4 and 5 are new.
	sendSSv2Rows(t, env, pipe, 1, []*service.Message{
		ssv2Row(`{"stake": 2.00, "bet_id": "r-2"}`, 0, 2),
		ssv2Row(`{"stake": 3.00, "bet_id": "r-3"}`, 0, 3),
		ssv2Row(`{"stake": 4.00, "bet_id": "r-4"}`, 0, 4),
		ssv2Row(`{"stake": 5.00, "bet_id": "r-5"}`, 0, 5),
	})

	// Exactly 5 rows total: the redelivered 2 and 3 must not duplicate, and
	// 4 and 5 must actually land -- not "nothing new happened because the
	// whole redelivered batch looked like a retry".
	waitForSSv2RowCount(t, ctx, sql, env, table, 5, 90*time.Second)
	for _, betID := range []string{"r-1", "r-2", "r-3", "r-4", "r-5"} {
		assertSSv2Scalar(t, ctx, sql, env, fmt.Sprintf("%s present exactly once", betID),
			fmt.Sprintf("select count(*) from %s where bet_id = '%s'", table, betID), "1")
	}
}
