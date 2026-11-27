// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

// TestConnectCacheResourceRestartReplaysOpenTransaction checks a restart with
// a cache backend that keeps its keys, for example Redis. The checkpoint stays
// below the START of an open transaction, so after the restart LogMiner emits
// the START and the rows of that transaction again. The commit must publish
// each row one time.
func TestConnectCacheResourceRestartReplaysOpenTransaction(t *testing.T) {
	// The mock cache lives in res, so a second ConnectCacheResource over res
	// sees the keys of the first one, as a durable backend does.
	res := service.MockResources(service.MockResourcesOptAddCache("txn_cache"))
	cfg := TransactionCacheConfig{
		CacheName: "txn_cache",
		CacheKey:  "oracledb_cdc",
	}
	newCache := func() *ConnectCacheResource {
		return NewConnectCacheResource(res, cfg, res.Metrics(), service.NewLoggerFromSlog(slog.Default()))
	}
	rowA := &sqlredo.DMLEvent{
		Operation:     sqlredo.OpInsert,
		Table:         "T",
		Data:          map[string]any{"ID": int64(1)},
		TransactionID: "txA",
	}
	rowB := &sqlredo.DMLEvent{
		Operation:     sqlredo.OpInsert,
		Table:         "T",
		Data:          map[string]any{"ID": int64(2)},
		TransactionID: "txA",
	}

	// Before the restart: the connector mines the START and two rows of txA,
	// then stops before the COMMIT.
	before := newCache()
	require.NoError(t, before.StartTransaction(t.Context(), "txA", 100))
	require.NoError(t, before.AddEvent(t.Context(), "txA", 101, rowA))
	require.NoError(t, before.AddEvent(t.Context(), "txA", 102, rowB))

	// After the restart: the connector resumes from SCN 99 and mines the same
	// START and rows again.
	after := newCache()
	require.NoError(t, after.StartTransaction(t.Context(), "txA", 100))
	require.NoError(t, after.AddEvent(t.Context(), "txA", 101, rowA))
	require.NoError(t, after.AddEvent(t.Context(), "txA", 102, rowB))

	txn, err := after.GetTransaction(t.Context(), "txA")
	require.NoError(t, err)
	require.NotNil(t, txn)
	assert.Equal(t, uint64(100), txn.SCN)
	assert.Equal(t, []*sqlredo.DMLEvent{rowA, rowB}, txn.Events)
}
