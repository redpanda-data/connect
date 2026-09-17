// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// TestIntegrationOracleDBCDCDistinctRSIDSSNPerRowChange verifies that row changes
// within a transaction produce distinct (rs_id, ssn, row_seq) triples for
// deduplication and ordering.
//
// For bulk DELETEs, Oracle's LogMiner emits array DELETE redo records where all
// rows share rs_id and ssn=0. The connector numbers those rows in row_seq so
// every change has a distinct triple. The other cases check that row_seq does
// not break what (rs_id, ssn) already told apart.
func TestIntegrationOracleDBCDCDistinctRSIDSSNPerRowChange(t *testing.T) {
	integration.CheckSkip(t)
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)

	const createTableSQL = "CREATE TABLE %s (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val NUMBER)"

	// rows is the size of every workload. 1000 rows reproduce the array DELETE
	// grouping on Oracle Free 23ai, where a 300-row DELETE did not group at all.
	const rows = 1000

	// Each case runs dml against a table that holds initialRows rows and expects
	// wantChanges messages, one per changed row, every one with its own
	// (rs_id, ssn, row_seq).
	// The initialRows are inserted before the stream starts, so they are not emitted.
	cases := []struct {
		name        string
		table       string
		initialRows int
		dml         func(table string)
		wantChanges int
	}{
		{
			name:        "single-row INSERTs",
			table:       "testdb.rsid_insert",
			initialRows: 0,
			dml: func(table string) {
				for range rows {
					db.MustExec("INSERT INTO " + table + " (val) VALUES (1)")
				}
			},
			wantChanges: rows,
		},
		{
			name:        "bulk UPDATE",
			table:       "testdb.rsid_update",
			initialRows: rows,
			dml:         func(table string) { db.MustExec("UPDATE " + table + " SET val = 2") },
			wantChanges: rows,
		},
		{
			name:        "bulk DELETE",
			table:       "testdb.rsid_delete",
			initialRows: rows,
			dml:         func(table string) { db.MustExec("DELETE FROM " + table) },
			wantChanges: rows,
		},
	}

	includes := make([]string, 0, len(cases))
	totalChanges := 0
	for _, tc := range cases {
		require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), tc.table,
			fmt.Sprintf(createTableSQL, tc.table)))
		for range tc.initialRows {
			db.MustExec("INSERT INTO " + tc.table + " (val) VALUES (1)")
		}
		includes = append(includes, strings.ToUpper(tc.table))
		totalChanges += tc.wantChanges
	}

	// snapshot_mode is none, so the stream only reports DML issued after this point.
	// The buffer holds every expected message so a failed subtest cannot block the stream.
	msgChan := startTestCDCStream(t, connStr, totalChanges, includes...)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.dml(tc.table)
			msgs := collectN(t, msgChan, tc.wantChanges)
			groups := groupByRecordIdentity(t, msgs)
			assert.Len(t, groups, len(msgs),
				"every row change must have a distinct (rs_id, ssn, row_seq) triple: %d row changes, %d distinct triples", len(msgs), len(groups))
		})
	}
}

func startTestCDCStream(t *testing.T, connStr string, buffer int, tableIncludes ...string) <-chan *service.Message {
	t.Helper()
	msgChan := make(chan *service.Message, buffer)
	cfg := `
oracledb_cdc:
  connection_string: ` + connStr + `
  snapshot_mode: none
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + strings.Join(tableIncludes, `", "`) + `"]
  batching:
    count: 1`

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamBuilder.AddInputYAML(cfg))
	require.NoError(t, streamBuilder.AddBatchConsumerFunc(func(ctx context.Context, mb service.MessageBatch) error {
		for _, msg := range mb {
			select {
			case msgChan <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	go func() {
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() { require.NoError(t, stream.StopWithin(10*time.Second)) })
	time.Sleep(10 * time.Second) // wait for miner to start before DML

	return msgChan
}

// collectN waits for n messages and fails the test if they do not all arrive in time.
func collectN(t *testing.T, c <-chan *service.Message, n int) []*service.Message {
	t.Helper()
	const timeout = 30 * time.Second // observed runs take under 3s per 1000 rows
	msgs := make([]*service.Message, 0, n)
	deadline := time.After(timeout)
	for len(msgs) < n {
		select {
		case msg := <-c:
			msgs = append(msgs, msg)
		case <-deadline:
			t.Fatalf("received %d of %d messages within %s", len(msgs), n, timeout)
		}
	}
	return msgs
}

// groupByRecordIdentity counts messages per (rs_id, ssn, row_seq) triple.
func groupByRecordIdentity(t *testing.T, msgs []*service.Message) map[string]int {
	t.Helper()
	groups := make(map[string]int)
	for i, msg := range msgs {
		rsID, ok := msg.MetaGet("rs_id")
		require.Truef(t, ok, "message %d missing rs_id", i)
		ssn, ok := msg.MetaGet("ssn")
		require.Truef(t, ok, "message %d missing ssn", i)
		rowSeq, ok := msg.MetaGet("row_seq")
		require.Truef(t, ok, "message %d missing row_seq", i)
		groups[rsID+":"+ssn+":"+rowSeq]++
	}
	return groups
}
