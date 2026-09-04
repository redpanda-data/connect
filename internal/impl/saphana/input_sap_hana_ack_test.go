// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	testCacheName = "testcache"
	testIncQuery  = `SELECT * FROM "T" WHERE "ID" > ? ORDER BY "ID"`
)

func enterpriseResourcesWithCache() *service.Resources {
	res := service.MockResources(service.MockResourcesOptAddCache(testCacheName))
	license.InjectTestService(res)
	return res
}

// newTestInput builds an input from the given config wired to a sqlmock DB,
// mirroring the state Connect would establish (hwmSafe seeded from hwm).
func newTestInput(t *testing.T, res *service.Resources, confYAML string) (*sapHANAInput, sqlmock.Sqlmock) {
	t.Helper()
	conf := parseInputConf(t, confYAML)
	s, err := newSAPHANAInput(conf, res)
	require.NoError(t, err)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s.db = db
	s.schemas = newSchemaCache(db, s.log, s.numericMapping)
	s.hwmSafe = s.hwm
	return s, mock
}

// newTestIncInput builds an incrementing-mode input with a short poll interval.
func newTestIncInput(t *testing.T, res *service.Resources, fetchSize int) (*sapHANAInput, sqlmock.Sqlmock) {
	t.Helper()
	return newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "0"
poll_interval: 1ms
fetch_size: %d
max_retries: 0
checkpoint_cache: %s
`, fetchSize, testCacheName))
}

func readCheckpoint(t *testing.T, res *service.Resources) *sapHANACheckpointState {
	t.Helper()
	var (
		raw    []byte
		getErr error
	)
	require.NoError(t, res.AccessCache(t.Context(), testCacheName, func(c service.Cache) {
		raw, getErr = c.Get(t.Context(), "sap_hana_hwm")
	}))
	if errors.Is(getErr, service.ErrKeyNotFound) {
		return nil
	}
	require.NoError(t, getErr)
	cp := &sapHANACheckpointState{}
	require.NoError(t, json.Unmarshal(raw, cp))
	return cp
}

// A transient error mid-cursor discards the partial batch; the in-memory HWM
// must rewind to the last safe value so the discarded rows are re-read on the
// next poll instead of being skipped forever.
func TestSAPHANAInputIncrementingScanErrorRewindsHWM(t *testing.T) {
	s, mock := newTestIncInput(t, enterpriseResourcesWithCache(), 10)

	rows := sqlmock.NewRows([]string{"ID"}).
		AddRow(int64(1)).
		AddRow(int64(2)).
		AddRow(int64(3)).
		RowError(2, errors.New("connection reset"))
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	_, _, err := s.ReadBatch(t.Context())
	require.ErrorContains(t, err, "iterating rows")
	assert.Equal(t, int64(0), s.hwm,
		"HWM must rewind to hwmSafe after the scanned rows were discarded")
}

// The full loss scenario from review: a transient mid-cursor error discards
// scanned rows, and the next poll comes back empty. The re-poll must query
// from the rewound HWM and the empty poll must persist that safe value, so a
// restart cannot skip the discarded rows.
func TestSAPHANAInputEmptyPollAfterScanErrorPersistsSafeHWM(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestIncInput(t, res, 10)

	rows := sqlmock.NewRows([]string{"ID"}).
		AddRow(int64(1)).
		AddRow(int64(2)).
		AddRow(int64(3)).
		RowError(2, errors.New("connection reset"))
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	_, _, err := s.ReadBatch(t.Context())
	require.ErrorContains(t, err, "iterating rows")

	// The re-poll must bind the rewound HWM (0), not the inflated one (2).
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}))
	// The poll after the empty one has no sqlmock expectation, so ReadBatch
	// errors out, which is the exit this test needs.
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err)

	cp := readCheckpoint(t, res)
	require.NotNil(t, cp, "empty poll should persist a checkpoint")
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(0), *cp.IncrHWMInt,
		"empty poll must persist the rewound safe HWM")
}

// With multiple batches in flight, a later batch acking first must not persist
// its HWM past the still-unacked earlier batch.
func TestSAPHANAInputCheckpointWaitsForOrderedAcks(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestIncInput(t, res, 2)

	rows := sqlmock.NewRows([]string{"ID"})
	for i := int64(1); i <= 5; i++ {
		rows.AddRow(i)
	}
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	batch1, ack1, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch1, 2)

	batch2, ack2, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch2, 2)

	require.NoError(t, ack2(t.Context(), nil))
	assert.Nil(t, readCheckpoint(t, res),
		"acking batch 2 while batch 1 is in flight must not persist a checkpoint")

	require.NoError(t, ack1(t.Context(), nil))
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(4), *cp.IncrHWMInt,
		"once both batches acked the highest contiguous HWM should persist")
}

// A nacked batch is dropped by contract when auto_replay_nacks is disabled, so
// its checkpoint slot must resolve like an ack: pinning the tracker would stall
// the input, and skipping the persist would strand later acked batches.
func TestSAPHANAInputNackResolvesCheckpointSlot(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestIncInput(t, res, 2)

	rows := sqlmock.NewRows([]string{"ID"})
	for i := int64(1); i <= 5; i++ {
		rows.AddRow(i)
	}
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	_, ack1, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	_, ack2, err := s.ReadBatch(t.Context())
	require.NoError(t, err)

	require.NoError(t, ack1(t.Context(), errors.New("rejected downstream")))
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp, "a nack must resolve its slot and persist progress")
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(2), *cp.IncrHWMInt)

	require.NoError(t, ack2(t.Context(), nil))
	cp = readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(4), *cp.IncrHWMInt)
}

// A freshly started polling input must issue its first query immediately
// rather than sleeping a full poll_interval before emitting anything.
func TestSAPHANAInputFirstPollQueriesImmediately(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "0"
poll_interval: 1h
fetch_size: 10
max_retries: 0
`)

	rows := sqlmock.NewRows([]string{"ID"}).AddRow(int64(1)).AddRow(int64(2))
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	batch, _, err := s.ReadBatch(ctx)
	require.NoError(t, err, "first poll must not wait for poll_interval")
	require.Len(t, batch, 2)
}

// A transient query failure is retried after the configured retry_backoff.
func TestSAPHANAInputQueryRetryUsesConfiguredBackoff(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "0"
poll_interval: 1ms
fetch_size: 10
max_retries: 1
retry_backoff: 1ms
`)

	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).
		WillReturnError(errors.New("temporarily unavailable"))
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(int64(1)))

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	batch, _, err := s.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}

const (
	testTSFirstQuery  = `SELECT * FROM "T" WHERE "TS" <= ? ORDER BY "TS"`
	testTSWindowQuery = `SELECT * FROM "T" WHERE "TS" > ? AND "TS" <= ? ORDER BY "TS"`
	testTSIncFallback = `SELECT * FROM "T" WHERE "TS" > ? AND "TS" <= ? ORDER BY "TS", "ID"`
	testTSIncTieBreak = `SELECT * FROM "T" WHERE ("TS" > ? OR ("TS" = ? AND "ID" > ?)) AND "TS" <= ? ORDER BY "TS", "ID"`
)

// timestamp mode: the first poll scans everything up to the window upper
// bound, the HWM advances to that bound after a full scan, and the next poll
// binds it as the window lower bound.
func TestSAPHANAInputTimestampWindowAdvances(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 0s
`)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(testTSFirstQuery).WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(base).AddRow(base.Add(time.Second)))

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.False(t, s.timestampHWM.IsZero(),
		"timestamp HWM must advance to the window upper bound after a full scan")
	require.Equal(t, s.tsQueryUpper, s.timestampHWM)

	mock.ExpectQuery(testTSWindowQuery).WithArgs(s.timestampHWM, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}))
	// The poll after the empty one has no expectation, ending the loop.
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// timestamp+incrementing mode: while no incrementing value has been observed
// the query falls back to a pure timestamp window (avoiding a nil bind), and
// once one is seen the tie-breaking predicate binds it.
func TestSAPHANAInputTimestampIncrementingTieBreak(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: timestamp+incrementing
table: T
timestamp_column: TS
incrementing_column: ID
timestamp_initial_value: "2026-01-01T00:00:00Z"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 0s
`)

	base := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	mock.ExpectQuery(testTSIncFallback).WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).AddRow(base, int64(5)))

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, int64(5), s.hwm)

	mock.ExpectQuery(testTSIncTieBreak).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), int64(5), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}))
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A persisted checkpoint is loaded by a fresh input, resuming the HWM with its
// original type instead of re-reading from the initial value.
func TestSAPHANAInputCheckpointResumesAcrossRestart(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s1, mock := newTestIncInput(t, res, 10)
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(int64(1)).AddRow(int64(2)))

	batch, ack, err := s1.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.NoError(t, ack(t.Context(), nil))

	conf := parseInputConf(t, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "0"
checkpoint_cache: %s
`, testCacheName))
	s2, err := newSAPHANAInput(conf, res)
	require.NoError(t, err)
	require.NoError(t, s2.loadCheckpoint(t.Context()))
	require.Equal(t, int64(2), s2.hwm, "restarted input must resume from the persisted HWM")
}
