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
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	gohdb "github.com/SAP/go-hdb/driver"
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

	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(hanaValueConverter{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s.db = db
	s.schemas = newSchemaCache(db, s.log, s.conf.numericMapping)
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

	// The window upper bound comes from the database clock (same session
	// timezone convention as a DEFAULT CURRENT_TIMESTAMP column), never from
	// the connector host's clock: a database hours away from the host would
	// otherwise skip or delay rows forever.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	dbNow1 := time.Date(2026, 1, 1, 7, 30, 0, 0, time.UTC)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow1))
	mock.ExpectQuery(testTSFirstQuery).WithArgs(dbNow1).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(base).AddRow(base.Add(time.Second)))

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.Equal(t, dbNow1, s.timestampHWM,
		"timestamp HWM must advance to the database-derived window upper bound after a full scan")

	dbNow2 := dbNow1.Add(time.Minute)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow2))
	mock.ExpectQuery(testTSWindowQuery).WithArgs(dbNow1, dbNow2).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}))
	// The poll after the empty one has no expectation, ending the loop.
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// timestamp_delay is applied database-side in seconds, and timestamp_clock
// selects the UTC clock for columns populated in UTC.
func TestSAPHANAInputTimestampDelayAndUTCClock(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
timestamp_clock: database_utc
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 5s
`)
	dbNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(hanaUTCClockQuery).WithArgs(float64(-5)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSFirstQuery).WithArgs(dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}))
	_, _, err := s.ReadBatch(t.Context())
	require.Error(t, err) // the poll after the empty one has no expectation
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

	initial := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	dbNow1 := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow1))
	mock.ExpectQuery(testTSIncFallback).WithArgs(initial, dbNow1).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).AddRow(base, int64(5)))

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Equal(t, int64(5), s.hwm)

	dbNow2 := dbNow1.Add(time.Minute)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow2))
	mock.ExpectQuery(testTSIncTieBreak).
		WithArgs(dbNow1, dbNow1, int64(5), dbNow2).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}))
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// timestamp+incrementing checkpoints mid-window at the last delivered row
// (its timestamp and incrementing value), so a restart resumes exactly after
// it via the tie-break predicate instead of re-reading the whole window.
func TestSAPHANAInputTimestampIncrementingCheckpointsAtLastRow(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp+incrementing
table: T
timestamp_column: TS
incrementing_column: ID
timestamp_initial_value: "2026-01-01T00:00:00Z"
poll_interval: 1ms
fetch_size: 2
max_retries: 0
timestamp_delay: 0s
checkpoint_cache: %s
`, testCacheName))

	initial := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := initial.Add(1 * time.Second)
	t2 := initial.Add(2 * time.Second)
	t3 := initial.Add(3 * time.Second)
	dbNow := initial.Add(time.Minute)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSIncFallback).WithArgs(initial, dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).
			AddRow(t1, int64(1)).AddRow(t2, int64(2)).AddRow(t3, int64(3)))

	batch, ack, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.NoError(t, ack(t.Context(), nil))

	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.TimestampHWM)
	require.NotNil(t, cp.IncrHWMInt)
	assert.True(t, t2.Equal(*cp.TimestampHWM), "mid-window checkpoint must carry the last delivered row's timestamp, got %v", *cp.TimestampHWM)
	assert.Equal(t, int64(2), *cp.IncrHWMInt)

	// Consuming the rest of the window advances to the window bound.
	batch, ack, err = s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, ack(t.Context(), nil))
	cp = readCheckpoint(t, res)
	require.NotNil(t, cp)
	assert.True(t, dbNow.Equal(*cp.TimestampHWM), "a fully consumed window checkpoints at the window bound, got %v", *cp.TimestampHWM)
	assert.Equal(t, int64(3), *cp.IncrHWMInt)
}

// The mid-window resume key must be one row's (timestamp, incrementing) pair.
// A NULL incrementing value leaves the HWM at an earlier row; pairing that
// stale HWM with the current row's timestamp would skip rows on resume.
func TestSAPHANAInputTimestampIncrementingNullKeyFallsBackToWindowStart(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp+incrementing
table: T
timestamp_column: TS
incrementing_column: ID
timestamp_initial_value: "2026-01-01T00:00:00Z"
poll_interval: 1ms
fetch_size: 2
max_retries: 0
timestamp_delay: 0s
checkpoint_cache: %s
`, testCacheName))

	initial := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t0 := initial.Add(1 * time.Second)
	t1 := initial.Add(2 * time.Second)
	dbNow := initial.Add(time.Minute)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSIncFallback).WithArgs(initial, dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).
			AddRow(t0, int64(99)).AddRow(t1, nil).AddRow(t1, int64(5)))

	_, ack, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, ack(t.Context(), nil))

	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.TimestampHWM)
	require.NotNil(t, cp.IncrHWMInt)
	assert.True(t, initial.Equal(*cp.TimestampHWM),
		"with no usable key on the last row the checkpoint must stay at the window start, got %v", *cp.TimestampHWM)
	assert.Equal(t, int64(99), *cp.IncrHWMInt)
}

// The schema metadata value is shared by every message from the cache, so a
// downstream component editing what MetaGetMut hands it must not be able to
// corrupt the cached tree (the documented contract of immutable metadata).
func TestSAPHANAInputSchemaMetadataIsImmutable(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: bulk
schema_name: S
table: T
fetch_size: 10
max_retries: 0
`)

	mock.ExpectQuery(hanaColumnQuery).WithArgs("S", "T").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE_NAME", "LENGTH", "SCALE", "IS_NULLABLE"}).
			AddRow("ID", "BIGINT", int64(19), int64(0), "FALSE"))
	mock.ExpectQuery(hanaPKQuery).WithArgs("S", "T").WillReturnRows(
		sqlmock.NewRows([]string{"COLUMN_NAME"}).AddRow("ID"))
	mock.ExpectQuery(`SELECT * FROM "S"."T"`).WillReturnRows(
		sqlmock.NewRows([]string{"ID"}).AddRow(int64(1)))

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)

	cached := s.schemas.entries["S.T"]
	require.NotNil(t, cached, "schema should be cached after the first row")
	before, err := json.Marshal(cached.result.Val)
	require.NoError(t, err)
	require.NotEqual(t, "null", string(before))

	got, ok := batch[0].MetaGetMut("schema")
	require.True(t, ok)
	tree, ok := got.(map[string]any)
	require.True(t, ok, "schema metadata should be a JSON-style tree, got %T", got)
	for k := range tree {
		delete(tree, k)
	}

	after, err := json.Marshal(cached.result.Val)
	require.NoError(t, err)
	assert.JSONEq(t, string(before), string(after),
		"mutating the metadata handed to a downstream component must not alter the shared schema cache")
}

// hanaValueConverter lets sqlmock rows carry the *big.Rat values go-hdb
// produces for DECIMAL columns, which are not standard driver.Values.
type hanaValueConverter struct{}

func (hanaValueConverter) ConvertValue(v any) (driver.Value, error) {
	if _, ok := v.(*big.Rat); ok {
		return v, nil
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}

func ratOf(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok, "bad decimal literal %q", s)
	return r
}

func plantCacheSentinel(t *testing.T, res *service.Resources) {
	t.Helper()
	require.NoError(t, res.AccessCache(t.Context(), testCacheName, func(c service.Cache) {
		require.NoError(t, c.Set(t.Context(), "sap_hana_hwm", []byte("sentinel"), nil))
	}))
}

func cacheHoldsSentinel(t *testing.T, res *service.Resources) bool {
	t.Helper()
	var raw []byte
	require.NoError(t, res.AccessCache(t.Context(), testCacheName, func(c service.Cache) {
		var err error
		raw, err = c.Get(t.Context(), "sap_hana_hwm")
		require.NoError(t, err)
	}))
	return string(raw) == "sentinel"
}

// An ack that does not advance the highest contiguous checkpoint (an
// out-of-order ack behind a pending batch, or an empty poll with no new rows)
// must not rewrite identical bytes to the cache on every occurrence.
func TestSAPHANAInputSkipsUnchangedCheckpointWrites(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestIncInput(t, res, 2)

	rows := sqlmock.NewRows([]string{"ID"})
	for i := int64(1); i <= 6; i++ {
		rows.AddRow(i)
	}
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	_, ack1, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	_, ack2, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	_, ack3, err := s.ReadBatch(t.Context())
	require.NoError(t, err)

	require.NoError(t, ack1(t.Context(), nil))
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.Equal(t, int64(2), *cp.IncrHWMInt)

	// ack3 resolves behind the still-pending batch 2: the highest contiguous
	// checkpoint is unchanged, so nothing should be written.
	plantCacheSentinel(t, res)
	require.NoError(t, ack3(t.Context(), nil))
	assert.True(t, cacheHoldsSentinel(t, res), "out-of-order ack must not rewrite an unchanged checkpoint")

	require.NoError(t, ack2(t.Context(), nil))
	cp = readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.Equal(t, int64(6), *cp.IncrHWMInt)

	// An empty poll snapshots the same HWM again; identical state, no write.
	plantCacheSentinel(t, res)
	mock.ExpectQuery(testIncQuery).WithArgs(int64(6)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}))
	_, _, err = s.ReadBatch(t.Context())
	require.Error(t, err) // the poll after the empty one has no expectation
	assert.True(t, cacheHoldsSentinel(t, res), "empty poll must not rewrite an unchanged checkpoint")
}

// Without schema metadata (no schema_name, or query mode) column types come
// from the driver's result metadata, so the wire encoding of a column is
// fixed per column rather than guessed from each row's bytes.
func TestSAPHANAInputUsesDriverColumnTypesWithoutSchema(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: query
query: "SELECT * FROM T"
fetch_size: 10
max_retries: 0
`)

	// mock.NewRows* (not the package-level constructor) honours the mock's
	// value converter, which is what admits *big.Rat.
	rows := mock.NewRowsWithColumnDefinition(
		sqlmock.NewColumn("BIN").OfType("VARBINARY", []byte{}),
		sqlmock.NewColumn("TXT").OfType("NVARCHAR", []byte{}),
		sqlmock.NewColumn("AMT").OfType("DECIMAL", gohdb.Decimal{}).WithPrecisionAndScale(10, 0),
	).AddRow(
		[]byte("abc"), // valid UTF-8 bytes in a binary column must still be treated as binary
		[]byte("héllo"),
		ratOf(t, "5"),
	)
	mock.ExpectQuery("SELECT * FROM T").WillReturnRows(rows)

	batch, _, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 1)

	body, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.JSONEq(t, `{"BIN":"YWJj","TXT":"héllo","AMT":5}`, string(body),
		"binary columns base64-encode, text columns are strings, DECIMAL(10,0) is an integer")
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
	resumed, err := s2.loadCheckpoint(t.Context())
	require.NoError(t, err)
	require.True(t, resumed, "a persisted incrementing HWM must be reported as resumed so the initial value is not re-applied")
	require.Equal(t, int64(2), s2.hwm, "restarted input must resume from the persisted HWM")
}

// TestSAPHANAInputBinaryIncrementingKey: a VARBINARY incrementing_column (a
// monotonic ULID is the realistic case) arrives as []byte. The peek-ahead at a
// fetch_size boundary compares two HWM values, and comparing two interfaces
// holding []byte with == panics, so the comparison must be type-aware. The
// key must also survive the checkpoint round trip, otherwise every restart
// re-reads the table.
func TestSAPHANAInputBinaryIncrementingKey(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
poll_interval: 1ms
fetch_size: 1
max_retries: 0
checkpoint_cache: %s
`, testCacheName))

	// The driver reports the column as VARBINARY, which is what keeps the
	// value a []byte through normalisation (bytes under a text-typed column
	// that happen to be valid UTF-8 would become a string).
	k1, k2 := []byte{0x01, 0x00}, []byte{0x01, 0x01}
	idCol := sqlmock.NewColumn("ID").OfType("VARBINARY", []byte{})
	mock.ExpectQuery(`SELECT * FROM "T" ORDER BY "ID"`).
		WillReturnRows(sqlmock.NewRowsWithColumnDefinition(idCol).AddRow(k1).AddRow(k2))

	batch1, ack1, err := s.ReadBatch(t.Context())
	require.NoError(t, err, "the peek-ahead must not panic on a []byte key")
	require.Len(t, batch1, 1)
	batch2, ack2, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch2, 1)

	require.NoError(t, ack1(t.Context(), nil))
	require.NoError(t, ack2(t.Context(), nil))
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp, "a []byte HWM must be persisted like any other key type")
	require.Equal(t, k2, cp.IncrHWMBytes)

	// The next poll binds the []byte HWM as bytes, and a restart resumes from it.
	mock.ExpectQuery(testIncQuery).WithArgs(k2).WillReturnError(errors.New("stop here"))
	_, _, err = s.ReadBatch(t.Context())
	require.ErrorContains(t, err, "stop here")
	require.NoError(t, mock.ExpectationsWereMet())

	s2, err := newSAPHANAInput(parseInputConf(t, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
checkpoint_cache: %s
`, testCacheName)), res)
	require.NoError(t, err)
	resumed, err := s2.loadCheckpoint(t.Context())
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, k2, s2.hwm)
}

// Two inputs that share a checkpoint cache and the default key would resume
// from each other's HWM, skipping rows or failing every poll on a type
// mismatch. The persisted state names its table, so a different table must
// refuse it on connect.
func TestSAPHANAInputCheckpointRejectsAnotherTablesState(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s1, mock := newTestIncInput(t, res, 10)
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(int64(1)))

	_, ack, err := s1.ReadBatch(t.Context())
	require.NoError(t, err)
	require.NoError(t, ack(t.Context(), nil))
	require.Equal(t, `"T"`, readCheckpoint(t, res).Source)

	conf := parseInputConf(t, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: OTHER
incrementing_column: ID
checkpoint_cache: %s
`, testCacheName))
	s2, err := newSAPHANAInput(conf, res)
	require.NoError(t, err)
	_, err = s2.loadCheckpoint(t.Context())
	require.ErrorContains(t, err, "checkpoint_cache_key")
}

// A checkpoint written before the state carried its table (v4.114.0) has no
// source; it must still load so upgrading never loses progress.
func TestSAPHANAInputCheckpointAcceptsStateWithoutSource(t *testing.T) {
	res := enterpriseResourcesWithCache()
	require.NoError(t, res.AccessCache(t.Context(), testCacheName, func(c service.Cache) {
		require.NoError(t, c.Set(t.Context(), "sap_hana_hwm", []byte(`{"incr_hwm_int":7}`), nil))
	}))
	s, _ := newTestIncInput(t, res, 10)
	resumed, err := s.loadCheckpoint(t.Context())
	require.NoError(t, err)
	require.True(t, resumed)
	require.Equal(t, int64(7), s.hwm)
}

// A batch whose checkpoint registration fails (the context ends while the
// tracker is full at checkpoint_limit) never reaches the framework. Its rows,
// including the one buffered by the peek-ahead, must be read again rather
// than skipped by the next ReadBatch.
func TestSAPHANAInputTrackFailureRewindsPosition(t *testing.T) {
	res := enterpriseResourcesWithCache()
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "0"
poll_interval: 1ms
fetch_size: 2
checkpoint_limit: 1
max_retries: 0
checkpoint_cache: %s
`, testCacheName))

	rows := sqlmock.NewRows([]string{"ID"})
	for i := int64(1); i <= 5; i++ {
		rows.AddRow(i)
	}
	mock.ExpectQuery(testIncQuery).WithArgs(int64(0)).WillReturnRows(rows)

	t.Log("Given: a first batch delivered and not yet acknowledged, filling checkpoint_limit")
	batch1, ack1, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, intsOf(t, decodeBatch(t, batch1), "ID"))

	t.Log("When: the second ReadBatch blocks on the full tracker and its context ends")
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = s.ReadBatch(cancelled)
	require.ErrorIs(t, err, context.Canceled)

	t.Log("Then: after the first batch is acknowledged the next read re-delivers rows 3 and 4")
	require.NoError(t, ack1(t.Context(), nil))
	mock.ExpectQuery(testIncQuery).WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(int64(3)).AddRow(int64(4)).AddRow(int64(5)))
	batch2, ack2, err := s.ReadBatch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []int64{3, 4}, intsOf(t, decodeBatch(t, batch2), "ID"))
	require.NoError(t, ack2(t.Context(), nil))
	cp := readCheckpoint(t, res)
	require.NotNil(t, cp)
	require.NotNil(t, cp.IncrHWMInt)
	assert.Equal(t, int64(4), *cp.IncrHWMInt)
}

func decodeBatch(t *testing.T, batch service.MessageBatch) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(batch))
	for _, msg := range batch {
		out = append(out, decodeRow(t, msg))
	}
	return out
}

// Pure timestamp mode checkpoints mid-window once a batch ends on a completed
// timestamp (the peeked next row is later), so a restart resumes with
// `ts > T` instead of re-reading the whole window. A tie group spanning the
// batch boundary keeps the previous checkpoint.
func TestSAPHANAInputTimestampCheckpointsCompletedTimestamp(t *testing.T) {
	conf := fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
poll_interval: 1ms
fetch_size: 2
max_retries: 0
timestamp_delay: 0s
checkpoint_cache: %s
`, testCacheName)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1, t2, t3 := base.Add(time.Second), base.Add(2*time.Second), base.Add(3*time.Second)
	dbNow := base.Add(time.Hour)

	t.Run("batch ends on a completed timestamp", func(t *testing.T) {
		res := enterpriseResourcesWithCache()
		s, mock := newTestInput(t, res, conf)
		t.Log("Given: six rows over three timestamps, two per timestamp, fetch_size 2")
		mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
			WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
		mock.ExpectQuery(testTSFirstQuery).WithArgs(dbNow).
			WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).
				AddRow(t1, int64(1)).AddRow(t1, int64(2)).
				AddRow(t2, int64(3)).AddRow(t2, int64(4)).
				AddRow(t3, int64(5)).AddRow(t3, int64(6)))

		t.Log("When: the first batch is acknowledged and the input restarts")
		batch, ack, err := s.ReadBatch(t.Context())
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2}, intsOf(t, decodeBatch(t, batch), "ID"))
		require.NoError(t, ack(t.Context(), nil))
		cp := readCheckpoint(t, res)
		require.NotNil(t, cp)
		require.NotNil(t, cp.TimestampHWM)
		assert.True(t, t1.Equal(*cp.TimestampHWM), "checkpoint must be the completed timestamp, got %v", *cp.TimestampHWM)

		s2, mock2 := newTestInput(t, res, conf)
		_, err = s2.loadCheckpoint(t.Context())
		require.NoError(t, err)

		t.Log("Then: the restarted input resumes after t1 and re-delivers none of its rows")
		mock2.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
			WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
		mock2.ExpectQuery(testTSWindowQuery).WithArgs(t1, dbNow).
			WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).
				AddRow(t2, int64(3)).AddRow(t2, int64(4)).
				AddRow(t3, int64(5)).AddRow(t3, int64(6)))
		var ids []int64
		for len(ids) < 4 {
			batch, ack, err := s2.ReadBatch(t.Context())
			require.NoError(t, err)
			require.NoError(t, ack(t.Context(), nil))
			ids = append(ids, intsOf(t, decodeBatch(t, batch), "ID")...)
		}
		assert.Equal(t, []int64{3, 4, 5, 6}, ids)
		require.NoError(t, mock2.ExpectationsWereMet())
		cp = readCheckpoint(t, res)
		require.NotNil(t, cp.TimestampHWM)
		assert.True(t, dbNow.Equal(*cp.TimestampHWM), "a consumed window checkpoints at its bound, got %v", *cp.TimestampHWM)
	})

	t.Run("tie group spans the batch boundary", func(t *testing.T) {
		res := enterpriseResourcesWithCache()
		s, mock := newTestInput(t, res, conf)
		t.Log("Given: three rows sharing t1, then one at t2 and one at t3, fetch_size 2")
		mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
			WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
		mock.ExpectQuery(testTSFirstQuery).WithArgs(dbNow).
			WillReturnRows(sqlmock.NewRows([]string{"TS", "ID"}).
				AddRow(t1, int64(1)).AddRow(t1, int64(2)).AddRow(t1, int64(3)).
				AddRow(t2, int64(4)).AddRow(t3, int64(5)))

		t.Log("When: the first batch, ending inside the t1 group, is acknowledged")
		_, ack, err := s.ReadBatch(t.Context())
		require.NoError(t, err)
		require.NoError(t, ack(t.Context(), nil))

		t.Log("Then: no timestamp is checkpointed, since t1 is not complete")
		cp := readCheckpoint(t, res)
		require.NotNil(t, cp)
		assert.Nil(t, cp.TimestampHWM)

		t.Log("And: the next batch (rows 3 and 4) ends on t2, which the peeked t3 row completes")
		batch, ack, err := s.ReadBatch(t.Context())
		require.NoError(t, err)
		require.Equal(t, []int64{3, 4}, intsOf(t, decodeBatch(t, batch), "ID"))
		require.NoError(t, ack(t.Context(), nil))
		cp = readCheckpoint(t, res)
		require.NotNil(t, cp.TimestampHWM)
		assert.True(t, t2.Equal(*cp.TimestampHWM), "got %v", *cp.TimestampHWM)
	})
}

// gaugeExporter records the latest value of every gauge by name. benthos
// re-invokes the ctor on each update, so values live on the exporter.
type gaugeExporter struct {
	mu     sync.Mutex
	gauges map[string]int64
}

func (*gaugeExporter) NewCounterCtor(string, ...string) service.MetricsExporterCounterCtor {
	return func(...string) service.MetricsExporterCounter { return noopExporterMetric{} }
}

func (*gaugeExporter) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(...string) service.MetricsExporterTimer { return noopExporterMetric{} }
}

func (e *gaugeExporter) NewGaugeCtor(name string, _ ...string) service.MetricsExporterGaugeCtor {
	return func(...string) service.MetricsExporterGauge { return gaugeHandle{e: e, name: name} }
}

func (*gaugeExporter) Close(context.Context) error { return nil }

func (e *gaugeExporter) set(name string, v int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gauges[name] = v
}

func (e *gaugeExporter) get(name string) (int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.gauges[name]
	return v, ok
}

type gaugeHandle struct {
	e    *gaugeExporter
	name string
}

func (g gaugeHandle) Set(v int64) {
	g.e.set(g.name, v)
}

type noopExporterMetric struct{}

func (noopExporterMetric) Incr(int64)          {}
func (noopExporterMetric) IncrFloat64(float64) {}
func (noopExporterMetric) Timing(int64)        {}

// resourcesWithGauges builds licensed resources holding the test cache, with
// metrics routed to a gaugeExporter.
func resourcesWithGauges(t *testing.T) (*service.Resources, *gaugeExporter) {
	t.Helper()
	exp := &gaugeExporter{gauges: map[string]int64{}}
	env := service.NewEnvironment()
	require.NoError(t, env.RegisterMetricsExporter("sap_hana_test_gauges", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) { return exp, nil }))
	b := env.NewResourceBuilder()
	require.NoError(t, b.SetMetricsYAML("sap_hana_test_gauges: {}"))
	require.NoError(t, b.AddCacheYAML(fmt.Sprintf("label: %s\nmemory: {}", testCacheName)))
	res, closeFn, err := b.Build()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn(context.Background()) })
	license.InjectTestService(res)
	return res, exp
}

// The pending-messages gauge shows batches awaiting acks (a ReadBatch blocked
// at checkpoint_limit looks idle otherwise), and the lag gauge shows how far
// the checkpointed timestamp trails the database clock.
func TestSAPHANAInputLagAndPendingGauges(t *testing.T) {
	res, exp := resourcesWithGauges(t)
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
poll_interval: 1ms
fetch_size: 2
max_retries: 0
timestamp_delay: 30s
checkpoint_cache: %s
`, testCacheName))

	t.Log("Given: a window whose bound is the database clock minus 30s, and rows ten minutes old")
	upper := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// The host clock runs an hour behind the database clock; only the offset
	// read at each poll matters.
	hostNow := upper.Add(-time.Hour)
	s.cp.now = func() time.Time { return hostNow }
	t1, t2, t3 := upper.Add(-11*time.Minute), upper.Add(-10*time.Minute), upper.Add(-9*time.Minute)
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(-30)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(upper))
	mock.ExpectQuery(testTSFirstQuery).WithArgs(upper).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(t1).AddRow(t2).AddRow(t3))

	t.Log("When: a batch is read")
	_, ack, err := s.ReadBatch(t.Context())
	require.NoError(t, err)

	t.Log("Then: it is pending until acknowledged, and the ack sets the lag")
	pending, ok := exp.get("sap_hana_checkpoint_pending_messages")
	require.True(t, ok)
	assert.Equal(t, int64(2), pending)
	_, ok = exp.get("sap_hana_hwm_lag_seconds")
	assert.False(t, ok, "no lag before a checkpoint resolves")

	require.NoError(t, ack(t.Context(), nil))
	pending, _ = exp.get("sap_hana_checkpoint_pending_messages")
	assert.Equal(t, int64(0), pending)
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	// Checkpoint is t2 (the peeked t3 is later); database now is upper+30s.
	assert.Equal(t, int64((10*time.Minute+30*time.Second)/time.Second), lag)

	t.Log("When: five minutes pass with no poll and no ack, as when ReadBatch is blocked at checkpoint_limit")
	hostNow = hostNow.Add(5 * time.Minute)
	s.cp.refreshLag()

	t.Log("Then: the lag keeps growing with the estimated database clock")
	lag, _ = exp.get("sap_hana_hwm_lag_seconds")
	assert.Equal(t, int64((15*time.Minute+30*time.Second)/time.Second), lag)
}

// The lag refresh loop recomputes the gauge on its own, so a stalled pipeline
// shows a growing lag without any poll or ack.
func TestSAPHANAInputLagRefreshLoop(t *testing.T) {
	res, exp := resourcesWithGauges(t)
	c := newCheckpointer(res, &sapHANAInputConfig{checkpointLimit: 10})
	var mu sync.Mutex
	hostNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return hostNow
	}

	t.Log("Given: a poll that read the database clock and a checkpoint one minute behind it")
	c.observeDBClock(hostNow)
	hwm := hostNow.Add(-time.Minute)
	c.recordLag(&sapHANACheckpointState{TimestampHWM: &hwm})
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	assert.Equal(t, int64(60), lag)

	t.Log("When: the refresh loop runs while the host clock moves on by an hour")
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	c.startLagRefresh(stop)
	mu.Lock()
	hostNow = hostNow.Add(time.Hour)
	mu.Unlock()

	t.Log("Then: the gauge catches up without a poll or an ack")
	assert.Eventually(t, func() bool {
		lag, _ := exp.get("sap_hana_hwm_lag_seconds")
		return lag == int64((time.Hour+time.Minute)/time.Second)
	}, 5*time.Second, 50*time.Millisecond)
}

// The lag gauge follows the persisted checkpoint: when the cache write fails,
// a restart resumes from the older position, so the gauge must not move.
func TestSAPHANAInputLagIgnoresFailedPersist(t *testing.T) {
	res, exp := resourcesWithGauges(t)
	s, _ := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
checkpoint_cache: %s
`, testCacheName))
	c := s.cp
	hostNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return hostNow }
	c.observeDBClock(hostNow)

	t.Log("Given: a checkpoint one minute behind the database clock, persisted")
	older := hostNow.Add(-time.Minute)
	ack, err := c.track(t.Context(), 1, int64(1), older)
	require.NoError(t, err)
	require.NoError(t, ack(t.Context(), nil))
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	assert.Equal(t, int64(60), lag)

	t.Log("When: a later checkpoint resolves but the cache write fails")
	c.cacheName = "missing_cache"
	newer := hostNow.Add(-time.Second)
	ack, err = c.track(t.Context(), 1, int64(2), newer)
	require.NoError(t, err)
	require.NoError(t, ack(t.Context(), nil))

	t.Log("Then: the gauge still shows the persisted checkpoint")
	lag, _ = exp.get("sap_hana_hwm_lag_seconds")
	assert.Equal(t, int64(60), lag)
}

// After a restart, the lag gauge starts from the loaded checkpoint, so a
// restart into a stall (downstream or cache down, no ack ever resolves) shows
// a growing lag instead of no lag.
func TestSAPHANAInputLagSeededFromLoadedCheckpoint(t *testing.T) {
	res, exp := resourcesWithGauges(t)
	dbNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cpHWM := dbNow.Add(-10 * time.Minute)
	raw, err := json.Marshal(&sapHANACheckpointState{Source: `"T"`, TimestampHWM: &cpHWM})
	require.NoError(t, err)
	require.NoError(t, res.AccessCache(t.Context(), testCacheName, func(c service.Cache) {
		require.NoError(t, c.Set(t.Context(), "sap_hana_hwm", raw, nil))
	}))

	t.Log("Given: a restarted input that loads a checkpoint ten minutes behind the database clock")
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 0s
checkpoint_cache: %s
`, testCacheName))
	// The host clock runs an hour behind the database clock.
	hostNow := dbNow.Add(-time.Hour)
	s.cp.now = func() time.Time { return hostNow }
	_, err = s.loadCheckpoint(t.Context())
	require.NoError(t, err)
	_, ok := exp.get("sap_hana_hwm_lag_seconds")
	assert.False(t, ok, "no lag before a poll reads the database clock")

	t.Log("When: a poll delivers a batch that is never acknowledged")
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSWindowQuery).WithArgs(cpHWM, dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(cpHWM.Add(time.Minute)))
	_, _, err = s.ReadBatch(t.Context())
	require.NoError(t, err)

	t.Log("Then: the lag is the database clock minus the loaded checkpoint")
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	assert.Equal(t, int64((10*time.Minute)/time.Second), lag)

	t.Log("When: five minutes pass with no poll and no ack")
	hostNow = hostNow.Add(5 * time.Minute)
	s.cp.refreshLag()

	t.Log("Then: the lag keeps growing")
	lag, _ = exp.get("sap_hana_hwm_lag_seconds")
	assert.Equal(t, int64((15*time.Minute)/time.Second), lag)
}

// Without a loaded checkpoint, the lag gauge starts from
// timestamp_initial_value, the position a restart would resume from.
func TestSAPHANAInputLagSeededFromTimestampInitialValue(t *testing.T) {
	res, exp := resourcesWithGauges(t)

	t.Log("Given: a first start with an empty checkpoint cache and an initial value one hour behind the database clock")
	s, mock := newTestInput(t, res, fmt.Sprintf(`
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
timestamp_initial_value: "2026-01-01T11:00:00Z"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 0s
checkpoint_cache: %s
`, testCacheName))
	dbNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	initial := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	hostNow := dbNow
	s.cp.now = func() time.Time { return hostNow }
	_, err := s.loadCheckpoint(t.Context())
	require.NoError(t, err)

	t.Log("When: a poll delivers a batch that is never acknowledged")
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSWindowQuery).WithArgs(initial, dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(initial.Add(time.Minute)))
	_, _, err = s.ReadBatch(t.Context())
	require.NoError(t, err)

	t.Log("Then: the lag is the database clock minus the initial value")
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	assert.Equal(t, int64(time.Hour/time.Second), lag)
}

// Without checkpoint_cache, the lag gauge also starts from
// timestamp_initial_value, the in-memory position before any ack, and then
// follows the acknowledged in-memory position.
func TestSAPHANAInputLagSeededWithoutCheckpointCache(t *testing.T) {
	res, exp := resourcesWithGauges(t)

	t.Log("Given: an input without checkpoint_cache and an initial value one hour behind the database clock")
	s, mock := newTestInput(t, res, `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: T
timestamp_column: TS
timestamp_initial_value: "2026-01-01T11:00:00Z"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
timestamp_delay: 0s
`)
	dbNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	initial := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	hostNow := dbNow
	s.cp.now = func() time.Time { return hostNow }
	_, err := s.loadCheckpoint(t.Context())
	require.NoError(t, err)

	t.Log("When: a poll delivers a batch that is not acknowledged yet")
	mock.ExpectQuery(hanaClockQuery).WithArgs(float64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"NOW"}).AddRow(dbNow))
	mock.ExpectQuery(testTSWindowQuery).WithArgs(initial, dbNow).
		WillReturnRows(sqlmock.NewRows([]string{"TS"}).AddRow(initial.Add(time.Minute)))
	_, ack, err := s.ReadBatch(t.Context())
	require.NoError(t, err)

	t.Log("Then: the lag is the database clock minus the initial value")
	lag, ok := exp.get("sap_hana_hwm_lag_seconds")
	require.True(t, ok)
	assert.Equal(t, int64(time.Hour/time.Second), lag)

	t.Log("When: the batch is acknowledged")
	require.NoError(t, ack(t.Context(), nil))

	t.Log("Then: the lag follows the acknowledged window bound, not the initial value")
	lag, _ = exp.get("sap_hana_hwm_lag_seconds")
	assert.Equal(t, int64(0), lag)
}
