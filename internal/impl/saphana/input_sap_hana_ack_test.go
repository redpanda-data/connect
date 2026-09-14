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

// incrementing_initial_value is a string in YAML but is bound as a query
// parameter, and go-hdb rejects mismatched Go types client-side, so it must
// be coerced to the incrementing column's actual type.
func TestCoerceIncrementingValue(t *testing.T) {
	tests := []struct {
		raw      string
		dataType string
		want     any
		wantErr  bool
	}{
		{raw: "000100", dataType: "NVARCHAR", want: "000100"},
		{raw: "100", dataType: "VARCHAR", want: "100"},
		{raw: "100", dataType: "BIGINT", want: int64(100)},
		{raw: "7", dataType: "INTEGER", want: int64(7)},
		{raw: "12.5", dataType: "DECIMAL", want: 12.5},
		{raw: "12.5", dataType: "DOUBLE", want: 12.5},
		{raw: "2024-01-01T00:00:00Z", dataType: "TIMESTAMP", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01 10:30:00", dataType: "TIMESTAMP", want: time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC)},
		{raw: "2024-01-01", dataType: "DATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "2024-01-01T00:00:00Z", dataType: "SECONDDATE", want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{raw: "abc", dataType: "BIGINT", wantErr: true},
		{raw: "yesterday", dataType: "TIMESTAMP", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.dataType+"/"+tc.raw, func(t *testing.T) {
			got, err := coerceIncrementingValue(tc.raw, tc.dataType)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The column type comes from the catalog at connect time; a zero-padded
// NVARCHAR key must not be coerced to an integer just because it parses.
func TestSAPHANAInputResolvesInitialValueFromColumnType(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
schema_name: S
table: T
incrementing_column: DOC_NO
incrementing_initial_value: "000100"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
`)
	require.Equal(t, int64(100), s.hwm, "constructor heuristic guesses integer before the catalog is consulted")

	mock.ExpectQuery(hanaColumnTypeQuery).WithArgs("S", "T", "DOC_NO").
		WillReturnRows(sqlmock.NewRows([]string{"DATA_TYPE_NAME"}).AddRow("NVARCHAR"))
	require.NoError(t, s.resolveIncrementingInitialValue(t.Context()))
	assert.Equal(t, "000100", s.hwm)

	mock.ExpectQuery(`SELECT * FROM "S"."T" WHERE "DOC_NO" > ? ORDER BY "DOC_NO"`).WithArgs("000100").
		WillReturnRows(sqlmock.NewRows([]string{"DOC_NO"}))
	_, _, err := s.ReadBatch(t.Context())
	require.Error(t, err) // the poll after the empty one has no expectation
	require.NoError(t, mock.ExpectationsWereMet())
}

// When the catalog lookup fails the heuristic value is kept (with a warning)
// rather than failing the connection outright.
func TestSAPHANAInputInitialValueFallsBackWhenCatalogUnavailable(t *testing.T) {
	s, mock := newTestInput(t, enterpriseResourcesWithCache(), `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: T
incrementing_column: ID
incrementing_initial_value: "100"
poll_interval: 1ms
fetch_size: 10
max_retries: 0
`)
	mock.ExpectQuery(hanaColumnTypeCurrentSchemaQuery).WithArgs("T", "ID").
		WillReturnError(errors.New("insufficient privilege"))
	require.NoError(t, s.resolveIncrementingInitialValue(t.Context()))
	assert.Equal(t, int64(100), s.hwm)
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
