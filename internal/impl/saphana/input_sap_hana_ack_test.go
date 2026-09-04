// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

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

// newTestIncInput builds an incrementing-mode input wired to a sqlmock DB,
// mirroring the state Connect would establish (hwmSafe seeded from hwm).
func newTestIncInput(t *testing.T, res *service.Resources, fetchSize int) (*sapHANAInput, sqlmock.Sqlmock) {
	t.Helper()
	conf := parseInputConf(t, fmt.Sprintf(`
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
	s, err := newSAPHANAInput(conf, res)
	require.NoError(t, err)

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s.db = db
	s.schemas = newSchemaCache(db, s.log)
	s.hwmSafe = s.hwm
	return s, mock
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
