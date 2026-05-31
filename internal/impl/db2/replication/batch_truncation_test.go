// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPollChangesBatchTruncationSafety verifies the watermark safety rule:
// when any change table returns exactly PollBatchSize rows the batch may have
// been cut in the middle of a transaction. The connector must NOT advance the
// checkpoint past the last CSN seen because the remaining rows in that
// transaction would be permanently skipped by the "COMMITSEQ > afterCSN"
// predicate on the next poll.
//
// Scenario: PollBatchSize=2, change table returns exactly 2 rows at CSN=10 and
// CSN=20. The last CSN (20) is trimmed; only the row at CSN=10 is delivered
// and maxCSN = 10. The next poll re-fetches from afterCSN=10 so CSN=20 is seen.
func TestPollChangesBatchTruncationSafety(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	cols := []string{
		"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
	}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}},
				}, nil
			}
			// Exactly PollBatchSize (2) rows — triggers truncation logic.
			return cols, [][]driver.Value{
				{[]byte{0, 0, 0, 0, 0, 0, 0, 10}, int64(1), "I", ts, int64(1)},
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(1), "I", ts, int64(2)},
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 2, // batch exactly fills → truncation kicks in
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)

	require.NoError(t, err)
	// Only the first event (CSN=10) is delivered; CSN=20 is trimmed for re-fetch.
	require.Len(t, events, 1, "batch-full: events at the last CSN must be trimmed")
	assert.Equal(t, uint64(10), events[0].CSN.Uint64())
	assert.Equal(t, uint64(10), maxCSN.Uint64(),
		"maxCSN must be the penultimate distinct CSN, not the trimmed last CSN")
}

// TestPollChangesBatchTruncationRetrimMultipleRows verifies that when multiple
// rows share the highest CSN in a full batch, ALL of them are trimmed together.
// Only rows with strictly lower CSN values are delivered.
//
// Scenario: PollBatchSize=3, rows at CSN=10, CSN=20, CSN=20. Both CSN=20 rows
// form the D+I pair of an UPDATE and must be trimmed together — splitting them
// would deliver a dangling delete with no matching insert. Only CSN=10 is
// delivered; maxCSN = 10.
func TestPollChangesBatchTruncationRetrimMultipleRows(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	cols := []string{
		"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
	}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}},
				}, nil
			}
			// Exactly PollBatchSize (3) rows; last two share CSN=20 (a D+I UPDATE pair).
			return cols, [][]driver.Value{
				{[]byte{0, 0, 0, 0, 0, 0, 0, 10}, int64(1), "I", ts, int64(1)},
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(1), "D", ts, int64(2)}, // before-image
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(2), "I", ts, int64(2)}, // after-image
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 3,
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)

	require.NoError(t, err)
	// Both CSN=20 rows (the D+I pair) are trimmed together so the pair is never split.
	require.Len(t, events, 1, "both rows at the last CSN must be trimmed when batch is full")
	assert.Equal(t, uint64(10), events[0].CSN.Uint64())
	assert.Equal(t, uint64(10), maxCSN.Uint64())
}

// TestPollChangesBatchTruncationAllSameCSN verifies the edge-case fallback:
// when every event in a full batch shares the same CSN (a single very large
// transaction exceeding PollBatchSize), no penultimate CSN exists. The connector
// must return all events and advance past that CSN, accepting at-least-once
// delivery semantics.
//
// Without this fallback the connector would stall permanently when a single
// transaction produces more change rows than PollBatchSize.
func TestPollChangesBatchTruncationAllSameCSN(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 42}
	cols := []string{
		"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
	}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}},
				}, nil
			}
			// All 3 rows share the same CSN — single oversized transaction.
			return cols, [][]driver.Value{
				{sharedCSN, int64(1), "I", ts, int64(1)},
				{sharedCSN, int64(2), "I", ts, int64(2)},
				{sharedCSN, int64(3), "I", ts, int64(3)},
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 3, // exactly full; all rows share one CSN
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)

	require.NoError(t, err)
	// All 3 events must be returned so the connector can make progress.
	require.Len(t, events, 3,
		"oversized single-transaction batch must deliver all events to avoid stall")
	assert.Equal(t, uint64(42), maxCSN.Uint64(),
		"maxCSN must equal the shared CSN so the next poll starts after it")
}

// TestPollChangesNoBatchTruncationWhenUnderLimit verifies that batch truncation
// is NOT applied when the change table returns fewer rows than PollBatchSize.
// All events must be delivered and maxCSN must equal the highest CSN seen.
func TestPollChangesNoBatchTruncationWhenUnderLimit(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	cols := []string{
		"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
	}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}},
				}, nil
			}
			// 2 rows when PollBatchSize=5 — batch is NOT full.
			return cols, [][]driver.Value{
				{[]byte{0, 0, 0, 0, 0, 0, 0, 10}, int64(1), "I", ts, int64(1)},
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(1), "I", ts, int64(2)},
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 5, // batch NOT full → no truncation applied
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)

	require.NoError(t, err)
	require.Len(t, events, 2, "under-limit batch must not be truncated")
	assert.Equal(t, uint64(10), events[0].CSN.Uint64())
	assert.Equal(t, uint64(20), events[1].CSN.Uint64())
	assert.Equal(t, uint64(20), maxCSN.Uint64(),
		"maxCSN must be the highest CSN when batch is under the limit")
}
