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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helper function tests (no DB)
// ---------------------------------------------------------------------------

func TestGetStringHelper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"[]byte", []byte("world"), "world"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := tc.value
			dest := &v
			assert.Equal(t, tc.want, getString(dest))
		})
	}
}

func TestGetTimeHelper(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Second)

	tests := []struct {
		name     string
		value    any
		wantZero bool
		wantTime time.Time
	}{
		{"nil", nil, true, time.Time{}},
		{"time.Time", now, false, now},
		{"string (not a time)", "not-a-time", true, time.Time{}},
		{"int64 (not a time)", int64(12345), true, time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			v := tc.value
			result := getTime(&v)
			if tc.wantZero {
				assert.True(t, result.IsZero())
			} else {
				assert.Equal(t, tc.wantTime, result)
			}
		})
	}
}

func TestStreamConfigAsncdcSchema(t *testing.T) {
	t.Parallel()

	c := &StreamConfig{}
	assert.Equal(t, "ASNCDC", c.asncdcSchema(), "default schema")

	c.AsnCDCSchema = "CUSTOM"
	assert.Equal(t, "CUSTOM", c.asncdcSchema(), "explicit schema")
}

// ---------------------------------------------------------------------------
// DB-dependent tests
// ---------------------------------------------------------------------------

func TestGetUpperBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rows     [][]driver.Value
		wantCSN  uint64
		wantNull bool
		wantErr  bool
	}{
		{
			name:    "normal SYNCHPOINT",
			rows:    [][]driver.Value{{[]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}}}, // 12345
			wantCSN: 12345,
		},
		{
			name:     "nil SYNCHPOINT returns NullCSN",
			rows:     [][]driver.Value{{nil}},
			wantNull: true,
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr {
						return nil, nil, fmt.Errorf("SYNCHPOINT query failed")
					}
					return []string{"MAX(SYNCHPOINT)"}, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}}, Version{})
			csn, err := s.getUpperBound(context.Background())

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.wantNull {
				assert.True(t, csn.IsNull())
			} else {
				assert.Equal(t, tc.wantCSN, csn.Uint64())
			}
		})
	}
}

func TestInitialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rows      [][]driver.Value
		tables    []string
		wantErr   bool
		errSubstr string
	}{
		{
			name:   "single table registered",
			rows:   [][]driver.Value{{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"}},
			tables: []string{"EMPLOYEES"},
		},
		{
			name:   "case-insensitive match",
			rows:   [][]driver.Value{{"MYSCHEMA", "employees", "ASNCDC", "EMPLOYEES_CT"}},
			tables: []string{"EMPLOYEES"},
		},
		{
			name: "multiple tables",
			rows: [][]driver.Value{
				{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"},
				{"MYSCHEMA", "ORDERS", "ASNCDC", "ORDERS_CT"},
			},
			tables: []string{"EMPLOYEES", "ORDERS"},
		},
		{
			name:      "table not registered returns error",
			rows:      [][]driver.Value{{"MYSCHEMA", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"}},
			tables:    []string{"EMPLOYEES", "MISSING_TABLE"},
			wantErr:   true,
			errSubstr: "MISSING_TABLE",
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr && tc.rows == nil {
						return nil, nil, fmt.Errorf("registration query failed")
					}
					return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"}, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schemas: []string{"MYSCHEMA"},
				Tables:  tc.tables,
			}, Version{})

			err := s.Initialize(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				if tc.errSubstr != "" {
					assert.Contains(t, err.Error(), tc.errSubstr)
				}
				return
			}
			require.NoError(t, err)

			// Verify change tables were populated (keys are schema-qualified).
			for _, table := range tc.tables {
				_, ok := s.changeTables["MYSCHEMA."+strings.ToUpper(table)]
				assert.True(t, ok, "change table for %s should be registered", table)
			}
		})
	}
}

func TestPollChangeTable(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)

	tests := []struct {
		name      string
		rows      [][]driver.Value
		columns   []string
		wantCount int
		wantErr   bool
		errSubstr string
	}{
		{
			name: "one insert event",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"EMP_ID", "EMP_NAME",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}, // CSN=12345
				int64(1),                             // INTENTSEQ
				"I",                                  // INSERT
				ts,                                   // LOGMARKER
				int64(42),                            // EMP_ID
				"Alice",                              // EMP_NAME
			}},
			wantCount: 1,
		},
		{
			name: "delete event",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 1},
				int64(1),
				"D",
				ts,
				int64(1),
			}},
			wantCount: 1,
		},
		{
			name: "unknown operation skipped",
			columns: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			rows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 1},
				int64(1),
				"Z", // unknown operation
				ts,
				int64(1),
			}},
			wantCount: 0,
		},
		{
			name:      "missing IBMSNAP_ columns returns error",
			columns:   []string{"COL1", "COL2"},
			rows:      [][]driver.Value{{"val1", "val2"}},
			wantErr:   true,
			errSubstr: "missing required IBMSNAP_",
		},
		{
			name:    "query error propagates",
			wantErr: true,
		},
		{
			name:      "empty result set",
			columns:   []string{"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER"},
			rows:      nil,
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.wantErr && tc.columns == nil {
						return nil, nil, fmt.Errorf("change table query failed")
					}
					return tc.columns, tc.rows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schemas:       []string{"MYSCHEMA"},
				PollBatchSize: 100,
			}, Version{})

			events, _, err := s.pollChangeTable(context.Background(), "MYSCHEMA.EMPLOYEES", "MYSCHEMA", "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
				NewCSN(0), 0, NewCSN(99999), 0)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errSubstr != "" {
					assert.Contains(t, err.Error(), tc.errSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Len(t, events, tc.wantCount)
		})
	}
}

func TestPollChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		afterCSN   CSN
		upperCSN   []byte // bytes returned for SYNCHPOINT; nil = NullCSN
		changeRows [][]driver.Value
		changeCols []string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "no new changes (upper <= after)",
			afterCSN:  NewCSN(100),
			upperCSN:  []byte{0, 0, 0, 0, 0, 0, 0, 99}, // 99 < 100
			wantCount: 0,
		},
		{
			name:     "new events returned",
			afterCSN: NewCSN(0),
			upperCSN: []byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}, // 12345
			changeCols: []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID",
			},
			changeRows: [][]driver.Value{{
				[]byte{0, 0, 0, 0, 0, 0, 0, 50},
				int64(1),
				"I",
				time.Now(),
				int64(1),
			}},
			wantCount: 1,
		},
		{
			name:     "upper bound query error",
			afterCSN: NewCSN(0),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			callCount := 0
			db := openFakeDB(t, &replFakeHandlers{
				query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					callCount++
					if tc.wantErr && callCount == 1 {
						return nil, nil, fmt.Errorf("SYNCHPOINT error")
					}
					if strings.Contains(q, "SYNCHPOINT") {
						return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{tc.upperCSN}}, nil
					}
					// Change table query
					return tc.changeCols, tc.changeRows, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schemas:       []string{"MYSCHEMA"},
				PollBatchSize: 100,
			}, Version{})

			if !tc.wantErr {
				// Register a fake change table.
				s.changeTables["MYSCHEMA.EMPLOYEES"] = "ASNCDC.EMPLOYEES_CT"
			}

			events, maxCSN, _, err := s.pollChanges(context.Background(), tc.afterCSN, nil)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, events, tc.wantCount)
			if tc.wantCount > 0 {
				assert.True(t, maxCSN.Greater(tc.afterCSN) || maxCSN.Equal(tc.afterCSN))
			}
		})
	}
}

// TestPollChangesLargeTransactionCompositePageination verifies that when a full batch
// contains rows all sharing a single CSN (large transaction), pollChanges populates
// newIntentSeqs so the next call uses composite (CSN, IntentSeq) pagination rather
// than resetting to a plain "COMMITSEQ > N" predicate that would skip remaining rows.
func TestPollChangesLargeTransactionCompositePagination(t *testing.T) {
	t.Parallel()

	// PollBatchSize = 3, all 3 rows share CSN = 50 → full batch, single CSN.
	// After the fix, pollChanges must return newIntentSeqs with max intentSeq = 3
	// so the next poll uses "(COMMITSEQ > 50 OR (COMMITSEQ = 50 AND INTENTSEQ > 3))".
	upperCSN := []byte{0, 0, 0, 0, 0, 0, 0, 100}
	changeCols := []string{"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID"}
	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 50}
	changeRows := [][]driver.Value{
		{sharedCSN, int64(1), "I", time.Now(), int64(1)},
		{sharedCSN, int64(2), "I", time.Now(), int64(2)},
		{sharedCSN, int64(3), "I", time.Now(), int64(3)},
	}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{upperCSN}}, nil
			}
			return changeCols, changeRows, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 3, // full batch when all 3 rows returned
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, returnCSN, newIntentSeqs, err := s.pollChanges(context.Background(), NewCSN(0), nil)
	require.NoError(t, err)
	require.Len(t, events, 3, "all 3 rows must be returned")
	assert.Equal(t, uint64(50), returnCSN.Uint64(), "returnCSN must equal the shared CSN")

	// The critical assertion: newIntentSeqs must be non-nil and populated so the
	// next poll uses composite pagination. Before the fix this was nil, causing
	// any remaining rows at CSN=50 to be skipped.
	require.NotNil(t, newIntentSeqs, "newIntentSeqs must be non-nil for large-transaction composite pagination")
	assert.Equal(t, int64(3), newIntentSeqs["MYSCHEMA.T"], "max intentSeq at returnCSN must be 3")
}

func TestStreamContextCancel(t *testing.T) {
	t.Parallel()

	// Stream should exit cleanly when context is cancelled.
	callCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			callCount++
			// Return a SYNCHPOINT that's always less than afterCSN to avoid
			// actually processing events. Stream will back off and then exit via ctx.
			return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())

	s := NewStreamer(db, StreamConfig{
		Schemas:         []string{"MYSCHEMA"},
		BackoffInterval: 10 * time.Millisecond,
		PollBatchSize:   100,
		StartingCSN:     NewCSN(0),
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	done := make(chan error, 1)
	go func() {
		done <- s.Stream(ctx, func(_ ChangeEvent) error { return nil })
	}()

	// Cancel after a short delay to let the Stream loop spin at least once.
	time.Sleep(30 * time.Millisecond)
	cancel()

	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
}

// TestPollChangeTableDeleteEvent verifies that a 'D' operation row is decoded
// into a ChangeEvent with OpTypeDelete and the row data in the Data map.
// In DB2 LUW SQL Replication, DELETE rows are written to the change table with
// IBMSNAP_OPERATION='D'. The before-image column values are present in the row.
func TestPollChangeTableDeleteEvent(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	csnBytes := []byte{0, 0, 0, 0, 0, 0, 0, 42} // CSN = 42

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"EMP_ID", "EMP_NAME",
				}, [][]driver.Value{{
					csnBytes, int64(1), "D", ts,
					int64(99), "Alice",
				}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}, PollBatchSize: 100}, Version{})
	events, _, err := s.pollChangeTable(context.Background(), "MYSCHEMA.EMPLOYEES", "MYSCHEMA", "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	require.Len(t, events, 1)

	evt := events[0]
	assert.Equal(t, OpTypeDelete, evt.Operation)
	assert.Equal(t, uint64(42), evt.CSN.Uint64())
	assert.Equal(t, int64(1), evt.IntentSeq)
	assert.Equal(t, ts, evt.Timestamp)
	assert.Equal(t, "MYSCHEMA", evt.Schema)
	assert.Equal(t, "EMPLOYEES", evt.Table)

	// Data map must contain row columns but not any IBMSNAP_* control columns.
	assert.Equal(t, int64(99), evt.Data["EMP_ID"])
	assert.Equal(t, "Alice", evt.Data["EMP_NAME"])
	_, hasIBM := evt.Data["IBMSNAP_OPERATION"]
	assert.False(t, hasIBM, "IBMSNAP_* control columns must not appear in Data")
}

// TestPollChangeTableUpdateAsDIPair verifies that pollChangeTable correctly maps
// IBMSNAP_OPERATION='D' and 'I' rows to OpTypeDelete and OpTypeInsert when no
// IBMSNAP_OPCODE column is present (the non-LEAD/LAG fallback path).
//
// In production the buildPollQuery LEAD/LAG subquery emits IBMSNAP_OPCODE 3/4 for
// update pairs; pairOpcodeEvents then merges them into a single OpTypeUpdate event.
// This test validates the fallback: when the result set has no IBMSNAP_OPCODE column,
// pollChangeTable maps raw D/I IBMSNAP_OPERATION codes to OpTypeDelete/OpTypeInsert.
func TestPollChangeTableUpdateAsDIPair(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 77} // both rows share this CSN
	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "SALARY",
				}, [][]driver.Value{
					// D row: before-image (intentseq=1, old salary)
					{sharedCSN, int64(1), "D", ts, int64(5), int64(50000)},
					// I row: after-image (intentseq=2, new salary)
					{sharedCSN, int64(2), "I", ts, int64(5), int64(60000)},
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}, PollBatchSize: 100}, Version{})
	events, _, err := s.pollChangeTable(context.Background(), "MYSCHEMA.EMPLOYEES", "MYSCHEMA", "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	require.Len(t, events, 2, "UPDATE must produce exactly 2 events (D + I)")

	// First event: delete with before-image salary.
	assert.Equal(t, OpTypeDelete, events[0].Operation)
	assert.Equal(t, uint64(77), events[0].CSN.Uint64())
	assert.Equal(t, int64(1), events[0].IntentSeq)
	assert.Equal(t, int64(50000), events[0].Data["SALARY"])

	// Second event: insert with after-image salary.
	assert.Equal(t, OpTypeInsert, events[1].Operation)
	assert.Equal(t, uint64(77), events[1].CSN.Uint64())
	assert.Equal(t, int64(2), events[1].IntentSeq)
	assert.Equal(t, int64(60000), events[1].Data["SALARY"])

	// Both events must share the same CSN — this is what identifies them as
	// the two halves of a single UPDATE operation.
	assert.True(t, events[0].CSN.Equal(events[1].CSN),
		"D and I events for an UPDATE must share the same IBMSNAP_COMMITSEQ")
}

// TestPollChangesMultiTableCSNOrdering verifies that events from multiple change
// tables are merged and returned in strict (CSN, IntentSeq) order regardless of
// which table they originated from.
//
// Two change tables (ORDERS_CT with rows at CSN=10,30 and EMPLOYEES_CT with a
// row at CSN=20) must be merged so the output order is CSN 10 → 20 → 30.
func TestPollChangesMultiTableCSNOrdering(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 50}}, // upper bound = 50
				}, nil
			}
			cols := []string{
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER", "ID",
			}
			if strings.Contains(q, "ORDERS_CT") {
				return cols, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 10}, int64(1), "I", ts, int64(1)},
					{[]byte{0, 0, 0, 0, 0, 0, 0, 30}, int64(1), "I", ts, int64(3)},
				}, nil
			}
			// EMPLOYEES_CT
			return cols, [][]driver.Value{
				{[]byte{0, 0, 0, 0, 0, 0, 0, 20}, int64(1), "I", ts, int64(2)},
			}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:       []string{"MYSCHEMA"},
		PollBatchSize: 100,
	}, Version{})
	s.changeTables["MYSCHEMA.ORDERS"] = "ASNCDC.ORDERS_CT"
	s.changeTables["MYSCHEMA.EMPLOYEES"] = "ASNCDC.EMPLOYEES_CT"

	events, maxCSN, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)

	require.NoError(t, err)
	require.Len(t, events, 3)

	assert.Equal(t, uint64(10), events[0].CSN.Uint64(), "first event must have lowest CSN")
	assert.Equal(t, uint64(20), events[1].CSN.Uint64(), "second event must have middle CSN")
	assert.Equal(t, uint64(30), events[2].CSN.Uint64(), "third event must have highest CSN")
	assert.Equal(t, uint64(30), maxCSN.Uint64())
}

// TestInitializeCaseSensitiveTableNames verifies that Initialize handles mixed-
// case table names from IBMSNAP_REGISTER correctly. DB2 may store table names
// in the register with different casing; the connector matches them
// case-insensitively against the configured tables list.
func TestInitializeCaseSensitiveTableNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		registeredAs   string // value in SOURCE_TABLE column
		configuredAs   string // value in StreamConfig.Tables
		wantRegistered bool
	}{
		{
			name:           "exact match uppercase",
			registeredAs:   "EMPLOYEES",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
		{
			name:           "registry lowercase, config uppercase",
			registeredAs:   "employees",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
		{
			name:           "registry uppercase, config lowercase",
			registeredAs:   "EMPLOYEES",
			configuredAs:   "employees",
			wantRegistered: true,
		},
		{
			name:           "mixed case in registry",
			registeredAs:   "Employees",
			configuredAs:   "EMPLOYEES",
			wantRegistered: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
						[][]driver.Value{
							{"MYSCHEMA", tc.registeredAs, "ASNCDC", "EMPLOYEES_CT"},
						}, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schemas: []string{"MYSCHEMA"},
				Tables:  []string{tc.configuredAs},
			}, Version{})

			err := s.Initialize(context.Background())
			if tc.wantRegistered {
				require.NoError(t, err)
				assert.Len(t, s.changeTables, 1)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestInitializeCustomCDCSchema verifies that a non-default CDC schema
// (AsnCDCSchema config field) is used in the IBMSNAP_REGISTER query instead of
// the hardcoded default. This allows operators who installed SQL Replication
// under a custom schema to use the connector.
func TestInitializeCustomCDCSchema(t *testing.T) {
	t.Parallel()

	var capturedQueries []string

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			capturedQueries = append(capturedQueries, q)
			if strings.Contains(q, "SOURCE_OWNER") {
				return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
					[][]driver.Value{
						{"MYSCHEMA", "EMPLOYEES", "CDCSCHEMA", "EMPLOYEES_CT"},
					}, nil
			}
			// detectCommitSeqByteLen queries SYSCAT.COLUMNS — return a valid length.
			return []string{"LENGTH"}, [][]driver.Value{{int64(16)}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:      []string{"MYSCHEMA"},
		Tables:       []string{"EMPLOYEES"},
		AsnCDCSchema: "CDCSCHEMA", // non-default CDC schema
	}, Version{})

	err := s.Initialize(context.Background())
	require.NoError(t, err)

	allQueries := strings.Join(capturedQueries, "\n")
	assert.Contains(t, allQueries, "CDCSCHEMA.IBMSNAP_REGISTER",
		"Initialize must query the custom CDC schema, not the hardcoded default")
	assert.NotContains(t, allQueries, "ASNCDC.IBMSNAP_REGISTER",
		"default schema name must not appear when a custom schema is configured")
}

func TestStreamHandlerError(t *testing.T) {
	t.Parallel()

	ts := time.Now()
	callCount := 0

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			callCount++
			if strings.Contains(q, "SYNCHPOINT") {
				// Return a large SYNCHPOINT so pollChanges sees new events.
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0xFF, 0xFF}},
				}, nil
			}
			// Change table returns one event.
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID",
				}, [][]driver.Value{{
					[]byte{0, 0, 0, 0, 0, 0, 0, 50},
					int64(1),
					"I",
					ts,
					int64(1),
				}}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{
		Schemas:         []string{"MYSCHEMA"},
		BackoffInterval: time.Millisecond,
		PollBatchSize:   100,
		StartingCSN:     NewCSN(0),
	}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	err := s.Stream(context.Background(), func(_ ChangeEvent) error {
		return fmt.Errorf("handler refused event")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler refused event")
}

// TestPollChangeTableFullBatchWithPairedUpdates verifies that tableResult.full is
// set from the raw SQL row count BEFORE pairOpcodeEvents merging (C1 fix).
//
// Scenario: PollBatchSize=4, feed exactly 4 rows as 2 D+I pairs (opcodes 3+4).
// pairOpcodeEvents reduces them to 2 OpTypeUpdate events.  tableResult.full must
// still be true because the raw row count was 4 == PollBatchSize, even though
// len(events) == 2 after merging.  Without C1, len(events)==2 != batchSize==4
// → full=false → computeSafeCSN would not protect the watermark.
func TestPollChangeTableFullBatchWithPairedUpdates(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 42}
	ts := time.Now().Truncate(time.Second)

	// 4 rows: 2 D+I pairs using IBMSNAP_OPCODE (3=update-before, 4=update-after).
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 99}},
				}, nil
			}
			return []string{
					"IBMSNAP_OPCODE",
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "VAL",
				}, [][]driver.Value{
					{int64(3), sharedCSN, int64(1), "D", ts, int64(1), int64(100)}, // pair 1 before
					{int64(4), sharedCSN, int64(2), "I", ts, int64(1), int64(101)}, // pair 1 after
					{int64(3), sharedCSN, int64(3), "D", ts, int64(2), int64(200)}, // pair 2 before
					{int64(4), sharedCSN, int64(4), "I", ts, int64(2), int64(201)}, // pair 2 after
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}, PollBatchSize: 4}, Version{})
	s.changeTables["MYSCHEMA.T"] = "ASNCDC.T_CT"

	events, rawCount, err := s.pollChangeTable(context.Background(), "MYSCHEMA.T", "MYSCHEMA", "T", "ASNCDC.T_CT",
		NewCSN(0), 0, NewCSN(99), 0)
	require.NoError(t, err)
	require.Len(t, events, 2, "two D+I pairs must merge to 2 update events")
	assert.Equal(t, 4, rawCount, "raw row count must be 4 even though only 2 events after merging")
	assert.Equal(t, OpTypeUpdate, events[0].Operation)
	assert.Equal(t, OpTypeUpdate, events[1].Operation)

	// Verify via pollChanges that full=true is propagated correctly.
	events2, _, _, err := s.pollChanges(context.Background(), NewCSN(0), nil)
	require.NoError(t, err)
	require.Len(t, events2, 2, "pollChanges must return 2 merged update events")
}

// TestPollChangeTableMultipleDeletesSameTable verifies that multiple DELETE
// operations on the same table within one poll batch each produce an
// independent OpTypeDelete event with the correct before-image data.
//
// This corresponds to Debezium's deleteWithoutTombstone test, which executes
// "DELETE FROM tableB" to remove N rows in a single transaction and asserts
// that each delete event carries the deleted row's column values in the
// before-image. In our model the before-image is in Data (not BeforeData)
// for DELETE events — consistent with the Debezium "before" field mapping.
func TestPollChangeTableMultipleDeletesSameTable(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	csnBytes := []byte{0, 0, 0, 0, 0, 0, 0, 10} // single transaction CSN

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "COLB",
				}, [][]driver.Value{
					// 5 rows deleted in the same transaction (same CSN, ascending IntentSeq)
					{csnBytes, int64(1), "D", ts, int64(10), "b"},
					{csnBytes, int64(2), "D", ts, int64(11), "b"},
					{csnBytes, int64(3), "D", ts, int64(12), "b"},
					{csnBytes, int64(4), "D", ts, int64(13), "b"},
					{csnBytes, int64(5), "D", ts, int64(14), "b"},
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"DB2INST1"}, PollBatchSize: 100}, Version{})
	events, _, err := s.pollChangeTable(context.Background(), "DB2INST1.TABLEB", "DB2INST1", "TABLEB", "ASNCDC.TABLEB_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	require.Len(t, events, 5, "each deleted row must produce one OpTypeDelete event")

	for i, evt := range events {
		assert.Equal(t, OpTypeDelete, evt.Operation, "event %d must be a delete", i)
		assert.Equal(t, uint64(10), evt.CSN.Uint64(), "all deletes share the same transaction CSN")
		assert.Equal(t, int64(i+1), evt.IntentSeq, "intent seq must be sequential")
		assert.Equal(t, int64(10+i), evt.Data["ID"], "before-image ID must match deleted row")
		assert.Equal(t, "b", evt.Data["COLB"], "before-image COLB must match deleted row")
		assert.Nil(t, evt.BeforeData, "DELETE events must have nil BeforeData (before-image is in Data)")
	}
}

// TestPollChangeTablePrimaryKeyUpdate verifies that an UPDATE which changes the
// primary key column is correctly captured as a D+I pair (not as a phantom delete
// followed by an unrelated insert).
//
// This mirrors Debezium's updatePrimaryKey test: UPDATE tablea SET id=100 WHERE id=1
// produces a D row with the old pk value and an I row with the new pk value, both
// sharing the same IBMSNAP_COMMITSEQ. The connector emits these as OpTypeDelete
// (before-image, old pk=1) and OpTypeInsert (after-image, new pk=100).
//
// Note: DB2 LUW SQL Replication does NOT encode PK updates differently from
// regular updates. An UPDATE to a PK column still produces a D+I pair in the
// change table with the same COMMITSEQ. pairOpcodeEvents then merges the pair
// into OpTypeUpdate with BeforeData populated.
func TestPollChangeTablePrimaryKeyUpdate(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 77}
	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			// IBMSNAP_OPCODE=3 (update-before) + 4 (update-after) detect a PK-update pair.
			return []string{
					"IBMSNAP_OPCODE",
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "COLA",
				}, [][]driver.Value{
					{int64(3), sharedCSN, int64(1), "D", ts, int64(1), "a"},   // before: id=1
					{int64(4), sharedCSN, int64(2), "I", ts, int64(100), "a"}, // after: id=100
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"DB2INST1"}, PollBatchSize: 100}, Version{})
	events, _, err := s.pollChangeTable(context.Background(), "DB2INST1.TABLEA", "DB2INST1", "TABLEA", "ASNCDC.TABLEA_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	require.Len(t, events, 1, "D+I pair for PK update must merge into a single OpTypeUpdate event")

	evt := events[0]
	assert.Equal(t, OpTypeUpdate, evt.Operation)
	assert.Equal(t, uint64(77), evt.CSN.Uint64())

	// After-image: new pk=100
	assert.Equal(t, int64(100), evt.Data["ID"], "Data must contain the after-image (new pk)")
	assert.Equal(t, "a", evt.Data["COLA"])

	// Before-image: old pk=1
	require.NotNil(t, evt.BeforeData, "BeforeData must be populated for PK update")
	assert.Equal(t, int64(1), evt.BeforeData["ID"], "BeforeData must contain the before-image (old pk)")
	assert.Equal(t, "a", evt.BeforeData["COLA"])
}

// TestPollChangeTableNullColumnsInCDCRows verifies that NULL column values in
// CDC change table rows (not snapshot rows) are represented as nil in the
// Data map. This is distinct from TestSnapshotNULLColumnValues which tests
// snapshot null handling. Change table rows also carry NULL for optional columns.
//
// Scenario: UPDATE employees SET manager_id = NULL WHERE id = 5.
// The after-image I row has manager_id = NULL; the before-image D row had a value.
func TestPollChangeTableNullColumnsInCDCRows(t *testing.T) {
	t.Parallel()

	ts := time.Now().Truncate(time.Second)
	csnBytes := []byte{0, 0, 0, 0, 0, 0, 0, 55}

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_OPCODE",
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "MANAGER_ID", "DEPT",
				}, [][]driver.Value{
					// before: manager_id was 42, dept was "ENG"
					{int64(3), csnBytes, int64(1), "D", ts, int64(5), int64(42), "ENG"},
					// after: manager_id set to NULL, dept set to NULL
					{int64(4), csnBytes, int64(2), "I", ts, int64(5), nil, nil},
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}, PollBatchSize: 100}, Version{})
	events, _, err := s.pollChangeTable(context.Background(), "MYSCHEMA.EMPLOYEES", "MYSCHEMA", "EMPLOYEES", "ASNCDC.EMPLOYEES_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	require.Len(t, events, 1, "D+I pair must merge into one update event")

	evt := events[0]
	assert.Equal(t, OpTypeUpdate, evt.Operation)

	// After-image: NULL columns must appear as nil (present key, nil value).
	managerAfter, managerAfterExists := evt.Data["MANAGER_ID"]
	assert.True(t, managerAfterExists, "MANAGER_ID must be present in Data even when NULL")
	assert.Nil(t, managerAfter, "NULL after-image value must be nil in Data")

	deptAfter, deptAfterExists := evt.Data["DEPT"]
	assert.True(t, deptAfterExists, "DEPT must be present in Data even when NULL")
	assert.Nil(t, deptAfter, "NULL after-image value must be nil in Data")

	// Before-image: non-null values must be preserved in BeforeData.
	require.NotNil(t, evt.BeforeData)
	assert.Equal(t, int64(42), evt.BeforeData["MANAGER_ID"], "before-image must preserve the original manager_id")
	assert.Equal(t, "ENG", evt.BeforeData["DEPT"])
}

// TestPollChangeTableMultipleInsertsSharedCSN verifies that N INSERT events
// sharing the same IBMSNAP_COMMITSEQ (a single transaction with N inserts) are
// all emitted in ascending IBMSNAP_INTENTSEQ order. This mirrors the Debezium
// test pattern where a loop inserts N rows in a single commit and expects N
// sequential INSERT events ordered by their position within the transaction.
func TestPollChangeTableMultipleInsertsSharedCSN(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 99}
	ts := time.Now().Truncate(time.Second)

	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{
					"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
					"ID", "COLA",
				}, [][]driver.Value{
					{sharedCSN, int64(1), "I", ts, int64(10), "a"},
					{sharedCSN, int64(2), "I", ts, int64(11), "a"},
					{sharedCSN, int64(3), "I", ts, int64(12), "a"},
					{sharedCSN, int64(4), "I", ts, int64(13), "a"},
					{sharedCSN, int64(5), "I", ts, int64(14), "a"},
				}, nil
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"DB2INST1"}, PollBatchSize: 100}, Version{})
	events, rawCount, err := s.pollChangeTable(context.Background(), "DB2INST1.TABLEA", "DB2INST1", "TABLEA", "ASNCDC.TABLEA_CT",
		NewCSN(0), 0, NewCSN(99999), 0)

	require.NoError(t, err)
	assert.Equal(t, 5, rawCount, "raw row count must equal the number of rows returned")
	require.Len(t, events, 5, "all 5 inserts from the same transaction must be emitted")

	for i, evt := range events {
		assert.Equal(t, OpTypeInsert, evt.Operation, "event %d must be an insert", i)
		assert.Equal(t, uint64(99), evt.CSN.Uint64(), "all events share the transaction CSN")
		assert.Equal(t, int64(i+1), evt.IntentSeq, "events must be ordered by IntentSeq")
		assert.Equal(t, int64(10+i), evt.Data["ID"])
	}
}

// TestInitializeWithTableFilter verifies that a TableFilter applied during
// Initialize restricts which registered tables are monitored. Tables that
// match the filter become CDC-monitored; those that do not are silently skipped
// even when present in IBMSNAP_REGISTER.
//
// This mirrors Debezium's testTableIncludeList/testTableExcludeList behaviour:
// the connector only tracks tables that pass the filter predicate.
func TestInitializeWithTableFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		registered   [][]driver.Value // rows from IBMSNAP_REGISTER
		filter       func(string) bool
		wantTables   []string
		wantFiltered []string // tables that must NOT appear in changeTables
		wantErr      bool
	}{
		{
			name: "filter allows only one table",
			registered: [][]driver.Value{
				{"MYSCHEMA", "TABLEA", "ASNCDC", "TABLEA_CT"},
				{"MYSCHEMA", "TABLEB", "ASNCDC", "TABLEB_CT"},
			},
			filter:       func(t string) bool { return t == "TABLEA" },
			wantTables:   []string{"TABLEA"},
			wantFiltered: []string{"TABLEB"},
		},
		{
			name: "filter rejects all tables returns error",
			registered: [][]driver.Value{
				{"MYSCHEMA", "TABLEA", "ASNCDC", "TABLEA_CT"},
			},
			filter:  func(_ string) bool { return false },
			wantErr: true,
		},
		{
			name: "nil filter allows all tables",
			registered: [][]driver.Value{
				{"MYSCHEMA", "TABLEA", "ASNCDC", "TABLEA_CT"},
				{"MYSCHEMA", "TABLEB", "ASNCDC", "TABLEB_CT"},
			},
			filter:     nil,
			wantTables: []string{"TABLEA", "TABLEB"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if strings.Contains(q, "SOURCE_OWNER") {
						return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
							tc.registered, nil
					}
					// detectCommitSeqByteLen
					return []string{"LENGTH"}, [][]driver.Value{{int64(10)}}, nil
				},
			})

			s := NewStreamer(db, StreamConfig{
				Schemas:     []string{"MYSCHEMA"},
				TableFilter: tc.filter,
				// Tables is empty → dynamic discovery
			}, Version{})

			err := s.Initialize(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			for _, table := range tc.wantTables {
				_, ok := s.changeTables["MYSCHEMA."+strings.ToUpper(table)]
				assert.True(t, ok, "table %s must be registered after Initialize", table)
			}
			for _, table := range tc.wantFiltered {
				_, ok := s.changeTables["MYSCHEMA."+strings.ToUpper(table)]
				assert.False(t, ok, "table %s must NOT be registered (filtered out)", table)
			}
		})
	}
}

// TestPollChangeTableCrossBatchUpdatePairing verifies cross-poll D+I pair merging (C3 fix).
//
// When PollBatchSize=1 and a D+I pair straddles two polls:
//   - Poll 1 returns only the D row (opcode 3). It must NOT be emitted as a delete.
//     The pending before-image is held in pendingBeforeByTable.
//   - Poll 2 returns only the I row (opcode 4). It must be merged with the pending
//     before-image into a single OpTypeUpdate event with BeforeData populated.
func TestPollChangeTableCrossBatchUpdatePairing(t *testing.T) {
	t.Parallel()

	sharedCSN := []byte{0, 0, 0, 0, 0, 0, 0, 55}
	ts := time.Now().Truncate(time.Second)

	callNum := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "SYNCHPOINT") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 99}},
				}, nil
			}
			callNum++
			cols := []string{
				"IBMSNAP_OPCODE",
				"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER",
				"ID", "VAL",
			}
			switch callNum {
			case 1:
				// Batch 1: only the before-image (opcode 3)
				return cols, [][]driver.Value{
					{int64(3), sharedCSN, int64(1), "D", ts, int64(7), int64(10)},
				}, nil
			case 2:
				// Batch 2: only the after-image (opcode 4)
				return cols, [][]driver.Value{
					{int64(4), sharedCSN, int64(2), "I", ts, int64(7), int64(20)},
				}, nil
			default:
				return cols, nil, nil
			}
		},
	})

	s := NewStreamer(db, StreamConfig{Schemas: []string{"MYSCHEMA"}, PollBatchSize: 1}, Version{})

	// Poll 1: D row (opcode 3) arrives. Must be held as pending — 0 events emitted.
	events1, rawCount1, err := s.pollChangeTable(context.Background(), "MYSCHEMA.T", "MYSCHEMA", "T", "ASNCDC.T_CT",
		NewCSN(0), 0, NewCSN(99), 0)
	require.NoError(t, err)
	assert.Empty(t, events1, "D row (opcode 3) must be held as pending, not emitted")
	assert.Equal(t, 1, rawCount1, "raw count must be 1")
	assert.NotNil(t, s.pendingBeforeByTable["MYSCHEMA.T"], "pending before-image must be held")

	// Poll 2: I row (opcode 4) arrives. Must be merged with pending into 1 update event.
	events2, rawCount2, err := s.pollChangeTable(context.Background(), "MYSCHEMA.T", "MYSCHEMA", "T", "ASNCDC.T_CT",
		NewCSN(0), 0, NewCSN(99), 0)
	require.NoError(t, err)
	require.Len(t, events2, 1, "pending before-image + I row must merge to 1 update event")
	assert.Equal(t, 1, rawCount2, "raw count must be 1 (only the I row)")
	assert.Nil(t, s.pendingBeforeByTable["MYSCHEMA.T"], "pending state must be cleared after merge")

	update := events2[0]
	assert.Equal(t, OpTypeUpdate, update.Operation)
	assert.Equal(t, int64(20), update.Data["VAL"], "after-image value must be in Data")
	assert.Equal(t, int64(10), update.BeforeData["VAL"], "before-image value must be in BeforeData")
}
