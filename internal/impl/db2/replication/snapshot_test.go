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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
)

// ---------------------------------------------------------------------------
// Pure-function tests (no DB)
// ---------------------------------------------------------------------------

func TestConvertDB2Value(t *testing.T) {
	t.Parallel()

	// Use the fake driver with colTypes so DatabaseTypeName returns the right values.
	db := openFakeDB(t, &replFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			return []string{"NAME", "DATA", "NUM"}, [][]driver.Value{
				{[]byte("Alice"), []byte{0xDE, 0xAD}, int64(42)},
			}, nil
		},
		colTypes: func(_ string) map[string]string {
			return map[string]string{
				"NAME": "VARCHAR",
				"DATA": "BLOB",
				"NUM":  "INTEGER",
			}
		},
	})

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(context.Background(), "SELECT NAME, DATA, NUM FROM T")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()); require.NoError(t, rows.Err()) }()

	colTypes, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, colTypes, 3)

	// Compute isStringCol for each column type (VARCHAR→true, BLOB→false, INTEGER→false).
	isStringCol := func(ct *sql.ColumnType) bool {
		t := strings.ToUpper(ct.DatabaseTypeName())
		return strings.Contains(t, "CHAR") || strings.Contains(t, "CLOB") || strings.Contains(t, "TEXT")
	}
	// []byte with VARCHAR → string
	assert.Equal(t, "Alice", convertDB2Value([]byte("Alice"), isStringCol(colTypes[0])))
	// []byte with BLOB → raw bytes
	assert.Equal(t, []byte{0xDE, 0xAD}, convertDB2Value([]byte{0xDE, 0xAD}, isStringCol(colTypes[1])))
	// int64 passes through unchanged
	assert.Equal(t, int64(42), convertDB2Value(int64(42), isStringCol(colTypes[2])))
	// nil returns nil regardless of column type
	assert.Nil(t, convertDB2Value(nil, isStringCol(colTypes[0])))
	// []byte with CHAR type → string (isStringCol=true directly)
	assert.Equal(t, "abc", convertDB2Value([]byte("abc"), true))
}

func TestSnapshotConfigAsncdcSchema(t *testing.T) {
	t.Parallel()

	c := &SnapshotConfig{}
	assert.Equal(t, "ASNCDC", c.asncdcSchema(), "default schema")

	c.AsnCDCSchema = "CUSTOM"
	assert.Equal(t, "CUSTOM", c.asncdcSchema(), "explicit schema")
}

// ---------------------------------------------------------------------------
// Tests requiring a fake database
// ---------------------------------------------------------------------------

func TestCaptureCurrentCSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryFunc func(query string, args []driver.Value) ([]string, [][]driver.Value, error)
		wantCSN   uint64
		wantErr   bool
	}{
		{
			name: "normal SYNCHPOINT bytes",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				// 8-byte big-endian value for uint64(12345) = 0x0000000000003039
				b := []byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{b}}, nil
			},
			wantCSN: 12345,
		},
		{
			name: "NULL SYNCHPOINT returns zero CSN",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			},
			wantCSN: 0,
		},
		{
			name: "table does not exist returns zero CSN (SQLSTATE 42704)",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				// Real DB2 driver returns SQLSTATE 42704 (undefined name) when the
				// IBMSNAP_REGISTER table has not yet been created (CDC not yet set up).
				return nil, nil, &db2cli.DB2Error{SQLState: "42704", NativeError: -204, Message: "IBMSNAP_REGISTER is an undefined name"}
			},
			wantCSN: 0,
		},
		{
			name: "table does not exist returns zero CSN (DB2 Error string format)",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				// Fallback path: wrapped error where AsType cannot extract DB2Error.
				return nil, nil, fmt.Errorf("sql: %w", fmt.Errorf("DB2 Error [42704] (Native: -204): IBMSNAP_REGISTER is an undefined name"))
			},
			wantCSN: 0,
		},
		{
			name: "unexpected error propagates",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				return nil, nil, fmt.Errorf("connection timeout")
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{query: tc.queryFunc})
			s := NewSnapshotter(db, SnapshotConfig{Schemas: []string{"MYSCHEMA"}}, Version{})

			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback() //nolint:errcheck

			csn, err := s.captureCurrentCSN(context.Background(), tx)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCSN, csn.Uint64())
		})
	}
}

func TestDiscoverPrimaryKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rows    [][]driver.Value
		wantPKs []string
		wantErr bool
	}{
		{
			name:    "single PK column",
			rows:    [][]driver.Value{{"ID   "}}, // trailing spaces trimmed
			wantPKs: []string{"ID"},
		},
		{
			name:    "composite PK",
			rows:    [][]driver.Value{{"COL_A"}, {"COL_B"}},
			wantPKs: []string{"COL_A", "COL_B"},
		},
		{
			name:    "no primary key returns empty slice",
			rows:    nil,
			wantPKs: nil,
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
						return nil, nil, fmt.Errorf("query error")
					}
					return []string{"COLNAME"}, tc.rows, nil
				},
			})

			s := NewSnapshotter(db, SnapshotConfig{Schemas: []string{"MYSCHEMA"}}, Version{})
			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback() //nolint:errcheck

			pks, err := s.discoverPrimaryKeys(context.Background(), tx, "MYSCHEMA", "EMPLOYEES")
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantPKs, pks)
		})
	}
}

func TestGetTableColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rows     [][]driver.Value
		wantCols []string
		wantErr  bool
	}{
		{
			name:     "normal columns",
			rows:     [][]driver.Value{{"ID"}, {"NAME  "}, {"SALARY"}},
			wantCols: []string{"ID", "NAME", "SALARY"},
		},
		{
			// rows is non-nil but empty: query succeeds, zero rows → triggers len(columns)==0 guard
			name:    "no columns returns error",
			rows:    [][]driver.Value{},
			wantErr: true,
		},
		{
			// rows is nil: used as sentinel to simulate a query-level failure
			name:    "query error propagates",
			rows:    nil,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openFakeDB(t, &replFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.rows == nil {
						return nil, nil, fmt.Errorf("query failed")
					}
					return []string{"COLNAME"}, tc.rows, nil
				},
			})

			s := NewSnapshotter(db, SnapshotConfig{Schemas: []string{"MYSCHEMA"}}, Version{})
			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback() //nolint:errcheck

			cols, err := s.getTableColumns(context.Background(), tx, "MYSCHEMA", "EMPLOYEES")
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCols, cols)
		})
	}
}

func TestFetchBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		columns       []string
		pks           []string
		lastKeyValues []any
		rows          [][]driver.Value
		wantCount     int
		wantQueryFn   func(t *testing.T, query string)
	}{
		{
			name:      "first page (no last key)",
			columns:   []string{"ID", "NAME"},
			pks:       []string{"ID"},
			rows:      [][]driver.Value{{int64(1), "Alice"}, {int64(2), "Bob"}},
			wantCount: 2,
			wantQueryFn: func(t *testing.T, query string) {
				t.Helper()
				assert.NotContains(t, query, "WHERE")
			},
		},
		{
			name:          "subsequent page uses WHERE clause",
			columns:       []string{"ID", "NAME"},
			pks:           []string{"ID"},
			lastKeyValues: []any{int64(2)},
			rows:          [][]driver.Value{{int64(3), "Charlie"}},
			wantCount:     1,
			wantQueryFn: func(t *testing.T, query string) {
				t.Helper()
				assert.Contains(t, query, "WHERE")
				assert.Contains(t, query, "?")
			},
		},
		{
			name:      "empty result set",
			columns:   []string{"ID"},
			pks:       []string{"ID"},
			rows:      nil,
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedQuery string
			db := openFakeDB(t, &replFakeHandlers{
				query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					capturedQuery = q
					return tc.columns, tc.rows, nil
				},
			})

			const batchSize = 100
			s := NewSnapshotter(db, SnapshotConfig{
				Schemas:   []string{"MYSCHEMA"},
				BatchSize: batchSize,
			}, Version{})

			tx, err := db.Begin()
			require.NoError(t, err)
			defer tx.Rollback() //nolint:errcheck

			// Build the pre-computed SQL strings the same way snapshotTable does.
			quotedCols := make([]string, len(tc.columns))
			for i, col := range tc.columns {
				quotedCols[i] = QuoteDB2Identifier(col)
			}
			quotedPKs := make([]string, len(tc.pks))
			for i, pk := range tc.pks {
				quotedPKs[i] = QuoteDB2Identifier(pk)
			}
			orderBy := strings.Join(quotedPKs, ", ")
			base := fmt.Sprintf(`SELECT %s FROM %s.%s`,
				strings.Join(quotedCols, ", "),
				QuoteDB2Identifier("MYSCHEMA"),
				QuoteDB2Identifier("EMPLOYEES"),
			)
			suffix := fmt.Sprintf(" ORDER BY %s FETCH FIRST %d ROWS ONLY", orderBy, batchSize)
			qNoBounds := base + suffix
			placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(tc.pks)), ", ")
			qWithBounds := base + fmt.Sprintf(" WHERE (%s) > (%s)", orderBy, placeholders) + suffix

			var isStringColCache []bool
			result, err := s.fetchBatch(context.Background(), tx, qNoBounds, qWithBounds, tc.columns, tc.lastKeyValues, &isStringColCache)
			require.NoError(t, err)
			assert.Len(t, result, tc.wantCount)

			if tc.wantQueryFn != nil {
				tc.wantQueryFn(t, capturedQuery)
			}
		})
	}
}

func TestSnapshotEndToEnd(t *testing.T) {
	t.Parallel()

	// Simulate: 3 rows in one table, captured in a single batch.
	// Query sequence:
	//   1. MAX(SYNCHPOINT) → returns CSN bytes for 12345
	//   2. KEYCOLUSE       → returns "ID"
	//   3. SYSCAT.COLUMNS  → returns "ID", "NAME"
	//   4. SELECT *        → returns 3 rows
	//   5. SELECT *        → returns empty (end of pagination)

	queryCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0x30, 0x39}}, // 12345
				}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}, {"NAME"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				// Return data on the first page, empty on the second.
				if queryCount == 4 {
					return []string{"ID", "NAME"}, [][]driver.Value{
						{int64(1), "Alice"},
						{int64(2), "Bob"},
						{int64(3), "Charlie"},
					}, nil
				}
				return []string{"ID", "NAME"}, nil, nil
			default:
				return nil, nil, nil
			}
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"EMPLOYEES"},
		BatchSize: 1000,
	}, Version{Major: 11, Minor: 5})

	var events []ChangeEvent
	startCSN, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(12345), startCSN.Uint64())
	assert.Len(t, events, 3)
	for _, e := range events {
		assert.Equal(t, OpTypeRead, e.Operation)
		assert.Equal(t, "MYSCHEMA", e.Schema)
		assert.Equal(t, "EMPLOYEES", e.Table)
		assert.True(t, e.CSN.IsNull())
	}
}

func TestSnapshotSkipsWhenNoPK(t *testing.T) {
	t.Parallel()

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				// No PK columns
				return []string{"COLNAME"}, nil, nil
			default:
				return nil, nil, nil
			}
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"NOTKEYED"},
		BatchSize: 100,
	}, Version{})

	_, err := s.Snapshot(context.Background(), func(_ ChangeEvent) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key")
}

func TestSnapshotHandlerError(t *testing.T) {
	t.Parallel()

	queryCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				return []string{"ID"}, [][]driver.Value{{int64(1)}}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"T"},
		BatchSize: 100,
	}, Version{})

	_, err := s.Snapshot(context.Background(), func(_ ChangeEvent) error {
		return fmt.Errorf("handler rejected event")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler rejected event")
}

func TestSnapshotContextCancellation(t *testing.T) {
	t.Parallel()

	queryCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				return []string{"ID"}, [][]driver.Value{{int64(1)}, {int64(2)}, {int64(3)}}, nil
			default:
				return nil, nil, nil
			}
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"T"},
		BatchSize: 100,
	}, Version{})

	ctx, cancel := context.WithCancel(context.Background())

	count := 0
	_, err := s.Snapshot(ctx, func(_ ChangeEvent) error {
		count++
		if count == 1 {
			cancel() // cancel after the first event
		}
		return ctx.Err()
	})

	require.Error(t, err)
}

// TestSnapshotMultipleTablesSequential verifies that when multiple tables are
// configured, the Snapshotter reads them one at a time in declaration order and
// emits OpTypeRead events with the correct Schema and Table fields for each.
// Every snapshot event must carry a null CSN since it originates outside the
// CDC log stream.
func TestSnapshotMultipleTablesSequential(t *testing.T) {
	t.Parallel()

	// The fake handler dispatches by inspecting the query string. KEYCOLUSE and
	// SYSCAT.COLUMNS queries embed the table name as a bind parameter (not in
	// the SQL text), so we track which table is currently being snapshotted by
	// counting how many times we have seen a FETCH FIRST query (data page queries)
	// for each table.
	//
	// Query sequence for 2 tables (EMPLOYEES then ORDERS):
	//   SYNCHPOINT         → CSN 100 (once, at start of Snapshot)
	//   KEYCOLUSE (args)   → "ID"        (EMPLOYEES)
	//   SYSCAT.COLUMNS     → "ID","NAME" (EMPLOYEES)
	//   FETCH FIRST …      → 2 rows      (EMPLOYEES page 1)
	//   FETCH FIRST …      → empty       (EMPLOYEES page 2, end)
	//   KEYCOLUSE (args)   → "ORDER_ID"  (ORDERS)
	//   SYSCAT.COLUMNS     → "ORDER_ID","AMOUNT" (ORDERS)
	//   FETCH FIRST …      → 1 row       (ORDERS page 1)
	//   FETCH FIRST …      → empty       (ORDERS page 2, end)

	fetchCount := 0
	keyColCount := 0
	colCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{
					{[]byte{0, 0, 0, 0, 0, 0, 0, 100}},
				}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				keyColCount++
				if keyColCount == 1 {
					return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
				}
				return []string{"COLNAME"}, [][]driver.Value{{"ORDER_ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				colCount++
				if colCount == 1 {
					return []string{"COLNAME"}, [][]driver.Value{{"ID"}, {"NAME"}}, nil
				}
				return []string{"COLNAME"}, [][]driver.Value{{"ORDER_ID"}, {"AMOUNT"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				fetchCount++
				switch fetchCount {
				case 1: // EMPLOYEES page 1
					return []string{"ID", "NAME"}, [][]driver.Value{
						{int64(1), "Alice"},
						{int64(2), "Bob"},
					}, nil
				case 2: // EMPLOYEES page 2 → end of table
					return []string{"ID", "NAME"}, nil, nil
				case 3: // ORDERS page 1
					return []string{"ORDER_ID", "AMOUNT"}, [][]driver.Value{
						{int64(1001), int64(500)},
					}, nil
				default: // ORDERS page 2 → end of table
					return []string{"ORDER_ID", "AMOUNT"}, nil, nil
				}
			}
			return nil, nil, nil
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"EMPLOYEES", "ORDERS"},
		BatchSize: 1000,
	}, Version{Major: 11, Minor: 5})

	var events []ChangeEvent
	startCSN, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, uint64(100), startCSN.Uint64())

	var empEvents, ordEvents []ChangeEvent
	for _, e := range events {
		assert.Equal(t, OpTypeRead, e.Operation, "all snapshot events must have operation=read")
		assert.Equal(t, "MYSCHEMA", e.Schema)
		assert.True(t, e.CSN.IsNull(), "snapshot events must carry a null CSN")
		switch e.Table {
		case "EMPLOYEES":
			empEvents = append(empEvents, e)
		case "ORDERS":
			ordEvents = append(ordEvents, e)
		}
	}

	assert.Len(t, empEvents, 2, "EMPLOYEES table must produce 2 snapshot events")
	assert.Len(t, ordEvents, 1, "ORDERS table must produce 1 snapshot event")
}

// TestSnapshotNULLColumnValues verifies that NULL column values in the source
// table are represented as nil in the ChangeEvent Data map. A nil value must
// be present as an explicit key (not absent), so that downstream consumers can
// distinguish "value is NULL" from "column was not selected".
func TestSnapshotNULLColumnValues(t *testing.T) {
	t.Parallel()

	queryCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}, {"MANAGER_ID"}, {"DEPT"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				if queryCount == 4 {
					// MANAGER_ID and DEPT are NULL for this employee.
					return []string{"ID", "MANAGER_ID", "DEPT"}, [][]driver.Value{
						{int64(1), nil, nil},
					}, nil
				}
				return []string{"ID", "MANAGER_ID", "DEPT"}, nil, nil
			}
			return nil, nil, nil
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"EMPLOYEES"},
		BatchSize: 100,
	}, Version{})

	var events []ChangeEvent
	_, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Equal(t, int64(1), events[0].Data["ID"])
	// NULL columns must appear in the Data map as nil, not as absent keys.
	managerVal, managerExists := events[0].Data["MANAGER_ID"]
	assert.True(t, managerExists, "MANAGER_ID key must be present even when the column value is NULL")
	assert.Nil(t, managerVal, "NULL column value must be nil in the Data map")

	deptVal, deptExists := events[0].Data["DEPT"]
	assert.True(t, deptExists, "DEPT key must be present even when the column value is NULL")
	assert.Nil(t, deptVal, "NULL column value must be nil in the Data map")
}

// TestSnapshotCompositePKKeysetPagination verifies that tables with composite
// primary keys are paginated correctly using multi-column keyset pagination.
// The WHERE clause must use tuple comparison: (PK1, PK2) > (lastVal1, lastVal2).
//
// This corresponds to the Debezium "composite PK" scenario where a table with
// two PK columns (e.g. id + region) must advance the page cursor using all PK
// columns simultaneously to avoid re-reading rows.
func TestSnapshotCompositePKKeysetPagination(t *testing.T) {
	t.Parallel()

	var capturedFetchQueries []string
	queryCount := 0

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			if strings.Contains(q, "FETCH FIRST") {
				capturedFetchQueries = append(capturedFetchQueries, q)
			}
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				// Composite PK: (REGION_ID, EMP_ID) — two columns
				return []string{"COLNAME"}, [][]driver.Value{{"REGION_ID"}, {"EMP_ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{
					{"REGION_ID"}, {"EMP_ID"}, {"NAME"},
				}, nil
			case strings.Contains(q, "FETCH FIRST"):
				switch len(capturedFetchQueries) {
				case 1: // first page
					return []string{"REGION_ID", "EMP_ID", "NAME"}, [][]driver.Value{
						{int64(1), int64(100), "Alice"},
						{int64(1), int64(101), "Bob"},
					}, nil
				case 2: // second page
					return []string{"REGION_ID", "EMP_ID", "NAME"}, [][]driver.Value{
						{int64(2), int64(200), "Charlie"},
					}, nil
				default: // end of data
					return []string{"REGION_ID", "EMP_ID", "NAME"}, nil, nil
				}
			}
			return nil, nil, nil
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"EMPLOYEES"},
		BatchSize: 2,
	}, Version{})

	var events []ChangeEvent
	_, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	assert.Len(t, events, 3, "must receive all 3 rows across two pages")

	require.Len(t, capturedFetchQueries, 3, "must issue 3 SELECT queries")

	// First page: no WHERE clause
	assert.NotContains(t, capturedFetchQueries[0], "WHERE",
		"first page must not have a WHERE clause")

	// Second page: composite tuple WHERE clause with both PK columns and placeholders
	assert.Contains(t, capturedFetchQueries[1], "WHERE",
		"second page must have a keyset WHERE clause")
	assert.Contains(t, capturedFetchQueries[1], "REGION_ID",
		"second page WHERE must reference the first PK column")
	assert.Contains(t, capturedFetchQueries[1], "EMP_ID",
		"second page WHERE must reference the second PK column")
	assert.Contains(t, capturedFetchQueries[1], "?",
		"second page WHERE must use parameter placeholders")
}

// TestSnapshotBlobAndClobColumns verifies that BLOB/binary columns in snapshot
// rows are returned as raw []byte and CLOB/text columns as strings.
//
// This corresponds to the Debezium type propagation test (shouldPropagateSourceTypeByDatatype)
// which verifies that binary and character large objects are decoded correctly.
// Our connector uses convertDB2Value: CHAR/VARCHAR/CLOB/TEXT → string,
// BLOB/binary → raw []byte (to be base64-encoded when marshalled to JSON).
func TestSnapshotBlobAndClobColumns(t *testing.T) {
	t.Parallel()

	queryCount := 0
	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{
					{"ID"}, {"DOC_CONTENT"}, {"THUMBNAIL"}, {"TITLE"},
				}, nil
			case strings.Contains(q, "FETCH FIRST"):
				if queryCount == 4 {
					return []string{"ID", "DOC_CONTENT", "THUMBNAIL", "TITLE"}, [][]driver.Value{
						{int64(1), []byte("hello world"), []byte{0xDE, 0xAD, 0xBE, 0xEF}, []byte("My Document")},
					}, nil
				}
				return []string{"ID", "DOC_CONTENT", "THUMBNAIL", "TITLE"}, nil, nil
			}
			return nil, nil, nil
		},
		colTypes: func(_ string) map[string]string {
			return map[string]string{
				"ID":          "INTEGER",
				"DOC_CONTENT": "CLOB",    // text large object → string
				"THUMBNAIL":   "BLOB",    // binary large object → raw bytes
				"TITLE":       "VARCHAR", // varchar → string
			}
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"DOCUMENTS"},
		BatchSize: 100,
	}, Version{})

	var events []ChangeEvent
	_, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, events, 1)

	row := events[0].Data

	// CLOB content must be decoded to string (not raw bytes)
	assert.Equal(t, "hello world", row["DOC_CONTENT"],
		"CLOB columns must be decoded to string via convertDB2Value")

	// BLOB content must remain as raw []byte
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, row["THUMBNAIL"],
		"BLOB columns must remain as raw []byte in the Data map")

	// VARCHAR must also be decoded to string
	assert.Equal(t, "My Document", row["TITLE"],
		"VARCHAR columns must be decoded to string")
}

// TestSnapshotKeysetPaginationTwoPages verifies that large tables are paginated
// using keyset pagination: the first page is fetched without a WHERE clause;
// subsequent pages add "WHERE (PK) > (lastKeyValue)" to advance past rows
// already seen. This avoids OFFSET-based scans which degrade on large tables.
func TestSnapshotKeysetPaginationTwoPages(t *testing.T) {
	t.Parallel()

	var capturedFetchQueries []string
	queryCount := 0

	db := openFakeDB(t, &replFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			queryCount++
			if strings.Contains(q, "FETCH FIRST") {
				capturedFetchQueries = append(capturedFetchQueries, q)
			}
			switch {
			case strings.Contains(q, "SYNCHPOINT"):
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			case strings.Contains(q, "KEYCOLUSE"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}}, nil
			case strings.Contains(q, "SYSCAT.COLUMNS"):
				return []string{"COLNAME"}, [][]driver.Value{{"ID"}, {"NAME"}}, nil
			case strings.Contains(q, "FETCH FIRST"):
				switch len(capturedFetchQueries) {
				case 1: // first page — no WHERE clause
					return []string{"ID", "NAME"}, [][]driver.Value{
						{int64(1), "Alice"},
						{int64(2), "Bob"},
					}, nil
				case 2: // second page — WHERE clause must be present
					return []string{"ID", "NAME"}, [][]driver.Value{
						{int64(3), "Charlie"},
					}, nil
				default: // third call → empty, end of pagination
					return []string{"ID", "NAME"}, nil, nil
				}
			}
			return nil, nil, nil
		},
	})

	s := NewSnapshotter(db, SnapshotConfig{
		Schemas:   []string{"MYSCHEMA"},
		Tables:    []string{"EMPLOYEES"},
		BatchSize: 2, // small batch forces multi-page read
	}, Version{})

	var events []ChangeEvent
	_, err := s.Snapshot(context.Background(), func(e ChangeEvent) error {
		events = append(events, e)
		return nil
	})

	require.NoError(t, err)
	assert.Len(t, events, 3, "must receive all 3 rows across two pages")

	require.Len(t, capturedFetchQueries, 3, "must issue exactly 3 SELECT queries (page1, page2, empty)")
	assert.NotContains(t, capturedFetchQueries[0], "WHERE",
		"first page must not have a WHERE clause")
	assert.Contains(t, capturedFetchQueries[1], "WHERE",
		"second page must add a keyset WHERE clause")
	assert.Contains(t, capturedFetchQueries[1], "?",
		"second page WHERE clause must use a parameter placeholder for the last key value")
}
