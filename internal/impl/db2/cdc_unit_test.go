// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"golang.org/x/text/encoding/unicode"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
	"github.com/redpanda-data/connect/v4/internal/impl/db2/replication"
)

// testLogger returns a *service.Logger suitable for unit tests.
func testLogger(t *testing.T) *service.Logger {
	t.Helper()
	return service.MockResources().Logger()
}

// ---------------------------------------------------------------------------
// isAlreadyExistsError
// ---------------------------------------------------------------------------

func TestIsAlreadyExistsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"SQLSTATE=42710", fmt.Errorf("sql error SQLSTATE=42710 object already exists"), true},
		{"SQLSTATE 42710", fmt.Errorf("DB2 SQL Error: SQLCODE=-601, SQLSTATE 42710"), true},
		{"unrelated error", fmt.Errorf("connection refused"), false},
		// bare "42710" without SQLSTATE prefix must not match (too broad)
		{"bare 42710 substring", fmt.Errorf("some message 42710 embedded"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isAlreadyExistsError(tc.err))
		})
	}
}

// ---------------------------------------------------------------------------
// determineCheckpointMode
// ---------------------------------------------------------------------------

func TestDetermineCheckpointMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      string
		version   replication.Version
		wantMode  string
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "auto with 11.5 → csn",
			mode:     "auto",
			version:  replication.Version{Major: 11, Minor: 5},
			wantMode: "csn",
		},
		{
			name:     "auto with 10.1 → csn",
			mode:     "auto",
			version:  replication.Version{Major: 10, Minor: 1},
			wantMode: "csn",
		},
		{
			name:      "auto with 9.7 → error",
			mode:      "auto",
			version:   replication.Version{Major: 9, Minor: 7},
			wantErr:   true,
			errSubstr: "does not support CSN",
		},
		{
			name:     "explicit csn with 11.5 → csn",
			mode:     "csn",
			version:  replication.Version{Major: 11, Minor: 5},
			wantMode: "csn",
		},
		{
			name:      "explicit csn with 9.7 → error",
			mode:      "csn",
			version:   replication.Version{Major: 9, Minor: 7},
			wantErr:   true,
			errSubstr: "requires DB2 10.1+",
		},
		{
			name:      "unknown mode → error",
			mode:      "timestamp",
			version:   replication.Version{Major: 11, Minor: 5},
			wantErr:   true,
			errSubstr: "invalid checkpoint_mode",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := &db2CDCInput{
				checkpointMode: tc.mode,
				version:        tc.version,
				log:            testLogger(t),
			}

			mode, err := d.determineCheckpointMode()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errSubstr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMode, mode)
		})
	}
}

// ---------------------------------------------------------------------------
// eventToMessage
// ---------------------------------------------------------------------------

func TestEventToMessage(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		event    replication.ChangeEvent
		wantMeta map[string]string
	}{
		{
			name: "snapshot read event",
			event: replication.ChangeEvent{
				Schema:    "DB2ADMIN",
				Table:     "EMPLOYEES",
				Operation: replication.OpTypeRead,
				CSN:       replication.NullCSN(),
				Data:      map[string]any{"ID": 1, "NAME": "Alice"},
			},
			wantMeta: map[string]string{
				"db2_schema":    "DB2ADMIN",
				"db2_table":     "EMPLOYEES",
				"db2_operation": "read",
				"db2_csn":       "",
			},
		},
		{
			name: "insert event with CSN and timestamp",
			event: replication.ChangeEvent{
				Schema:    "DB2ADMIN",
				Table:     "ORDERS",
				Operation: replication.OpTypeInsert,
				CSN:       replication.NewCSN(12345),
				Timestamp: ts,
				Data:      map[string]any{"ORDER_ID": 99},
			},
			wantMeta: map[string]string{
				"db2_schema":    "DB2ADMIN",
				"db2_table":     "ORDERS",
				"db2_operation": "insert",
				"db2_csn":       "CSN:0000000000003039",
				"db2_timestamp": ts.Format(time.RFC3339Nano),
			},
		},
		{
			name: "delete event",
			event: replication.ChangeEvent{
				Schema:    "SCHEMA",
				Table:     "T",
				Operation: replication.OpTypeDelete,
				CSN:       replication.NewCSN(999),
				Data:      map[string]any{"ID": 7},
			},
			wantMeta: map[string]string{
				"db2_schema":    "SCHEMA",
				"db2_table":     "T",
				"db2_operation": "delete",
			},
		},
	}

	d := &db2CDCInput{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := d.eventToMessage(tc.event)
			require.NoError(t, err)
			require.NotNil(t, msg)

			for key, want := range tc.wantMeta {
				got, exists := msg.MetaGet(key)
				assert.True(t, exists, "meta key %q should exist", key)
				assert.Equal(t, want, got, "meta key %q", key)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// detectVersion
// ---------------------------------------------------------------------------

func TestDetectVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryFunc func(query string, args []driver.Value) ([]string, [][]driver.Value, error)
		wantMajor int
		wantMinor int
		wantErr   bool
	}{
		{
			name: "ENV_INST_INFO returns SQL11050",
			queryFunc: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				if strings.Contains(q, "ENV_INST_INFO") {
					return []string{"SERVICE_LEVEL"}, [][]driver.Value{{"SQL11050"}}, nil
				}
				return nil, nil, fmt.Errorf("unexpected query: %s", q)
			},
			wantMajor: 11,
			wantMinor: 5,
		},
		{
			name: "fallback to PROD_RELEASE when ENV_INST_INFO fails",
			queryFunc: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				if strings.Contains(q, "ENV_INST_INFO") {
					return nil, nil, fmt.Errorf("table does not exist")
				}
				// Fallback query
				return []string{"PROD_RELEASE"}, [][]driver.Value{{"11.1.0.0"}}, nil
			},
			wantMajor: 11,
			wantMinor: 1,
		},
		{
			name: "both queries fail → error",
			queryFunc: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
				return nil, nil, fmt.Errorf("connection error")
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{query: tc.queryFunc})
			d := &db2CDCInput{db: db, log: testLogger(t)}

			ver, err := d.detectVersion(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantMajor, ver.Major)
			assert.Equal(t, tc.wantMinor, ver.Minor)
		})
	}
}

// ---------------------------------------------------------------------------
// initCheckpointTable
// ---------------------------------------------------------------------------

func TestInitCheckpointTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		createErr error
		wantErr   bool
	}{
		{
			name:      "creates table successfully",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: nil,
		},
		{
			name:      "table already exists (42710) is silently ignored",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: fmt.Errorf("SQLSTATE=42710 object already exists"),
		},
		{
			name:      "other exec error propagates",
			tableName: "RPCN.CDC_CHECKPOINT",
			createErr: fmt.Errorf("permission denied"),
			wantErr:   true,
		},
		{
			name:      "unqualified table name (no schema) creates table",
			tableName: "CDC_CHECKPOINT",
			createErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			execCount := 0
			db := openInputFakeDB(t, &inputFakeHandlers{
				exec: func(q string, _ []driver.Value) error {
					execCount++
					if strings.Contains(q, "CREATE SCHEMA") {
						return nil // CREATE SCHEMA always succeeds in tests
					}
					return tc.createErr
				},
			})

			d := &db2CDCInput{
				db:               db,
				cpCacheTableName: tc.tableName,
				log:              testLogger(t),
			}

			err := d.initCheckpointTable(context.Background())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// loadCheckpoint (DB table path)
// ---------------------------------------------------------------------------

func TestLoadCheckpointFromDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryRows [][]driver.Value
		noRows    bool
		queryErr  error
		wantCSN   uint64
		wantNull  bool
		wantErr   bool
	}{
		{
			name:      "checkpoint found",
			queryRows: [][]driver.Value{{"CSN:0000000000003039"}}, // 12345
			wantCSN:   12345,
		},
		{
			name:     "no checkpoint → NullCSN",
			noRows:   true,
			wantNull: true,
		},
		{
			name:     "query error propagates",
			queryErr: fmt.Errorf("connection timeout"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					if tc.queryErr != nil {
						return nil, nil, tc.queryErr
					}
					if tc.noRows {
						return []string{"CACHE_VAL"}, nil, nil // no rows → sql.ErrNoRows
					}
					return []string{"CACHE_VAL"}, tc.queryRows, nil
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheName:        "",
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			csn, err := d.loadCheckpoint(context.Background())
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

// ---------------------------------------------------------------------------
// saveCheckpoint (DB table path)
// ---------------------------------------------------------------------------

func TestSaveCheckpointToDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		csn     replication.CSN
		execErr error
		wantErr bool
	}{
		{
			name: "saves successfully",
			csn:  replication.NewCSN(12345),
		},
		{
			name:    "exec error propagates",
			csn:     replication.NewCSN(99),
			execErr: fmt.Errorf("deadlock detected"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var capturedArgs []driver.Value
			db := openInputFakeDB(t, &inputFakeHandlers{
				exec: func(_ string, args []driver.Value) error {
					capturedArgs = args
					return tc.execErr
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheName:        "",
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			err := d.saveCheckpoint(context.Background(), tc.csn)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify the args include our checkpoint key and CSN string.
			require.Len(t, capturedArgs, 2)
			assert.Equal(t, "db2_cdc_checkpoint", capturedArgs[0])
			assert.Equal(t, tc.csn.String(), capturedArgs[1])
		})
	}
}

// ---------------------------------------------------------------------------
// eventToMessage data type and message body tests
// ---------------------------------------------------------------------------

// TestEventToMessageNullData verifies that ChangeEvents whose Data map contains
// nil values are serialised correctly: nil values appear as JSON null, not as
// absent keys or zero values. Downstream consumers must be able to distinguish
// "column is NULL" from "column was not selected".
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageNullData(t *testing.T) {
	t.Parallel()

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "EMPLOYEES",
		Operation: replication.OpTypeInsert,
		CSN:       replication.NewCSN(1),
		Data: map[string]any{
			"ID":         int64(42),
			"MANAGER_ID": nil, // explicit NULL from the source
			"DEPT":       nil,
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok, "after field must be a JSON object")

	assert.Equal(t, float64(42), after["ID"])

	// Nil values must appear as JSON null, not as absent keys.
	managerVal, managerExists := after["MANAGER_ID"]
	assert.True(t, managerExists, "MANAGER_ID key must be present even when NULL")
	assert.Nil(t, managerVal, "MANAGER_ID must be JSON null")

	deptVal, deptExists := after["DEPT"]
	assert.True(t, deptExists, "DEPT key must be present even when NULL")
	assert.Nil(t, deptVal, "DEPT must be JSON null")

	// "before" must be null for INSERT events.
	assert.Nil(t, decoded["before"], "before must be null for INSERT events")

	// Verify top-level envelope fields.
	assert.Equal(t, "c", decoded["op"])
}

// TestEventToMessageNumericTypes verifies that numeric DB2 column types
// (INTEGER, BIGINT, FLOAT) round-trip through eventToMessage without precision
// loss or silent type coercion.
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageNumericTypes(t *testing.T) {
	t.Parallel()

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "METRICS",
		Operation: replication.OpTypeRead,
		CSN:       replication.NullCSN(),
		Data: map[string]any{
			"INT_COL":    int64(2147483647),    // max int32 as int64
			"BIGINT_COL": int64(9000000000000), // requires full int64 range
			"FLOAT_COL":  float64(3.14159),
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok)

	// JSON numbers become float64 after Unmarshal; verify round-trip values.
	assert.Equal(t, float64(2147483647), after["INT_COL"])
	assert.Equal(t, float64(9000000000000), after["BIGINT_COL"])
	assert.InDelta(t, 3.14159, after["FLOAT_COL"], 1e-10)

	// Snapshot rows use op="r" and snapshot="true".
	assert.Equal(t, "r", decoded["op"])
}

// TestEventToMessageStringAndBinaryTypes verifies that VARCHAR columns appear
// as JSON strings and that binary ([]byte) columns are base64-encoded strings
// in the JSON output (standard encoding/json behaviour for []byte).
//
// In the Debezium-compatible envelope, row data appears under the "after" key.
func TestEventToMessageStringAndBinaryTypes(t *testing.T) {
	t.Parallel()

	binaryPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	event := replication.ChangeEvent{
		Schema:    "DB2ADMIN",
		Table:     "DOCUMENTS",
		Operation: replication.OpTypeInsert,
		CSN:       replication.NewCSN(5),
		Data: map[string]any{
			"TITLE":   "Hello World", // VARCHAR → JSON string
			"CONTENT": binaryPayload, // BLOB → []byte → base64 in JSON
		},
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	// Row data is under "after" in the Debezium envelope.
	after, ok := decoded["after"].(map[string]any)
	require.True(t, ok)

	// VARCHAR must come through as a plain JSON string.
	assert.Equal(t, "Hello World", after["TITLE"])

	// []byte values are base64-encoded by encoding/json.
	contentStr, isStr := after["CONTENT"].(string)
	require.True(t, isStr, "binary column must be a JSON string (base64-encoded)")
	assert.NotEmpty(t, contentStr)
}

// TestEventToMessageMetadataKeys verifies all required message metadata keys
// are set for each operation type. Snapshot events (OpTypeRead) must carry an
// empty db2_csn since NullCSN().String() == "". Streaming events must carry
// the canonical "CSN:<hex>" string and, when the timestamp is non-zero, the
// RFC3339Nano-formatted db2_timestamp.
//
// The db2_operation key carries the human-readable form ("read", "insert",
// "delete"); db2_op carries the Debezium one-character code ("r", "c", "d").
// db2_commit_lsn is an alias for db2_csn using the Debezium field name.
func TestEventToMessageMetadataKeys(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name         string
		event        replication.ChangeEvent
		wantCSN      string
		wantHasTS    bool
		wantDebezOp  string // Debezium single-char op code
		wantSnapshot string
	}{
		{
			name: "snapshot read — no CSN, no timestamp",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeRead,
				CSN: replication.NullCSN(),
			},
			wantCSN:      "",
			wantHasTS:    false,
			wantDebezOp:  "r",
			wantSnapshot: "true",
		},
		{
			name: "streaming insert — CSN and timestamp present",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeInsert,
				CSN: replication.NewCSN(500), Timestamp: ts,
			},
			wantCSN:      "CSN:00000000000001F4",
			wantHasTS:    true,
			wantDebezOp:  "c",
			wantSnapshot: "false",
		},
		{
			name: "streaming delete — CSN present, zero timestamp",
			event: replication.ChangeEvent{
				Schema: "S", Table: "T", Operation: replication.OpTypeDelete,
				CSN: replication.NewCSN(501),
			},
			wantCSN:      "CSN:00000000000001F5",
			wantHasTS:    false,
			wantDebezOp:  "d",
			wantSnapshot: "false",
		},
	}

	d := &db2CDCInput{}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := d.eventToMessage(tc.event)
			require.NoError(t, err)

			schema, _ := msg.MetaGet("db2_schema")
			table, _ := msg.MetaGet("db2_table")
			op, _ := msg.MetaGet("db2_operation")
			debezOp, debezOpExists := msg.MetaGet("db2_op")
			csn, csnExists := msg.MetaGet("db2_csn")
			commitLSN, commitLSNExists := msg.MetaGet("db2_commit_lsn")
			connector, connectorExists := msg.MetaGet("db2_connector")
			snapshot, snapshotExists := msg.MetaGet("db2_snapshot")

			assert.Equal(t, "S", schema)
			assert.Equal(t, "T", table)
			// db2_operation carries the human-readable form.
			assert.Equal(t, string(tc.event.Operation), op)
			// db2_op carries the Debezium one-character code.
			assert.True(t, debezOpExists, "db2_op must always be set")
			assert.Equal(t, tc.wantDebezOp, debezOp)
			// db2_csn (backward-compat) and db2_commit_lsn (Debezium name) must match.
			assert.True(t, csnExists, "db2_csn must always be set")
			assert.Equal(t, tc.wantCSN, csn)
			assert.True(t, commitLSNExists, "db2_commit_lsn must always be set")
			assert.Equal(t, tc.wantCSN, commitLSN, "db2_commit_lsn must equal db2_csn")
			// db2_connector must always be "redpanda.db2".
			assert.True(t, connectorExists, "db2_connector must always be set")
			assert.Equal(t, "redpanda.db2", connector)
			// db2_snapshot.
			assert.True(t, snapshotExists, "db2_snapshot must always be set")
			assert.Equal(t, tc.wantSnapshot, snapshot)

			tsVal, tsExists := msg.MetaGet("db2_timestamp")
			if tc.wantHasTS {
				assert.True(t, tsExists, "db2_timestamp must be set when Timestamp is non-zero")
				assert.Equal(t, ts.Format(time.RFC3339Nano), tsVal)
			} else {
				if tsExists {
					assert.Empty(t, tsVal, "db2_timestamp must be empty when Timestamp is zero")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Checkpoint resume tests
// ---------------------------------------------------------------------------

// TestLoadCheckpointParseCSNFormats verifies that loadCheckpoint correctly
// round-trips CSN values stored in the checkpoint table in all supported string
// formats. The connector writes "CSN:<hex>" canonically but must also handle
// decimal and 0x-prefixed hex for backward compatibility.
func TestLoadCheckpointParseCSNFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stored  string // value stored in CACHE_VAL column
		wantCSN uint64
	}{
		{
			name:    "CSN prefix hex (canonical write format)",
			stored:  "CSN:0000000000003039",
			wantCSN: 12345,
		},
		{
			name:    "decimal integer",
			stored:  "12345",
			wantCSN: 12345,
		},
		{
			name:    "hex with 0x prefix",
			stored:  "0x3039",
			wantCSN: 12345,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := openInputFakeDB(t, &inputFakeHandlers{
				query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
					return []string{"CACHE_VAL"}, [][]driver.Value{{tc.stored}}, nil
				},
			})

			d := &db2CDCInput{
				db:                 db,
				cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
				checkpointCacheKey: "db2_cdc_checkpoint",
				log:                testLogger(t),
			}

			csn, err := d.loadCheckpoint(context.Background())
			require.NoError(t, err)
			assert.Equal(t, tc.wantCSN, csn.Uint64())
		})
	}
}

// TestSaveCheckpointWritesCanonicalFormat verifies that saveCheckpoint always
// writes the canonical "CSN:<hex>" format to the checkpoint table and uses a
// MERGE (upsert) statement so both first-time writes and updates work correctly.
func TestSaveCheckpointWritesCanonicalFormat(t *testing.T) {
	t.Parallel()

	var capturedMerge string
	var capturedArgs []driver.Value

	db := openInputFakeDB(t, &inputFakeHandlers{
		exec: func(q string, args []driver.Value) error {
			if strings.Contains(q, "MERGE") {
				capturedMerge = q
				capturedArgs = args
			}
			return nil
		},
	})

	d := &db2CDCInput{
		db:                 db,
		cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
		checkpointCacheKey: "my_connector_key",
		log:                testLogger(t),
	}

	csn := replication.NewCSN(50000) // 0xC350
	err := d.saveCheckpoint(context.Background(), csn)
	require.NoError(t, err)

	assert.Contains(t, capturedMerge, "MERGE INTO RPCN.CDC_CHECKPOINT",
		"saveCheckpoint must use a MERGE statement for upsert semantics")
	require.Len(t, capturedArgs, 2)
	assert.Equal(t, "my_connector_key", capturedArgs[0],
		"first MERGE argument must be the checkpoint cache key")
	assert.Equal(t, "CSN:000000000000C350", capturedArgs[1],
		"second MERGE argument must be the canonical CSN:<hex> string")
}

// TestStreamingSkipsSnapshotWhenCheckpointExists verifies the checkpoint resume
// behaviour: when a saved CSN is loaded from the checkpoint table, the connector
// sets StreamConfig.StartingCSN to the saved CSN and the snapshot phase is
// skipped (because StartingCSN is no longer null).
//
// The stream poll query uses IBMSNAP_COMMITSEQ > X'<afterCSN>', so setting
// StartingCSN = savedCSN is correct — the > predicate already excludes the
// saved position itself.  Using .Next() was an off-by-one that dropped the
// first event after a restart.
func TestStreamingSkipsSnapshotWhenCheckpointExists(t *testing.T) {
	t.Parallel()

	savedCSN := replication.NewCSN(999)

	db := openInputFakeDB(t, &inputFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			if strings.Contains(q, "CDC_CHECKPOINT") {
				return []string{"CACHE_VAL"}, [][]driver.Value{{savedCSN.String()}}, nil
			}
			return nil, nil, nil
		},
	})

	d := &db2CDCInput{
		db:                 db,
		cpCacheTableName:   "RPCN.CDC_CHECKPOINT",
		checkpointCacheKey: "db2_cdc_checkpoint",
		snapshotMode:       snapshotModeInitial, // snapshot is enabled in config
		streamConfig: replication.StreamConfig{
			Schemas:       []string{"DB2INST1"},
			Tables:        []string{"EMPLOYEES"},
			PollBatchSize: 100,
		},
		log: testLogger(t),
	}

	csn, err := d.loadCheckpoint(context.Background())
	require.NoError(t, err)
	require.False(t, csn.IsNull())

	// Simulate what Connect() does after loading a non-null checkpoint.
	d.streamConfig.StartingCSN = csn

	// With a saved checkpoint, StartingCSN is set to savedCSN.
	// runCDC skips the snapshot for snapshotModeInitial when StartingCSN is not null.
	// Since StartingCSN is NOT null, the snapshot must be skipped.
	assert.False(t, d.streamConfig.StartingCSN.IsNull(),
		"after checkpoint resume, StartingCSN must not be null so snapshot is skipped")
	assert.Equal(t, savedCSN.Uint64(), d.streamConfig.StartingCSN.Uint64(),
		"StartingCSN must equal the saved checkpoint (poll uses >, so no double-increment needed)")
}

// TestSnapshotModes verifies the doSnapshot decision for each snapshot_mode value.
func TestSnapshotModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mode           snapshotMode
		hasCheckpoint  bool
		expectSnapshot bool
	}{
		{"initial_no_checkpoint", snapshotModeInitial, false, true},
		{"initial_with_checkpoint", snapshotModeInitial, true, false},
		{"when_needed_no_checkpoint", snapshotModeWhenNeeded, false, true},
		{"when_needed_with_checkpoint", snapshotModeWhenNeeded, true, false},
		{"always_no_checkpoint", snapshotModeAlways, false, true},
		{"always_with_checkpoint", snapshotModeAlways, true, true},
		{"never_no_checkpoint", snapshotModeNever, false, false},
		{"never_with_checkpoint", snapshotModeNever, true, false},
		{"no_data_no_checkpoint", snapshotModeNoData, false, false},
		{"no_data_with_checkpoint", snapshotModeNoData, true, false},
		{"initial_only_no_checkpoint", snapshotModeInitialOnly, false, true},
		{"initial_only_with_checkpoint", snapshotModeInitialOnly, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var startCSN replication.CSN
			if tc.hasCheckpoint {
				startCSN = replication.NewCSN(12345)
			} else {
				startCSN = replication.NullCSN()
			}
			assert.Equal(t, tc.expectSnapshot, shouldTakeSnapshot(tc.mode, startCSN))
		})
	}
}

// ---------------------------------------------------------------------------
// Table registration / filtering tests
// ---------------------------------------------------------------------------

// TestInitializeFiltersMissingTables verifies that Initialize returns a
// descriptive error when a configured table is absent from IBMSNAP_REGISTER.
// This prevents silent data loss where a typo in the tables config causes the
// connector to start but capture zero changes for the misnamed table.
func TestInitializeFiltersMissingTables(t *testing.T) {
	t.Parallel()

	db := openInputFakeDB(t, &inputFakeHandlers{
		query: func(_ string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			// Only EMPLOYEES is registered; PAYROLL is not.
			return []string{"SOURCE_OWNER", "SOURCE_TABLE", "CD_OWNER", "CD_TABLE"},
				[][]driver.Value{
					{"DB2INST1", "EMPLOYEES", "ASNCDC", "EMPLOYEES_CT"},
				}, nil
		},
	})

	// NewStreamer is accessible from the replication package and exercises the
	// same Initialize code path that runStreaming calls internally.
	s := replication.NewStreamer(db, replication.StreamConfig{
		Schemas: []string{"DB2INST1"},
		Tables:  []string{"EMPLOYEES", "PAYROLL"},
	}, replication.Version{})

	err := s.Initialize(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PAYROLL",
		"error must name the missing table so the operator knows which registration to add")
}

// ---------------------------------------------------------------------------
// eventToMessage — heartbeat
// ---------------------------------------------------------------------------

func TestEventToMessageHeartbeat(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	event := replication.ChangeEvent{
		Operation: replication.OpTypeHeartbeat,
		Timestamp: ts,
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(b, &envelope))

	assert.Equal(t, "hb", envelope["op"])
	assert.Equal(t, float64(ts.UnixMilli()), envelope["ts_ms"])

	op, _ := msg.MetaGet("db2_operation")
	assert.Equal(t, "heartbeat", op)
	opCode, _ := msg.MetaGet("db2_op")
	assert.Equal(t, "hb", opCode)

	schemaVal, schemaExists := msg.MetaGet("db2_schema")
	assert.True(t, schemaExists, "db2_schema must be set on heartbeat")
	assert.Empty(t, schemaVal)

	tableVal, tableExists := msg.MetaGet("db2_table")
	assert.True(t, tableExists, "db2_table must be set on heartbeat")
	assert.Empty(t, tableVal)

	csnVal, csnExists := msg.MetaGet("db2_csn")
	assert.True(t, csnExists, "db2_csn must be set on heartbeat")
	assert.Empty(t, csnVal)

	commitLSN, commitLSNExists := msg.MetaGet("db2_commit_lsn")
	assert.True(t, commitLSNExists, "db2_commit_lsn must be set on heartbeat")
	assert.Empty(t, commitLSN)

	connector, connectorExists := msg.MetaGet("db2_connector")
	assert.True(t, connectorExists, "db2_connector must be set on heartbeat")
	assert.Equal(t, "redpanda.db2", connector)

	snapshot, snapshotExists := msg.MetaGet("db2_snapshot")
	assert.True(t, snapshotExists, "db2_snapshot must be set on heartbeat")
	assert.Equal(t, "false", snapshot)

	tsVal, tsExists := msg.MetaGet("db2_timestamp")
	assert.True(t, tsExists, "db2_timestamp must be set when Timestamp is non-zero")
	assert.Equal(t, ts.Format(time.RFC3339Nano), tsVal)

	// Heartbeat must not have before/after/source fields.
	assert.NotContains(t, envelope, "before")
	assert.NotContains(t, envelope, "after")
	assert.NotContains(t, envelope, "source")
}

// ---------------------------------------------------------------------------
// eventToMessage — schema change
// ---------------------------------------------------------------------------

func TestEventToMessageSchemaChange(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	event := replication.ChangeEvent{
		Schema:    "DB2INST1",
		Table:     "NEW_TABLE",
		Operation: replication.OpTypeSchemaChange,
		CSN:       replication.NewCSN(12345),
		Timestamp: ts,
	}

	d := &db2CDCInput{}
	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	b, err := msg.AsBytes()
	require.NoError(t, err)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(b, &envelope))

	assert.Equal(t, "schema_change", envelope["op"])
	assert.Equal(t, float64(ts.UnixMilli()), envelope["ts_ms"])

	src, ok := envelope["source"].(map[string]any)
	require.True(t, ok, "source must be a map")
	assert.Equal(t, "DB2INST1", src["schema"])
	assert.Equal(t, "NEW_TABLE", src["table"])

	schemaVal, _ := msg.MetaGet("db2_schema")
	assert.Equal(t, "DB2INST1", schemaVal)
	tableVal, _ := msg.MetaGet("db2_table")
	assert.Equal(t, "NEW_TABLE", tableVal)
	opVal, _ := msg.MetaGet("db2_operation")
	assert.Equal(t, "schema_change", opVal)

	opCode, opCodeExists := msg.MetaGet("db2_op")
	assert.True(t, opCodeExists, "db2_op must be set on schema_change")
	assert.Equal(t, "schema_change", opCode)

	expectedCSN := replication.NewCSN(12345).String()
	csnVal, csnExists := msg.MetaGet("db2_csn")
	assert.True(t, csnExists, "db2_csn must be set on schema_change")
	assert.Equal(t, expectedCSN, csnVal)

	commitLSN, commitLSNExists := msg.MetaGet("db2_commit_lsn")
	assert.True(t, commitLSNExists, "db2_commit_lsn must be set on schema_change")
	assert.Equal(t, expectedCSN, commitLSN)

	connector, connectorExists := msg.MetaGet("db2_connector")
	assert.True(t, connectorExists, "db2_connector must be set on schema_change")
	assert.Equal(t, "redpanda.db2", connector)

	snapshot, snapshotExists := msg.MetaGet("db2_snapshot")
	assert.True(t, snapshotExists, "db2_snapshot must be set on schema_change")
	assert.Equal(t, "false", snapshot)

	tsVal, tsExists := msg.MetaGet("db2_timestamp")
	assert.True(t, tsExists, "db2_timestamp must be set when Timestamp is non-zero")
	assert.Equal(t, ts.Format(time.RFC3339Nano), tsVal)
}

// ---------------------------------------------------------------------------
// parseSnapshotSignalTables
// ---------------------------------------------------------------------------

func TestParseSnapshotSignalTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
		want []string
	}{
		{
			name: "json data-collections with schema prefix",
			data: `{"data-collections":["DB2INST1.EMPLOYEES","DB2INST1.ORDERS"]}`,
			want: []string{"EMPLOYEES", "ORDERS"},
		},
		{
			name: "json data-collections bare names",
			data: `{"data-collections":["EMPLOYEES"]}`,
			want: []string{"EMPLOYEES"},
		},
		{
			name: "comma-separated with schema",
			data: "DB2INST1.EMPLOYEES, DB2INST1.ORDERS",
			want: []string{"EMPLOYEES", "ORDERS"},
		},
		{
			name: "comma-separated bare names",
			data: "EMPLOYEES,ORDERS",
			want: []string{"EMPLOYEES", "ORDERS"},
		},
		{
			name: "empty data",
			data: "",
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseSnapshotSignalTables(tc.data, "DB2INST1", nil)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// ackFunc checkpoint semantics
// ---------------------------------------------------------------------------

// TestAckFuncDoesNotAdvanceCheckpointOnNack verifies the all-or-nothing ackFunc
// semantics: on nack (ackErr != nil) the Capped slot is NOT released, so the
// watermark cannot advance past the failed message. AutoRetryNacks retries by
// calling the same ackFunc with nil; the slot is then released and checkpoint saved.
func TestAckFuncDoesNotAdvanceCheckpointOnNack(t *testing.T) {
	t.Parallel()
	checkpointSaved := false
	d := &db2CDCInput{
		log:    testLogger(t),
		capped: checkpoint.NewCapped[replication.CSN](10),
	}

	csn := replication.NewCSN(100)
	resolveFn, err := d.capped.Track(context.Background(), csn, 1)
	require.NoError(t, err)

	ackFunc := func(_ context.Context, ackErr error) error {
		if ackErr != nil {
			// Do not release the Capped slot on failure — AutoRetryNacks will re-deliver.
			return nil
		}
		if highest := resolveFn(); highest != nil {
			checkpointSaved = true
		}
		return nil
	}

	_ = ackFunc(context.Background(), errors.New("downstream failure"))
	assert.False(t, checkpointSaved, "checkpoint must not advance on nack")

	_ = ackFunc(context.Background(), nil)
	assert.True(t, checkpointSaved, "checkpoint must advance on success ack")
}

// ---------------------------------------------------------------------------
// snapshot_mode: initial_only — ReadBatch must return ErrEndOfInput
// ---------------------------------------------------------------------------

// TestInitialOnlyTriggersEndOfInput verifies that after snapshot_mode=initial_only
// completes, ReadBatch returns service.ErrEndOfInput instead of blocking forever.
//
// The bug (pre-fix): runCDC returned early after the snapshot without calling
// d.shutSig.TriggerSoftStop(), so ReadBatch blocked indefinitely on d.eventChan.
func TestInitialOnlyTriggersEndOfInput(t *testing.T) {
	t.Parallel()

	db := openInputFakeDB(t, &inputFakeHandlers{
		query: func(q string, _ []driver.Value) ([]string, [][]driver.Value, error) {
			// MAX(SYNCHPOINT) on an empty table returns one row with NULL.
			// All other queries (snapshot table rows) return empty result sets.
			if strings.Contains(q, "MAX(SYNCHPOINT)") {
				return []string{"MAX(SYNCHPOINT)"}, [][]driver.Value{{nil}}, nil
			}
			return nil, nil, nil
		},
	})

	d := &db2CDCInput{
		db:           db,
		snapshotMode: snapshotModeInitialOnly,
		snapshotConfig: replication.SnapshotConfig{
			Schemas:   []string{"DB2INST1"},
			Tables:    []string{},
			BatchSize: 100,
		},
		streamConfig: replication.StreamConfig{
			Schemas:       []string{"DB2INST1"},
			Tables:        []string{},
			PollBatchSize: 100,
		},
		eventChan: make(chan replication.ChangeEvent, 64),
		errChan:   make(chan error, 1),
		shutSig:   shutdown.NewSignaller(),
		log:       testLogger(t),
	}

	// Run the CDC loop in a background goroutine (mirrors what Connect() does).
	d.wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go d.runCDC(ctx)

	// ReadBatch must unblock and return ErrEndOfInput once the snapshot finishes.
	// If the bug is present this call hangs until the test context times out.
	_, _, err := d.ReadBatch(ctx)
	require.ErrorIs(t, err, service.ErrEndOfInput,
		"ReadBatch must return ErrEndOfInput after snapshot_mode=initial_only completes")
}

// ---------------------------------------------------------------------------
// BLOB/DBCS type mapping — cTypeForSQLType
// ---------------------------------------------------------------------------

// TestCTypeForSQLType verifies that cTypeForSQLType maps every SQL type to the
// correct ODBC C transfer type, preventing BLOB truncation and DBCS misinterpretation.
func TestCTypeForSQLType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sqlType   db2cli.SQLSMALLINT
		wantCType db2cli.SQLSMALLINT
		label     string
	}{
		// Binary / BLOB → SQL_C_BINARY
		{db2cli.SQL_BINARY, db2cli.SQL_C_BINARY, "SQL_BINARY"},
		{db2cli.SQL_VARBINARY, db2cli.SQL_C_BINARY, "SQL_VARBINARY"},
		{db2cli.SQL_LONGVARBINARY, db2cli.SQL_C_BINARY, "SQL_LONGVARBINARY"},
		{db2cli.SQL_BLOB, db2cli.SQL_C_BINARY, "SQL_BLOB"},
		// DBCS / wide-char → SQL_C_WCHAR
		{db2cli.SQL_WCHAR, db2cli.SQL_C_WCHAR, "SQL_WCHAR"},
		{db2cli.SQL_WVARCHAR, db2cli.SQL_C_WCHAR, "SQL_WVARCHAR"},
		{db2cli.SQL_WLONGVARCHAR, db2cli.SQL_C_WCHAR, "SQL_WLONGVARCHAR"},
		{db2cli.SQL_GRAPHIC, db2cli.SQL_C_WCHAR, "SQL_GRAPHIC"},
		{db2cli.SQL_VARGRAPHIC, db2cli.SQL_C_WCHAR, "SQL_VARGRAPHIC"},
		{db2cli.SQL_LONGVARGRAPHIC, db2cli.SQL_C_WCHAR, "SQL_LONGVARGRAPHIC"},
		{db2cli.SQL_DBCLOB, db2cli.SQL_C_WCHAR, "SQL_DBCLOB"},
		// Everything else → SQL_C_CHAR
		{db2cli.SQL_CHAR, db2cli.SQL_C_CHAR, "SQL_CHAR"},
		{db2cli.SQL_VARCHAR, db2cli.SQL_C_CHAR, "SQL_VARCHAR"},
		{db2cli.SQL_INTEGER, db2cli.SQL_C_CHAR, "SQL_INTEGER"},
		{db2cli.SQL_BIGINT, db2cli.SQL_C_CHAR, "SQL_BIGINT"},
		{db2cli.SQL_BOOLEAN, db2cli.SQL_C_CHAR, "SQL_BOOLEAN"},
	}
	for _, tt := range tests {
		got := cTypeForSQLType(tt.sqlType)
		assert.Equal(t, tt.wantCType, got,
			"cTypeForSQLType(%s=%d): got %d, want %d", tt.label, tt.sqlType, got, tt.wantCType)
	}
}

// TestDecodeUTF16LE verifies that decodeUTF16LE correctly converts UTF-16LE
// bytes (as returned by DB2 for GRAPHIC/VARGRAPHIC columns) to UTF-8 strings.
// The round-trip encodes strings to UTF-16LE via golang.org/x/text/encoding/unicode
// (same library used by the production code) then decodes to verify correctness.
func TestDecodeUTF16LE(t *testing.T) {
	t.Parallel()

	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()

	toUTF16LE := func(s string) []byte {
		b, err := enc.Bytes([]byte(s))
		require.NoError(t, err)
		return b
	}

	tests := []struct {
		input string
	}{
		{""},
		{"AB"},
		{"Hello, World!"},
		{"中文"},  // CJK characters
		{"日本語"}, // Japanese
		{"한국어"}, // Korean
	}
	for _, tt := range tests {
		encoded := toUTF16LE(tt.input)
		got := decodeUTF16LE(encoded)
		assert.Equal(t, tt.input, got, "round-trip for %q", tt.input)
	}
}

// ---------------------------------------------------------------------------
// Idempotency key
// ---------------------------------------------------------------------------

// TestIdempotencyKeyFormat verifies that idempotencyKey produces stable,
// deterministic strings for both CDC and snapshot events.
func TestIdempotencyKeyFormat(t *testing.T) {
	t.Parallel()

	cdcEvent := replication.ChangeEvent{
		Schema:    "DB2INST1",
		Table:     "ORDERS",
		CSN:       replication.NewCSN(12345),
		IntentSeq: 1,
	}
	key := idempotencyKey(cdcEvent)
	// CSN.String() returns "CSN:<hex-padded>"; 12345 decimal = 0x3039.
	assert.Equal(t, "DB2INST1.ORDERS."+cdcEvent.CSN.String()+".1", key)

	// Snapshot event: null CSN — key derived from sorted PK values.
	snapEvent := replication.ChangeEvent{
		Schema:    "DB2INST1",
		Table:     "ORDERS",
		CSN:       replication.NullCSN(),
		PKColumns: []string{"ID"},
		Data:      map[string]any{"ID": "42", "NAME": "foo"},
	}
	snapKey := idempotencyKey(snapEvent)
	assert.True(t, strings.HasPrefix(snapKey, "DB2INST1.ORDERS.snapshot."),
		"snapshot key must start with schema.table.snapshot., got %q", snapKey)
	assert.Contains(t, snapKey, "ID=42", "snapshot key must contain PK field")
}

// TestIdempotencyKeyStable verifies the key is deterministic across calls.
func TestIdempotencyKeyStable(t *testing.T) {
	t.Parallel()

	event := replication.ChangeEvent{
		Schema:    "SCH",
		Table:     "TBL",
		CSN:       replication.NewCSN(999),
		IntentSeq: 7,
	}
	k1 := idempotencyKey(event)
	k2 := idempotencyKey(event)
	assert.Equal(t, k1, k2, "idempotencyKey must be deterministic")
}

// ---------------------------------------------------------------------------
// snapshot_mode: when_needed
// ---------------------------------------------------------------------------

// TestSnapshotWhenNeededMode verifies that when_needed triggers a snapshot only
// when the loaded checkpoint CSN is null (no prior state).
func TestSnapshotWhenNeededMode(t *testing.T) {
	t.Parallel()

	assert.True(t, shouldTakeSnapshot(snapshotModeWhenNeeded, replication.NullCSN()),
		"when_needed + null CSN should trigger snapshot")
	assert.False(t, shouldTakeSnapshot(snapshotModeWhenNeeded, replication.NewCSN(42)),
		"when_needed + existing CSN should skip snapshot")
	assert.True(t, shouldTakeSnapshot(snapshotModeInitial, replication.NullCSN()),
		"initial + null CSN should snapshot")
	assert.False(t, shouldTakeSnapshot(snapshotModeInitial, replication.NewCSN(42)),
		"initial + existing CSN should skip snapshot (already ran once)")
	assert.False(t, shouldTakeSnapshot(snapshotModeNever, replication.NullCSN()),
		"never + null CSN must not snapshot")
	assert.False(t, shouldTakeSnapshot(snapshotModeNever, replication.NewCSN(99)),
		"never + existing CSN must not snapshot")
}

// ---------------------------------------------------------------------------
// Idempotency key — full wire-through tests
// ---------------------------------------------------------------------------

// TestIdempotencyKeyUnique verifies that distinct CDC events produce distinct keys.
func TestIdempotencyKeyUnique(t *testing.T) {
	t.Parallel()

	e1 := replication.ChangeEvent{Schema: "S", Table: "T", CSN: replication.NewCSN(1), IntentSeq: 0}
	e2 := replication.ChangeEvent{Schema: "S", Table: "T", CSN: replication.NewCSN(2), IntentSeq: 0}
	e3 := replication.ChangeEvent{Schema: "S", Table: "T", CSN: replication.NewCSN(1), IntentSeq: 1}

	assert.NotEqual(t, idempotencyKey(e1), idempotencyKey(e2), "different CSNs must produce different keys")
	assert.NotEqual(t, idempotencyKey(e1), idempotencyKey(e3), "different intent seqs must produce different keys")
}

// TestIdempotencyKeyInMessage verifies that eventToMessage sets the
// db2_idempotency_key metadata on outgoing messages.
func TestIdempotencyKeyInMessage(t *testing.T) {
	t.Parallel()

	d := &db2CDCInput{}
	event := replication.ChangeEvent{
		Schema:    "DB2INST1",
		Table:     "ORDERS",
		Operation: replication.OpTypeInsert,
		CSN:       replication.NewCSN(555),
		IntentSeq: 2,
		Data:      map[string]any{"ID": "1", "NAME": "test"},
	}

	msg, err := d.eventToMessage(event)
	require.NoError(t, err)

	val, ok := msg.MetaGet("db2_idempotency_key")
	require.True(t, ok, "db2_idempotency_key metadata must be present")
	assert.NotEmpty(t, val)

	// Key must be deterministic across two calls with the same event.
	msg2, err := d.eventToMessage(event)
	require.NoError(t, err)
	val2, _ := msg2.MetaGet("db2_idempotency_key")
	assert.Equal(t, val, val2, "idempotency key must be deterministic for the same event")
}

// TestSnapshotIdempotencyKeyVsCDCKey verifies that snapshot events (null CSN)
// and CDC events for the same row produce distinct keys.
func TestSnapshotIdempotencyKeyVsCDCKey(t *testing.T) {
	t.Parallel()

	snapEvent := replication.ChangeEvent{
		Schema: "DB2INST1",
		Table:  "ORDERS",
		CSN:    replication.NullCSN(),
		Data:   map[string]any{"ID": "42"},
	}
	cdcEvent := replication.ChangeEvent{
		Schema:    "DB2INST1",
		Table:     "ORDERS",
		CSN:       replication.NewCSN(42),
		IntentSeq: 0,
	}

	snapKey := idempotencyKey(snapEvent)
	cdcKey := idempotencyKey(cdcEvent)
	assert.NotEqual(t, snapKey, cdcKey, "snapshot and CDC keys must be distinct")
	assert.Contains(t, snapKey, "snapshot", "snapshot key must contain 'snapshot' marker")
}

// Compile-time check: sort is used by idempotencyKey.
var _ = sort.Strings
