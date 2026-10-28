// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	oracledbtest "github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
)

// updateGolden rewrites testdata/schema_golden.json from the snapshot phase's
// schema instead of comparing against it. Only use it when an intentional
// change to the emitted schema has been made.
var updateGolden = flag.Bool("update", false, "rewrite testdata/schema_golden.json from the snapshot schema instead of comparing")

const schemaGoldenPath = "testdata/schema_golden.json"

// TestIntegrationOracleDBCDCDataTypeConsistency verifies that the snapshot and
// streaming (LogMiner) paths agree on the schema and the Go type of every
// column value. Decimals must be strings, not json.Number, which breaks Avro
// encoding.
// go test ./internal/impl/oracledb/ -run TestIntegrationOracleDBCDCDataTypeConsistencyGolden -update
func TestIntegrationOracleDBCDCDataTypeConsistencyGolden(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// decodeWithNumber mirrors downstream json.Decoder.UseNumber parsing.
	decodeWithNumber := func(t *testing.T, raw string) map[string]any {
		t.Helper()
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m), "decoding message body %q", raw)
		return m
	}

	t.Log("Creating all_types table and seeding the snapshot row")
	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)

	fullTable := db.Schema + ".all_types"
	create := `CREATE TABLE ` + db.Schema + `.all_types (
	id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	num_plain   NUMBER,
	num_38      NUMBER(38),
	num_38_0    NUMBER(38,0),
	num_38_2    NUMBER(38,2),
	num_10_2    NUMBER(10,2),
	num_5_0     NUMBER(5,0),
	num_star_2  NUMBER(*,2),
	num_neg     NUMBER(5,-2),
	num_int     INTEGER,
	flt         FLOAT,
	bin_float   BINARY_FLOAT,
	bin_double  BINARY_DOUBLE,
	vc          VARCHAR2(100),
	ch          CHAR(5),
	nvc         NVARCHAR2(100),
	dt          DATE,
	ts          TIMESTAMP,
	ts_tz       TIMESTAMP WITH TIME ZONE,
	ts_ltz      TIMESTAMP WITH LOCAL TIME ZONE,
	ts_9        TIMESTAMP(9),
	ts_frac     TIMESTAMP(6),
	nch         NCHAR(5),
	rw          RAW(16)
)`
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), fullTable, create))

	// A literal INSERT controls exactly what LogMiner SQL_REDO reports.
	insertSQL := `INSERT INTO ` + db.Schema + `.all_types
	(num_plain, num_38, num_38_0, num_38_2, num_10_2, num_5_0, num_star_2, num_neg, num_int, flt, bin_float, bin_double, vc, ch, nvc, dt, ts, ts_tz, ts_ltz, ts_9, ts_frac, nch, rw)
	VALUES (
		12345.678,
		123456789012345678901234567890,
		678,
		12.34,
		56.78,
		42,
		78.91,
		1234,
		99,
		3.5,
		1.5,
		2.5,
		'hello', 'abc', 'world',
		TO_DATE('2024-01-15','YYYY-MM-DD'),
		TO_TIMESTAMP('2024-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS'),
		TO_TIMESTAMP_TZ('2024-01-15 10:30:00.000000 +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'),
		TO_TIMESTAMP_TZ('2024-01-15 10:30:00.000000 +05:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'),
		TO_TIMESTAMP('2024-01-15 10:30:00.123456789','YYYY-MM-DD HH24:MI:SS.FF9'),
		TO_TIMESTAMP('2024-01-15 10:30:00.123456','YYYY-MM-DD HH24:MI:SS.FF6'),
		N'nch',
		HEXTORAW('48656C6C6F')
	)`

	// Assigns bare numeric literals, the redo most likely to leak int64/json.Number.
	updateSQL := `UPDATE ` + db.Schema + `.all_types SET
	num_plain  = 100,
	num_38     = 200,
	num_38_0   = 300,
	num_38_2   = 5,
	num_10_2   = -7,
	num_5_0    = -42,
	num_star_2 = 3,
	num_neg    = 8765,
	num_int    = 0,
	flt        = 9,
	vc         = 'updated'`

	// Seed one row for the snapshot.
	db.MustExec(insertSQL)

	t.Log("Preparing the capture helpers and pipeline config")
	// capturedMessage is an emitted CDC message plus the common schema from its metadata.
	type capturedMessage struct {
		body      map[string]any
		schema    schema.Common
		operation string
	}
	var captured oracledbtest.MsgBatch

	// decodeMessage converts a raw emitted message into a capturedMessage.
	decodeMessage := func(msg *service.Message) capturedMessage {
		t.Helper()
		b, aErr := msg.AsBytes()
		assert.NoError(t, aErr)
		op, _ := msg.MetaGet("operation")
		return capturedMessage{
			body:      decodeWithNumber(t, string(b)),
			schema:    oracledbtest.ExtractSchema(t, msg),
			operation: op,
		}
	}

	// waitForOperation discards messages until one with the wanted operation
	// arrives (delivery is at-least-once). Seeing a forbidden operation fails
	// immediately.
	waitForOperation := func(want string, forbidden ...string) capturedMessage {
		t.Helper()
		var (
			found *service.Message
			badOp string
		)
		require.Eventually(t, func() bool {
			captured.Lock()
			defer captured.Unlock()
			for len(captured.Msgs) > 0 {
				m := captured.Msgs[0]
				captured.Msgs = captured.Msgs[1:]
				op, _ := m.MetaGet("operation")
				if slices.Contains(forbidden, op) {
					badOp = op
					return true
				}
				if op == want {
					found = m
					captured.Msgs = nil
					return true
				}
			}
			return false
		}, time.Minute*5, time.Second, "waiting for a %q message", want)
		require.Emptyf(t, badOp, "captured forbidden operation %q while waiting for %q", badOp, want)
		return decodeMessage(found)
	}

	cfg := `
oracledb_cdc:
  connection_string: %s
  checkpoint_cache_table_name: ` + db.CheckpointTable() + `
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["` + db.Schema + `.ALL_TYPES"]`

	t.Log("Starting pipeline and capturing the snapshot read")
	stream := oracledbtest.StartPipelineWithLogLevel(t, fmt.Sprintf(cfg, connStr), "WARN", captured.Consumer())

	phases := map[string]capturedMessage{}
	// Each phase is pinned to an operation so a redelivered earlier event
	// can't stand in for it.
	phases["snapshot"] = waitForOperation("read")

	t.Log("Streaming an INSERT and an UPDATE")
	db.MustExec(insertSQL)
	phases["stream-insert"] = waitForOperation("insert")

	db.MustExec(updateSQL)
	phases["stream-update"] = waitForOperation("update")

	require.NoError(t, stream.StopWithin(time.Second*10))

	t.Log("Restarting from the checkpoint to force the catalog schema path")
	// Restart leg: resuming from the checkpoint skips the snapshot, forcing the
	// ALL_TAB_COLUMNS schema path. Divergence from the driver-derived schema
	// (e.g. "TIMESTAMP(6)" vs "TimeStampDTY") only surfaces here.
	captured.Reset()

	stream = oracledbtest.StartPipelineWithLogLevel(t, fmt.Sprintf(cfg, connStr), "WARN", captured.Consumer())

	// Forbid "read": a re-run snapshot would silently skip the catalog path.
	db.MustExec(insertSQL)
	phases["stream-post-restart"] = waitForOperation("insert", "read")

	require.NoError(t, stream.StopWithin(time.Second*10))

	t.Log("Collecting columns and logging each phase's value types")
	phaseNames := []string{"snapshot", "stream-insert", "stream-update", "stream-post-restart"}

	// The snapshot schema is the reference for all phases.
	refSchema := childSchemaMap(phases["snapshot"].schema)
	require.NotEmpty(t, refSchema, "snapshot message carried no schema")

	cols := map[string]struct{}{}
	for _, p := range phaseNames {
		for k := range phases[p].body {
			cols[k] = struct{}{}
		}
	}
	sortedCols := make([]string, 0, len(cols))
	for k := range cols {
		sortedCols = append(sortedCols, k)
	}
	sort.Strings(sortedCols)

	// Diagnostic table: value Go type per column per phase.
	{
		var b strings.Builder
		fmt.Fprintf(&b, "%-14s | %-12s", "COLUMN", "schema")
		for _, p := range phaseNames {
			fmt.Fprintf(&b, " | %-18s", p)
		}
		t.Log(b.String())
	}
	for _, col := range sortedCols {
		var b strings.Builder
		fmt.Fprintf(&b, "%-14s | %-14s", col, describeColumnSchema(refSchema[col]))
		for _, p := range phaseNames {
			v, ok := phases[p].body[col]
			if !ok {
				fmt.Fprintf(&b, " | %-18s", "(absent)")
				continue
			}
			fmt.Fprintf(&b, " | %-18s", fmt.Sprintf("%T", v))
		}
		t.Log(b.String())
	}

	t.Log("Comparing each phase's schema and values against the snapshot and golden file")
	// The cross-phase checks are relative parity only; the golden file pins
	// the absolute emitted schema.
	if *updateGolden {
		writeSchemaGolden(t, refSchema)
	}
	golden := loadSchemaGolden(t)

	for _, col := range sortedCols {
		t.Run(col, func(t *testing.T) {
			for _, p := range phaseNames {
				ps := childSchemaMap(phases[p].schema)
				if len(ps) == 0 {
					continue
				}
				got, ok := ps[col]
				if !ok {
					continue
				}
				want, ok := golden[col]
				if !assert.Truef(t, ok, "column %q (phase %q) has no entry in %s", col, p, schemaGoldenPath) {
					continue
				}
				assert.Equalf(t, want, schemaGoldenJSON(t, got),
					"emitted schema for column %q changed in phase %q. This alters the schema that "+
						"schema_registry_encode registers. If intended, rerun with -update and add an "+
						"upgrade note to CHANGELOG.md", col, p)
			}

			// Compare the full schema, not just the type: a decimal precision/scale
			// flip between paths is a Schema Registry incompatibility.
			for _, p := range phaseNames {
				ps := childSchemaMap(phases[p].schema)
				if len(ps) == 0 {
					continue
				}
				assert.Equalf(t, refSchema[col], ps[col],
					"schema for %q in phase %q differs from snapshot", col, p)
			}

			// Every phase's Go type must match the snapshot's (NULLs excepted).
			refVal, refOK := phases["snapshot"].body[col]
			refType := fmt.Sprintf("%T", refVal)
			for _, p := range phaseNames {
				v, ok := phases[p].body[col]
				if !ok || v == nil || !refOK || refVal == nil {
					continue
				}
				assert.Equalf(t, refType, fmt.Sprintf("%T", v),
					"value Go type for %q in phase %q (%v) differs from snapshot (%v)",
					col, p, v, refVal)
			}

			// Decimals must be strings, not json.Number, which breaks Avro encoding.
			if ct := refSchema[col].Type; ct == schema.Decimal || ct == schema.BigDecimal {
				for _, p := range phaseNames {
					v, ok := phases[p].body[col]
					if !ok || v == nil {
						continue
					}
					_, isStr := v.(string)
					assert.Truef(t, isStr, "decimal column %q in phase %q is %T, want string", col, p, v)
				}
			}

			// Timestamps must be RFC 3339; a type-only check would let a raw redo
			// rendering ("2024-01-15 10:30:00") through.
			if refSchema[col].Type == schema.Timestamp {
				for _, p := range phaseNames {
					v, ok := phases[p].body[col]
					if !ok || v == nil {
						continue
					}
					s, isStr := v.(string)
					if assert.Truef(t, isStr, "timestamp column %q in phase %q is %T, want string", col, p, v) {
						_, perr := time.Parse(time.RFC3339, s)
						assert.NoErrorf(t, perr, "timestamp column %q in phase %q is not RFC 3339: %q", col, p, s)
					}
				}
			}

			// The streamed value comes from an offset-less TO_TIMESTAMP_TZ rendered
			// in the session zone (UTC), not from the insert's +05:00 offset.
			if col == "TS_LTZ" {
				for _, p := range phaseNames {
					assert.Equalf(t, "2024-01-15T05:30:00Z", phases[p].body[col],
						"ts_ltz in phase %q not normalised to UTC", p)
				}
			}

			// Checks the snapshot/LogMiner parse path, not the Avro micros fix,
			// which the golden unit pins.
			if col == "TS_FRAC" {
				for _, p := range phaseNames {
					assert.Equalf(t, "2024-01-15T10:30:00.123456Z", phases[p].body[col],
						"ts_frac in phase %q lost fractional seconds", p)
				}
			}
		})
	}
}

// childSchemaMap indexes a record schema's children by name, keeping the FULL
// schema.Common per column (type, optionality, logical decimal
// precision/scale) so cross-phase comparisons catch flips in any of them —
// not just in the CommonType.
func childSchemaMap(c schema.Common) map[string]schema.Common {
	out := map[string]schema.Common{}
	for i := range c.Children {
		out[c.Children[i].Name] = c.Children[i]
	}
	return out
}

// describeColumnSchema renders a column schema for the diagnostic table,
// including decimal precision/scale when present (e.g. "DECIMAL(38,2)").
func describeColumnSchema(c schema.Common) string {
	if c.Logical != nil && c.Logical.Decimal != nil {
		return fmt.Sprintf("%s(%d,%d)", c.Type.String(), c.Logical.Decimal.Precision, c.Logical.Decimal.Scale)
	}
	return c.Type.String()
}

// schemaGoldenJSON renders a column schema as canonical JSON via ToAny(),
// dropping the derived "fingerprint" key (recursively) to keep diffs quiet.
// Decimal precision/scale and timestamp unit/adjust_to_utc are included.
func schemaGoldenJSON(t *testing.T, c schema.Common) string {
	t.Helper()
	b, err := json.MarshalIndent(stripFingerprint(c.ToAny()), "", "  ")
	require.NoError(t, err)
	return string(b)
}

func stripFingerprint(v any) any {
	switch m := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(m))
		for k, e := range m {
			if k == "fingerprint" {
				continue
			}
			out[k] = stripFingerprint(e)
		}
		return out
	case []any:
		out := make([]any, len(m))
		for i, e := range m {
			out[i] = stripFingerprint(e)
		}
		return out
	}
	return v
}

func writeSchemaGolden(t *testing.T, cols map[string]schema.Common) {
	t.Helper()
	out := make(map[string]json.RawMessage, len(cols))
	for name, c := range cols {
		out[name] = json.RawMessage(schemaGoldenJSON(t, c))
	}
	b, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(schemaGoldenPath), 0o755))
	require.NoError(t, os.WriteFile(schemaGoldenPath, append(b, '\n'), 0o644))
	t.Logf("rewrote %s", schemaGoldenPath)
}

// loadSchemaGolden returns the golden per-column schemas, re-indented to the
// same canonical form as schemaGoldenJSON.
func loadSchemaGolden(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile(schemaGoldenPath)
	require.NoError(t, err, "reading golden schema; generate it with -update")
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	out := make(map[string]string, len(raw))
	for name, v := range raw {
		nb, err := json.MarshalIndent(v, "", "  ")
		require.NoError(t, err)
		out[name] = string(nb)
	}
	return out
}
