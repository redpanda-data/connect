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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
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
	"github.com/redpanda-data/connect/v4/internal/license"
)

// capturedMessage holds a single emitted CDC message decoded for type analysis
// alongside the common schema that travelled with it in metadata.
type capturedMessage struct {
	// body is the JSON payload decoded with UseNumber() so that a bare JSON
	// number surfaces as json.Number and a quoted JSON string surfaces as a Go
	// string — exactly the distinction that the schema_registry_encode Avro
	// path is sensitive to.
	body   map[string]any
	schema schema.Common
	// operation is the message's "operation" metadata ("read" for snapshot
	// rows, "insert"/"update"/"delete" for streamed rows) — used to assert a
	// phase actually exercised the code path it claims to.
	operation string
}

// decodeWithNumber mirrors how benthos parses message bytes downstream (via
// json.Decoder.UseNumber), so we observe the same Go types a downstream
// processor such as schema_registry_encode would see.
func decodeWithNumber(t *testing.T, raw string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	require.NoError(t, dec.Decode(&m), "decoding message body %q", raw)
	return m
}

// TestIntegrationOracleDBCDCDataTypeConsistency verifies that the snapshot
// (sql.Scan) and streaming (LogMiner) paths agree on both the yielded schema
// and the Go type of every column value, and that the value is consistent with
// the schema. In particular decimal columns must surface as canonical string
// values (never a bare JSON number / json.Number), since downstream Avro
// encoding rejects json.Number for string-typed fields.
func TestIntegrationOracleDBCDCDataTypeConsistency(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)

	const fullTable = "testdb.all_types"
	create := `CREATE TABLE testdb.all_types (
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
		rw          RAW(16)
	)`
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), fullTable, create))

	// A single literal INSERT so we control exactly what Oracle stores and what
	// LogMiner SQL_REDO reports. Used once before launch (snapshot) and once
	// after launch (streaming).
	insertSQL := `INSERT INTO testdb.all_types
		(num_plain, num_38, num_38_0, num_38_2, num_10_2, num_5_0, num_star_2, num_neg, num_int, flt, bin_float, bin_double, vc, ch, nvc, dt, ts, ts_tz, rw)
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
			HEXTORAW('48656C6C6F')
		)`

	// An UPDATE whose SET clause assigns bare numeric literals (including
	// integer-valued assignments to fractional decimal columns and negatives).
	// UPDATE SET redo is the path most likely to surface bare numerics that the
	// streaming converter turns into int64/json.Number.
	updateSQL := `UPDATE testdb.all_types SET
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

	var (
		mu       sync.Mutex
		captured []capturedMessage
		stream   *service.Stream
		err      error
	)

	collect := func(_ context.Context, mb service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range mb {
			b, aErr := msg.AsBytes()
			assert.NoError(t, aErr)
			op, _ := msg.MetaGet("operation")
			captured = append(captured, capturedMessage{
				body:      decodeWithNumber(t, string(b)),
				schema:    oracledbtest.ExtractSchema(t, msg),
				operation: op,
			})
		}
		return nil
	}

	// waitForOperation drains captured messages until one with the wanted
	// operation arrives, discarding others (checkpointing is at-least-once, so
	// a restarted stream may redeliver earlier events first). Capturing an
	// operation in forbidden fails immediately: each phase must prove it
	// exercised the code path it claims to — e.g. the post-restart leg forbids
	// "read", because a re-run snapshot would make it silently re-test the
	// snapshot-seeded path instead of the catalog-derived one.
	waitForOperation := func(want string, forbidden ...string) capturedMessage {
		t.Helper()
		var (
			msg   capturedMessage
			badOp string
		)
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			for len(captured) > 0 {
				m := captured[0]
				captured = captured[1:]
				if slices.Contains(forbidden, m.operation) {
					badOp = m.operation
					return true
				}
				if m.operation == want {
					msg = m
					captured = nil
					return true
				}
			}
			return false
		}, time.Minute*5, time.Second, "waiting for a %q message", want)
		require.Emptyf(t, badOp, "captured forbidden operation %q while waiting for %q", badOp, want)
		return msg
	}

	cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
    min_scn_window_size: 0
    backoff_interval: 1s
  include: ["TESTDB.ALL_TYPES"]`

	{
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: WARN`))
		require.NoError(t, streamBuilder.AddBatchConsumerFunc(collect))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if rErr := stream.Run(t.Context()); rErr != nil && !errors.Is(rErr, context.Canceled) {
				t.Error(rErr)
			}
		}()
	}

	// Capture one message per phase: snapshot read, streaming INSERT, streaming
	// UPDATE — each pinned to its operation so a duplicate delivery of an
	// earlier event can't silently substitute for the phase under test.
	phases := map[string]capturedMessage{}
	phases["snapshot"] = waitForOperation("read")

	db.MustExec(insertSQL)
	phases["stream-insert"] = waitForOperation("insert")

	db.MustExec(updateSQL)
	phases["stream-update"] = waitForOperation("update")

	require.NoError(t, stream.StopWithin(time.Second*10))

	// Restart leg: rebuild an identical stream, which resumes from the
	// persisted SCN checkpoint and therefore SKIPS the snapshot. With no
	// snapshot to seed the schema cache from driver column metadata, every
	// message now carries the schema derived from ALL_TAB_COLUMNS — exactly
	// what happens on a real connector restart. A divergence between the two
	// schema sources (e.g. catalog "TIMESTAMP(6)" vs driver "TimeStampDTY")
	// only surfaces on this leg: within a single process lifetime streaming
	// reuses the snapshot-seeded schema and the catalog mapping never appears
	// in any published message.
	mu.Lock()
	captured = nil
	mu.Unlock()

	{
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.AddInputYAML(fmt.Sprintf(cfg, connStr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: WARN`))
		require.NoError(t, streamBuilder.AddBatchConsumerFunc(collect))

		stream, err = streamBuilder.Build()
		require.NoError(t, err)
		license.InjectTestService(stream.Resources())

		go func() {
			if rErr := stream.Run(t.Context()); rErr != nil && !errors.Is(rErr, context.Canceled) {
				t.Error(rErr)
			}
		}()
	}

	// The post-restart phase must capture the fresh INSERT and must never see
	// a snapshot read: if the checkpoint wasn't persisted or resumed, the
	// second stream re-runs the snapshot and this leg silently degrades into a
	// duplicate of the snapshot phase — passing every schema assertion below
	// without exercising the catalog-derived schema path it exists to cover.
	// Redelivered streaming events (at-least-once) are drained and discarded.
	db.MustExec(insertSQL)
	phases["stream-post-restart"] = waitForOperation("insert", "read")

	require.NoError(t, stream.StopWithin(time.Second*10))

	phaseNames := []string{"snapshot", "stream-insert", "stream-update", "stream-post-restart"}

	// Use the snapshot schema as the reference for column types; assert all
	// phases that carry a schema agree with it.
	refSchema := childSchemaMap(phases["snapshot"].schema)
	require.NotEmpty(t, refSchema, "snapshot message carried no schema")

	// Gather the union of columns observed across every phase.
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

	for _, col := range sortedCols {
		t.Run(col, func(t *testing.T) {
			// The FULL column schema — type, optionality, and logical decimal
			// precision/scale — must agree across all phases that carry one.
			// Comparing only the CommonType would let a Decimal(38,2) vs
			// Decimal(22,2) flip between the snapshot-seeded and
			// catalog-derived paths pass unnoticed, even though Avro decimal
			// precision/scale are part of the type and such a flip is exactly
			// the Schema Registry incompatibility this test exists to catch.
			for _, p := range phaseNames {
				ps := childSchemaMap(phases[p].schema)
				if len(ps) == 0 {
					continue
				}
				assert.Equalf(t, refSchema[col], ps[col],
					"schema for %q in phase %q differs from snapshot", col, p)
			}

			// Establish the reference Go type from the snapshot value, then
			// require every other phase to match it (NULLs excepted).
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

			// Decimal / BigDecimal columns must be canonical strings — never a
			// bare JSON number (json.Number) — in EVERY phase. This is the bug
			// under test: a leaked json.Number breaks downstream Avro encoding
			// into string-typed fields.
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

			// Timestamp columns must render as RFC 3339 in EVERY phase.
			// Matching Go types (%T == string) alone would let the streaming
			// coercion leave a raw redo rendering ("2024-01-15 10:30:00") in
			// place, which downstream Avro timestamp encoding rejects.
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
