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
		(num_plain, num_38, num_38_0, num_38_2, num_10_2, num_5_0, num_int, flt, bin_float, bin_double, vc, ch, nvc, dt, ts, ts_tz, rw)
		VALUES (
			12345.678,
			123456789012345678901234567890,
			678,
			12.34,
			56.78,
			42,
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
			captured = append(captured, capturedMessage{
				body:   decodeWithNumber(t, string(b)),
				schema: oracledbtest.ExtractSchema(t, msg),
			})
		}
		return nil
	}

	waitForMessage := func() capturedMessage {
		t.Helper()
		assert.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(captured) >= 1
		}, time.Minute*5, time.Second)
		mu.Lock()
		defer mu.Unlock()
		require.GreaterOrEqual(t, len(captured), 1)
		msg := captured[0]
		captured = nil
		return msg
	}

	cfg := `
oracledb_cdc:
  connection_string: %s
  stream_snapshot: true
  snapshot_max_batch_size: 10
  logminer:
    scn_window_size: 20000
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

	// Capture one message per phase: snapshot read, streaming INSERT, streaming UPDATE.
	phases := map[string]capturedMessage{}
	phases["snapshot"] = waitForMessage()

	db.MustExec(insertSQL)
	phases["stream-insert"] = waitForMessage()

	db.MustExec(updateSQL)
	phases["stream-update"] = waitForMessage()

	require.NoError(t, stream.StopWithin(time.Second*10))

	phaseNames := []string{"snapshot", "stream-insert", "stream-update"}

	// Use the snapshot schema as the reference for column types; assert all
	// phases that carry a schema agree with it.
	refSchema := childTypeMap(phases["snapshot"].schema)
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
		fmt.Fprintf(&b, "%-14s | %-12s", col, refSchema[col].String())
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
			// Schema for the column must agree across all phases that carry one.
			for _, p := range phaseNames {
				ps := childTypeMap(phases[p].schema)
				if len(ps) == 0 {
					continue
				}
				assert.Equalf(t, refSchema[col], ps[col],
					"schema type for %q in phase %q differs from snapshot", col, p)
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
			if ct := refSchema[col]; ct == schema.Decimal || ct == schema.BigDecimal {
				for _, p := range phaseNames {
					v, ok := phases[p].body[col]
					if !ok || v == nil {
						continue
					}
					_, isStr := v.(string)
					assert.Truef(t, isStr, "decimal column %q in phase %q is %T, want string", col, p, v)
				}
			}
		})
	}
}

// childTypeMap indexes a record schema's children by name → CommonType.
func childTypeMap(c schema.Common) map[string]schema.CommonType {
	out := map[string]schema.CommonType{}
	for i := range c.Children {
		out[c.Children[i].Name] = c.Children[i].Type
	}
	return out
}
