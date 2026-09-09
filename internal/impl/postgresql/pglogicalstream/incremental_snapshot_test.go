// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// fakeQueryDriver is a minimal database/sql driver that ignores whatever SQL
// text it's given and always returns the canned rows/columns it was
// constructed with. It exists so tests can exercise code that queries
// *sql.DB (i.e. Stream.incSnapshotConn) without a real Postgres connection.
type fakeQueryDriver struct {
	columns []string
	rows    [][]driver.Value
	queries *int
}

func (d *fakeQueryDriver) Open(string) (driver.Conn, error) {
	return &fakeQueryConn{driver: d}, nil
}

type fakeQueryConn struct{ driver *fakeQueryDriver }

func (c *fakeQueryConn) Prepare(string) (driver.Stmt, error) {
	return &fakeQueryStmt{conn: c}, nil
}
func (*fakeQueryConn) Close() error              { return nil }
func (*fakeQueryConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not implemented") }

type fakeQueryStmt struct{ conn *fakeQueryConn }

func (*fakeQueryStmt) Close() error  { return nil }
func (*fakeQueryStmt) NumInput() int { return -1 }
func (*fakeQueryStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *fakeQueryStmt) Query([]driver.Value) (driver.Rows, error) {
	if s.conn.driver.queries != nil {
		*s.conn.driver.queries++
	}
	return &fakeQueryRows{columns: s.conn.driver.columns, rows: s.conn.driver.rows}, nil
}

type fakeQueryRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

func (r *fakeQueryRows) Columns() []string { return r.columns }
func (*fakeQueryRows) Close() error        { return nil }
func (r *fakeQueryRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

var fakeQueryDriverSeq atomic.Int64

// newFakeQueryDB registers a fresh fakeQueryDriver under a unique name (since
// sql.Register panics on reuse) and opens a *sql.DB backed by it. If queries
// is non-nil it's incremented once per Query call, letting tests assert on
// query counts (e.g. to prove caching avoids a repeat round trip).
func newFakeQueryDB(t *testing.T, columns []string, rows [][]driver.Value, queries *int) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("fake_pglog_test_%d", fakeQueryDriverSeq.Add(1))
	sql.Register(name, &fakeQueryDriver{columns: columns, rows: rows, queries: queries})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestResolveIncrementalPKColumnsUsesSnapshotConn(t *testing.T) {
	// pgConn is deliberately left nil: if resolveIncrementalPKColumns (or
	// anything it calls) touched s.pgConn instead of s.incSnapshotConn, this
	// would panic with a nil pointer dereference rather than returning a
	// result -- this is precisely the deadlock/crash bug being guarded
	// against, since s.pgConn is occupied by the replication protocol once
	// streaming has started.
	db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"tenant_id"}, {"id"}}, nil)
	s := &Stream{incSnapshotConn: db}

	cols, err := s.resolveIncrementalPKColumns(t.Context(), TableFQN{Schema: `"public"`, Table: `"orders"`})
	require.NoError(t, err)
	assert.Equal(t, []string{`"tenant_id"`, `"id"`}, cols)
}

func TestResolveIncrementalPKColumnsNoPrimaryKey(t *testing.T) {
	db := newFakeQueryDB(t, []string{"attname"}, nil, nil)
	s := &Stream{incSnapshotConn: db}

	_, err := s.resolveIncrementalPKColumns(t.Context(), TableFQN{Schema: `"public"`, Table: `"orders"`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no primary key found")
}

func TestIncrementalPKColumnsCachesAndUnquotes(t *testing.T) {
	queries := 0
	// pgConn is left nil for the same reason as above: incrementalPKColumns
	// backs both the coordinator's PK resolution and live DML dedup lookups,
	// either of which may run concurrently with replication streaming.
	db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, &queries)
	s := &Stream{
		incSnapshotConn:    db,
		incSnapshotPKCache: make(map[string][]string),
	}

	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	cols, err := s.incrementalPKColumns(t.Context(), table)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, cols, "cached columns must be unquoted")
	assert.Equal(t, 1, queries)

	// Second call for the same table must be served from the cache, not
	// issue a second query.
	cols, err = s.incrementalPKColumns(t.Context(), table)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, cols)
	assert.Equal(t, 1, queries, "second lookup for the same table must be cached")
}

func TestResolveIncrementalMaxKeyEmptyTableIsNotAnError(t *testing.T) {
	// Zero rows means the table currently has nothing to backfill; this must
	// be reported as (nil, nil), not an error that aborts the whole stream.
	db := newFakeQueryDB(t, []string{"id"}, nil, nil)
	s := &Stream{incSnapshotConn: db}

	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}
	pk, err := s.resolveIncrementalMaxKey(t.Context(), table, []string{"id"}, "SELECT id FROM orders")
	require.NoError(t, err)
	assert.Nil(t, pk)
}

func TestCanonicalizePKValue(t *testing.T) {
	id := uuid.New()

	t.Run("binary uuid normalizes to canonical string", func(t *testing.T) {
		assert.Equal(t, id.String(), canonicalizePKValue([16]byte(id)))
	})

	t.Run("byte slice normalizes to string", func(t *testing.T) {
		assert.Equal(t, "hello", canonicalizePKValue([]byte("hello")))
	})

	t.Run("other types pass through unchanged", func(t *testing.T) {
		assert.Equal(t, int32(5), canonicalizePKValue(int32(5)))
		assert.Nil(t, canonicalizePKValue(nil))
	})
}

// TestCanonicalizePKValueDedupsAcrossDecodePaths proves the fix end-to-end
// against the dedup window: without canonicalizing PK values before they
// reach incrementalsnapshot.PrimaryKey, a UUID primary key decoded as a raw
// [16]byte on one path and a canonical string on the other would never
// dedup, since the window's key is built by directly formatting each
// PrimaryKey element.
func TestCanonicalizePKValueDedupsAcrossDecodePaths(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "widgets"}
	id := uuid.New()

	window := incrementalsnapshot.NewWindowBuffer()

	// Simulates a row buffered by the snapshot backfill path.
	backfillPK := incrementalsnapshot.PrimaryKey{canonicalizePKValue([16]byte(id))}
	window.Add(incrementalsnapshot.Row{Table: table, PK: backfillPK, Data: map[string]any{"id": id.String()}})
	require.Equal(t, 1, window.Len())

	// Simulates the same row arriving via the live streaming decode path.
	streamedPK := incrementalsnapshot.PrimaryKey{canonicalizePKValue(id.String())}
	removed := window.Remove(table, streamedPK)
	assert.True(t, removed, "the same uuid decoded via either path must dedup to the same window key")
	assert.Equal(t, 0, window.Len())
}

func TestIntegrationSnapshotStreamPKParity(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, replURL := createDockerInstance(t)
	defer cleanup()

	// createDockerInstance hands back a replication DSN; plain queries need
	// one without it. Use the driver the snapshot connection uses.
	queryDSN := strings.ReplaceAll(replURL, " replication=database", "")
	pcfg, err := pgxpool.ParseConfig(queryDSN)
	require.NoError(t, err)
	db := stdlib.OpenDB(*pcfg.ConnConfig)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())

	cases := []struct {
		name    string
		colType string
		value   string
	}{
		{"int4", "integer", "42"},
		{"int8", "bigint", "9007199254740993"},
		{"int2", "smallint", "32767"},
		{"text", "text", "'abc'"},
		{"varchar", "varchar(10)", "'xy'"},
		// char(n) is blank-padded by its output function.
		{"bpchar", "char(5)", "'ab'"},
		{"uuid", "uuid", "'0b7f2e1e-4a5b-4c3d-8e9f-0a1b2c3d4e5f'::uuid"},
		// numeric reaches the snapshot decoder's default branch as raw text
		// while the stream canonicalises it, so it is the type most likely
		// to drift apart.
		{"numeric", "numeric", "1.50"},
		{"numeric_scaled", "numeric(10,2)", "1.5"},
		{"numeric_nan", "numeric", "'NaN'::numeric"},
		// bytea arrives as []byte from the stream and as a string from the
		// snapshot, which is what canonicalizePKValue reconciles.
		{"bytea", "bytea", "'\\x0102ff'::bytea"},
		{"timestamptz", "timestamptz", "'2026-01-02 03:04:05.678+00'::timestamptz"},
		{"timestamp", "timestamp", "'2026-01-02 03:04:05.678'::timestamp"},
		{"date", "date", "'2026-01-02'::date"},
		{"float8", "double precision", "0.1"},
		{"bool", "boolean", "true"},
		{"inet", "inet", "'192.168.0.1/24'::inet"},
	}
	// Excluded: numeric with a negative scale, e.g. numeric(5,-2). The
	// streaming decoder rejects it outright because
	// pgNumericModFromAtttypmod misreads the scale. That is a pre-existing
	// fault in replication_message_decoders.go, unrelated to key parity.

	for _, tc := range cases {
		_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id %s PRIMARY KEY)", tc.name, tc.colType))
		require.NoError(t, err, "creating table for %s", tc.name)
	}

	_, err = db.Exec("CREATE PUBLICATION parity_pub FOR ALL TABLES")
	require.NoError(t, err)
	_, err = db.Exec("SELECT pg_create_logical_replication_slot('parity_slot', 'pgoutput')")
	require.NoError(t, err)

	for _, tc := range cases {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (%s)", tc.name, tc.value))
		require.NoError(t, err, "inserting into %s", tc.name)
	}

	streamed := streamedPKValues(t, db)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			streamVal, decoded := streamed[tc.name]
			require.True(t, decoded, "no streamed insert decoded for %s", tc.name)

			rows, err := db.Query(fmt.Sprintf("SELECT id FROM %s", tc.name))
			require.NoError(t, err)
			defer rows.Close()
			columnTypes, err := rows.ColumnTypes()
			require.NoError(t, err)
			scanArgs, getters := prepareScannersAndGetters(columnTypes)
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(scanArgs...))
			snapshotVal, err := getters[0](scanArgs[0])
			require.NoError(t, err)
			require.NoError(t, rows.Err())

			snapshotKey := fmt.Sprintf("%v", canonicalizePKValue(snapshotVal))
			streamKey := fmt.Sprintf("%v", canonicalizePKValue(streamVal))
			assert.Equal(t, snapshotKey, streamKey,
				"snapshot and stream must reduce a %s key to the same window key", tc.colType)
		})
	}
}

// streamedPKValues drains the pgoutput slot and returns, per table, the
// decoded primary key of its insert -- the value OnStreamedRow would key on.
func streamedPKValues(t *testing.T, db *sql.DB) map[string]any {
	t.Helper()

	rows, err := db.Query(`SELECT data FROM pg_logical_slot_get_binary_changes(
		'parity_slot', NULL, NULL, 'proto_version', '1', 'publication_names', 'parity_pub')`)
	require.NoError(t, err)
	defer rows.Close()

	typeMap := pgtype.NewMap()
	relations := map[uint32]*RelationMessage{}
	out := map[string]any{}

	for rows.Next() {
		var data []byte
		require.NoError(t, rows.Scan(&data))

		msg, err := Parse(data)
		require.NoError(t, err)

		switch m := msg.(type) {
		case *RelationMessage:
			relations[m.RelationID] = m
		case *InsertMessage:
			rel, found := relations[m.RelationID]
			require.True(t, found, "insert for unknown relation %d", m.RelationID)

			for i, col := range rel.Columns {
				// Flag 1 marks the column as part of the key.
				if col.Flags != 1 {
					continue
				}
				val, err := decodeTextColumnData(typeMap, m.Tuple.Columns[i].Data, col.DataType, col.TypeModifier)
				require.NoError(t, err, "decoding streamed key for %s", rel.RelationName)
				out[rel.RelationName] = val
			}
		}
	}
	require.NoError(t, rows.Err())
	return out
}
