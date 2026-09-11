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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestIncrementalSnapshotKeyResolution(t *testing.T) {
	orders := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}
	ordersFQN := TableFQN{Schema: `"public"`, Table: `"orders"`}

	t.Run("key columns come back quoted, in key order", func(t *testing.T) {
		db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"tenant_id"}, {"id"}}, nil)
		s := &Stream{incSnapshotConn: db}

		cols, err := s.resolveIncrementalPKColumns(t.Context(), ordersFQN)
		require.NoError(t, err)
		assert.Equal(t, []string{`"tenant_id"`, `"id"`}, cols)
	})

	t.Run("no primary key is unusable, not a failure", func(t *testing.T) {
		db := newFakeQueryDB(t, []string{"attname"}, nil, nil)
		s := &Stream{incSnapshotConn: db}

		_, err := s.resolveIncrementalPKColumns(t.Context(), ordersFQN)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no primary key found")
		// The coordinator drops rather than fails on this, so it must be
		// distinguishable from a transient error.
		assert.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable)
	})

	t.Run("columns are unquoted and cached", func(t *testing.T) {
		queries := 0
		db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, &queries)
		s := &Stream{
			incSnapshotConn:    db,
			incSnapshotPKCache: make(map[string][]string),
		}

		cols, err := s.incrementalPKColumns(t.Context(), orders)
		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, cols, "cached columns must be unquoted")
		// Two: the key columns, then their types.
		assert.Equal(t, 2, queries)

		// The dedup path resolves these per streamed row, so a repeat
		// lookup must not cost a round trip.
		cols, err = s.incrementalPKColumns(t.Context(), orders)
		require.NoError(t, err)
		assert.Equal(t, []string{"id"}, cols)
		assert.Equal(t, 2, queries, "second lookup for the same table must be cached")
	})

	t.Run("an empty table has no max key and no error", func(t *testing.T) {
		// Zero rows means nothing to backfill, which the coordinator treats
		// as an exhausted table rather than a failure.
		db := newFakeQueryDB(t, []string{"id"}, nil, nil)
		s := &Stream{incSnapshotConn: db}

		pk, err := s.resolveIncrementalMaxKey(t.Context(), orders, []string{"id"}, "SELECT id FROM orders")
		require.NoError(t, err)
		assert.Nil(t, pk)
	})
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
		{"timestamptz", "timestamptz", "'2026-01-02 03:04:05.678+00'::timestamptz"},
		{"timestamp", "timestamp", "'2026-01-02 03:04:05.678'::timestamp"},
		{"date", "date", "'2026-01-02'::date"},
		{"float8", "double precision", "0.1"},
		{"bool", "boolean", "true"},
		{"inet", "inet", "'192.168.0.1/24'::inet"},
	}
	// Excluded: bytea. The two decode paths do reduce to the same window
	// key, but the key is also bound as the next chunk's bound and written
	// to the checkpoint, and raw bytes survive neither. It is rejected as a
	// key type instead -- refer to
	// TestIntegrationIncrementalSnapshotRejectsUnbindableKey.
	//
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

// TestSnapshotSignalRejectsUnreplicatedTable: a signal may only name a table
// the publication carries. Backfilling an unreplicated one has no live
// changes to deduplicate against, so a write during the backfill would be
// lost behind the stale snapshot row.
func TestSnapshotSignalRejectsUnreplicatedTable(t *testing.T) {
	signalRow := func(tables string) *StreamMessage {
		return &StreamMessage{
			Operation: InsertOpType,
			Schema:    "public",
			Table:     "rpcn_signal",
			Data: map[string]any{
				"type": "snapshot",
				"data": fmt.Sprintf(`{"tables": [%s]}`, tables),
			},
		}
	}

	newStream := func(replicated map[incrementalsnapshot.TableID]struct{}) *Stream {
		signalTable := incrementalsnapshot.TableID{Schema: "public", Table: "rpcn_signal"}
		return &Stream{
			// Any non-nil coordinator: the check runs before it is used.
			incSnapshotCoordinator: &incsnapshot.Coordinator{},
			signalTable:            &signalTable,
			snapshotSchema:         "public",
			incSnapshotReplicated:  replicated,
			// The accept path resolves the key columns too.
			incSnapshotConn:    newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, nil),
			incSnapshotPKCache: map[string][]string{},
		}
	}

	replicated := map[incrementalsnapshot.TableID]struct{}{
		{Schema: "public", Table: "flights"}: {},
	}

	t.Run("replicated table is accepted", func(t *testing.T) {
		got, err := newStream(replicated).snapshotSignalTables(t.Context(), signalRow(`"flights"`))
		require.NoError(t, err)
		assert.Equal(t, []incrementalsnapshot.TableID{{Schema: "public", Table: "flights"}}, got)
	})

	t.Run("unreplicated table is rejected", func(t *testing.T) {
		_, err := newStream(replicated).snapshotSignalTables(t.Context(), signalRow(`"users"`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "public.users is not replicated")
	})

	t.Run("one unreplicated table rejects the whole request", func(t *testing.T) {
		// Queueing only the valid half would leave the caller with no way to
		// tell which tables were accepted.
		_, err := newStream(replicated).snapshotSignalTables(t.Context(), signalRow(`"flights", "users"`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "public.users is not replicated")
	})

	t.Run("a nil set means FOR ALL TABLES", func(t *testing.T) {
		got, err := newStream(nil).snapshotSignalTables(t.Context(), signalRow(`"anything"`))
		require.NoError(t, err)
		assert.Equal(t, []incrementalsnapshot.TableID{{Schema: "public", Table: "anything"}}, got)
	})
}

// TestSnapshotSignalRejectsTableWithoutPrimaryKey: the backfill pages by
// key, so a table without one can never be read, and accepting the signal
// would queue it into the checkpoint. REPLICA IDENTITY FULL replicates a
// table without a key, so this is reachable rather than broken.
func TestSnapshotSignalRejectsTableWithoutPrimaryKey(t *testing.T) {
	signalTable := incrementalsnapshot.TableID{Schema: "public", Table: "rpcn_signal"}
	s := &Stream{
		incSnapshotCoordinator: &incsnapshot.Coordinator{},
		signalTable:            &signalTable,
		snapshotSchema:         "public",
		incSnapshotReplicated: map[incrementalsnapshot.TableID]struct{}{
			{Schema: "public", Table: "nopk"}: {},
		},
		// No rows: the table has no primary key.
		incSnapshotConn:    newFakeQueryDB(t, []string{"attname"}, nil, nil),
		incSnapshotPKCache: map[string][]string{},
	}

	_, err := s.snapshotSignalTables(t.Context(), &StreamMessage{
		Operation: InsertOpType,
		Schema:    "public",
		Table:     "rpcn_signal",
		Data: map[string]any{
			"type": "snapshot",
			"data": `{"tables": ["nopk"]}`,
		},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable)
	assert.Contains(t, err.Error(), "no primary key found")
}

// TestSnapshotSignalRejectionVsFailure: only a signal the connector will
// never honour may be logged and skipped.
//
// The row is forwarded and its position acknowledged either way, so a
// request dropped because a check could not run is dropped for good. A
// failure must instead reach the caller, which restarts the stream and
// redelivers the still-unacknowledged row.
func TestSnapshotSignalRejectionVsFailure(t *testing.T) {
	signalTable := incrementalsnapshot.TableID{Schema: "public", Table: "rpcn_signal"}
	replicated := map[incrementalsnapshot.TableID]struct{}{
		{Schema: "public", Table: "flights"}: {},
		{Schema: "public", Table: "nopk"}:    {},
	}

	withKey := func(t *testing.T) *sql.DB {
		return newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, nil)
	}
	withoutKey := func(t *testing.T) *sql.DB {
		return newFakeQueryDB(t, []string{"attname"}, nil, nil)
	}
	unreachable := func(t *testing.T) *sql.DB {
		// Closed, so the validation query fails the way a reset connection
		// or a pool timeout would.
		db := withKey(t)
		require.NoError(t, db.Close())
		return db
	}
	// The query resolves the table with ::regclass, so a name that does not
	// resolve fails at execution rather than returning no rows.
	pgFailure := func(code string) func(*testing.T) *sql.DB {
		return func(t *testing.T) *sql.DB {
			return newFailingQueryDB(t, &pgconn.PgError{Code: code, Message: "from the server"})
		}
	}

	for _, test := range []struct {
		name     string
		conn     func(*testing.T) *sql.DB
		payload  string
		rejected bool
	}{
		{name: "unreplicated table", conn: withKey, payload: `{"tables": ["users"]}`, rejected: true},
		{name: "no primary key", conn: withoutKey, payload: `{"tables": ["nopk"]}`, rejected: true},
		{name: "malformed payload", conn: withKey, payload: `not json`, rejected: true},
		{name: "empty table list", conn: withKey, payload: `{"tables": []}`, rejected: true},
		{name: "invalid table name", conn: withKey, payload: `{"tables": ["*"]}`, rejected: true},
		{name: "validation query fails", conn: unreachable, payload: `{"tables": ["flights"]}`, rejected: false},
		// A typo, or a schema-qualified name in one string, resolves to no
		// relation. Propagating that would wedge replication for every
		// table until someone deleted the signal row.
		{name: "table does not exist", conn: pgFailure(pgErrUndefinedTable), payload: `{"tables": ["flights"]}`, rejected: true},
		{name: "schema does not exist", conn: pgFailure(pgErrInvalidSchemaName), payload: `{"tables": ["flights"]}`, rejected: true},
		{name: "no privilege on the table", conn: pgFailure(pgErrInsufficientPrivilege), payload: `{"tables": ["flights"]}`, rejected: true},
		// A server-side error that says nothing about the table stays
		// retryable.
		{name: "server error is retryable", conn: pgFailure("40001"), payload: `{"tables": ["flights"]}`, rejected: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &Stream{
				incSnapshotCoordinator: &incsnapshot.Coordinator{},
				signalTable:            &signalTable,
				snapshotSchema:         "public",
				incSnapshotReplicated:  replicated,
				incSnapshotConn:        test.conn(t),
				incSnapshotPKCache:     map[string][]string{},
			}

			msg := &StreamMessage{
				Operation: InsertOpType,
				Schema:    "public",
				Table:     "rpcn_signal",
				Data:      map[string]any{"type": "snapshot", "data": test.payload},
			}

			_, err := s.snapshotSignalTables(t.Context(), msg)
			require.Error(t, err)
			assert.Equal(t, test.rejected, errors.Is(err, errSignalRejected))

			// What the replication loop acts on: a rejection is swallowed
			// after logging, anything else propagates and restarts the
			// stream so the row is redelivered.
			s.logger = service.MockResources().Logger()
			err = s.dispatchSnapshotSignal(t.Context(), msg)
			if test.rejected {
				assert.NoError(t, err, "a rejection must not stop replication")
			} else {
				assert.Error(t, err, "a failed check must reach the caller")
			}
		})
	}
}

// TestDeduplicateStreamedRowOnlyTouchesTheCurrentTable: a table a previous
// run finished is still in the checkpoint, so a gate on that would resolve
// keys for it on every restart. The lookup is a live query, and its failure
// used to stop replication for every table -- for work OnStreamedRow
// discards, since it only matters for the table being read.
func TestDeduplicateStreamedRowOnlyTouchesTheCurrentTable(t *testing.T) {
	// Closed, so any key lookup fails the way a reset connection would.
	broken := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, nil)
	require.NoError(t, broken.Close())

	s := &Stream{
		// Snapshotting nothing: a zero coordinator reads no table.
		incSnapshotCoordinator: &incsnapshot.Coordinator{},
		incSnapshotConn:        broken,
		incSnapshotPKCache:     map[string][]string{},
	}

	for _, op := range []OpType{InsertOpType, UpdateOpType, DeleteOpType} {
		require.NoError(t, s.deduplicateStreamedRow(t.Context(), &StreamMessage{
			Operation: op,
			Schema:    "public",
			Table:     "finished",
			Data:      map[string]any{"id": 1},
		}), "a row on a table the snapshot is not reading must not touch the database")
	}
}

var fakeQueryDriverSeq atomic.Int64

func newFailingQueryDB(t *testing.T, err error) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("fake_pglog_test_%d", fakeQueryDriverSeq.Add(1))
	sql.Register(name, &fakeQueryDriver{queryErr: err})
	db, openErr := sql.Open(name, "")
	require.NoError(t, openErr)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newFakeQueryDBCapturing also collects the SQL text of every Prepare into
// prepared.
func newFakeQueryDBCapturing(t *testing.T, columns []string, rows [][]driver.Value, queries *int, prepared *[]string) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("fake_pglog_test_%d", fakeQueryDriverSeq.Add(1))
	sql.Register(name, &fakeQueryDriver{columns: columns, rows: rows, queries: queries, prepared: prepared})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newFakeQueryDB(t *testing.T, columns []string, rows [][]driver.Value, queries *int) *sql.DB {
	t.Helper()
	return newFakeQueryDBCapturing(t, columns, rows, queries, nil)
}

type fakeQueryDriver struct {
	columns  []string
	rows     [][]driver.Value
	queries  *int
	prepared *[]string
	queryErr error
	// keyTypes answers the primary key type query as {column, type name}
	// rows. Left nil, each configured key column is reported as int8, which
	// the chunk query can bind -- so a test that only cares about the key
	// columns needs to say nothing about their types.
	keyTypes [][]driver.Value
}

func (d *fakeQueryDriver) Open(string) (driver.Conn, error) {
	return &fakeQueryConn{driver: d}, nil
}

type fakeQueryConn struct{ driver *fakeQueryDriver }

func (c *fakeQueryConn) Prepare(query string) (driver.Stmt, error) {
	if c.driver.prepared != nil {
		*c.driver.prepared = append(*c.driver.prepared, query)
	}
	return &fakeQueryStmt{conn: c, query: query}, nil
}
func (*fakeQueryConn) Close() error              { return nil }
func (*fakeQueryConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not implemented") }

type fakeQueryStmt struct {
	conn  *fakeQueryConn
	query string
}

func (*fakeQueryStmt) Close() error  { return nil }
func (*fakeQueryStmt) NumInput() int { return -1 }
func (*fakeQueryStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *fakeQueryStmt) Query([]driver.Value) (driver.Rows, error) {
	if s.conn.driver.queries != nil {
		*s.conn.driver.queries++
	}
	if err := s.conn.driver.queryErr; err != nil {
		return nil, err
	}
	// The key type query has its own shape, so it needs its own result.
	if strings.Contains(s.query, "pg_type") {
		return &fakeQueryRows{
			columns: []string{"attname", "typname"},
			rows:    s.conn.driver.keyTypeRows(),
		}, nil
	}
	return &fakeQueryRows{columns: s.conn.driver.columns, rows: s.conn.driver.rows}, nil
}

func (d *fakeQueryDriver) keyTypeRows() [][]driver.Value {
	if d.keyTypes != nil {
		return d.keyTypes
	}
	rows := make([][]driver.Value, 0, len(d.rows))
	for _, row := range d.rows {
		if len(row) > 0 {
			rows = append(rows, []driver.Value{row[0], "int8"})
		}
	}
	return rows
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

// TestIntegrationIncrementalSnapshotRejectsUnbindableKey covers a key type the
// chunk query cannot bind. Before the check, such a table halted replication
// for every table: FetchChunk failed, the error reached processChange, the
// stream restarted, reloaded the same checkpoint and failed identically.
func TestIntegrationIncrementalSnapshotRejectsUnbindableKey(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, replURL := createDockerInstance(t)
	defer cleanup()

	queryDSN := strings.ReplaceAll(replURL, " replication=database", "")
	pcfg, err := pgxpool.ParseConfig(queryDSN)
	require.NoError(t, err)
	db := stdlib.OpenDB(*pcfg.ConnConfig)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())

	_, err = db.Exec(`CREATE DOMAIN blob_key AS bytea`)
	require.NoError(t, err)

	for _, ddl := range []string{
		`CREATE TABLE bytea_key (id bytea PRIMARY KEY, payload text)`,
		`CREATE TABLE composite_bytea_key (tenant bigint, id bytea, payload text, PRIMARY KEY (tenant, id))`,
		`CREATE TABLE domain_bytea_key (id blob_key PRIMARY KEY, payload text)`,
		`CREATE TABLE bigint_key (id bigint PRIMARY KEY, payload text)`,
		`CREATE TABLE text_key (id text PRIMARY KEY, payload text)`,
	} {
		_, err := db.Exec(ddl)
		require.NoError(t, err, ddl)
	}

	stream := &Stream{
		incSnapshotConn:    db,
		incSnapshotPKCache: map[string][]string{},
	}
	tableID := func(name string) incrementalsnapshot.TableID {
		return incrementalsnapshot.TableID{Schema: "public", Table: name}
	}

	for _, name := range []string{"bytea_key", "composite_bytea_key", "domain_bytea_key"} {
		t.Run("rejects "+name, func(t *testing.T) {
			_, err := stream.incrementalPKColumns(t.Context(), tableID(name))
			require.Error(t, err)
			assert.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable,
				"must be unusable, so a signal is rejected and a resumed queue entry is dropped rather than looping")

			// And the signal path turns that into a rejection, which is
			// logged and skipped rather than restarting the stream.
			err = stream.checkBackfillable(t.Context(), tableID(name))
			assert.ErrorIs(t, err, errSignalRejected)
		})
	}

	for _, name := range []string{"bigint_key", "text_key"} {
		t.Run("accepts "+name, func(t *testing.T) {
			cols, err := stream.incrementalPKColumns(t.Context(), tableID(name))
			require.NoError(t, err)
			assert.Equal(t, []string{"id"}, cols)
		})
	}

	t.Run("an accepted key pages across several chunks", func(t *testing.T) {
		// The property the rejected types break: the key of the last row of
		// one chunk, put through the checkpoint's JSON encoding, binds as the
		// next chunk's lower bound and reads the rows that follow it.
		for i := 1; i <= 7; i++ {
			_, err := db.Exec(`INSERT INTO text_key VALUES ($1, $2)`, fmt.Sprintf("k%02d", i), "v")
			require.NoError(t, err)
		}

		table := tableID("text_key")
		maxQ, err := incsnapshot.BuildMaxKeyQuery(table, []string{"id"})
		require.NoError(t, err)
		maxPK, err := stream.resolveIncrementalMaxKey(t.Context(), table, []string{"id"}, maxQ)
		require.NoError(t, err)

		var seen []string
		var lower incrementalsnapshot.PrimaryKey
		for chunk := 0; chunk < 10; chunk++ {
			q, args, err := incsnapshot.BuildChunkQuery(table, []string{"id"}, lower, maxPK, 3)
			require.NoError(t, err)
			rows, err := stream.fetchIncrementalChunk(t.Context(), table, []string{"id"}, q, args)
			require.NoError(t, err, "chunk %d must bind the previous chunk's key", chunk)
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				seen = append(seen, fmt.Sprintf("%v", row.PK[0]))
			}
			// Through the checkpoint, as a resume would.
			encoded, err := json.Marshal(rows[len(rows)-1].PK)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(encoded, &lower))
		}

		assert.Equal(t, []string{"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, seen,
			"every row exactly once, so each bound round-tripped")
	})
}
