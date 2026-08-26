// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	_ "github.com/lib/pq" // registers "postgres" driver for sql.Open in tests

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestLSNSuite(t *testing.T) {
	suite.Run(t, new(lsnSuite))
}

type lsnSuite struct {
	suite.Suite
}

func (s *lsnSuite) R() *require.Assertions {
	return s.Require()
}

func (s *lsnSuite) Equal(e, a any, args ...any) {
	s.R().Equal(e, a, args...)
}

func (s *lsnSuite) NoError(err error) {
	s.R().NoError(err)
}

func (s *lsnSuite) TestScannerInterface() {
	var lsn LSN
	lsnText := "00000016/B374D848"
	lsnUint64 := uint64(97500059720)
	var err error

	err = lsn.Scan(lsnText)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan([]byte(lsnText))
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	lsn = 0
	err = lsn.Scan(lsnUint64)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan(int64(lsnUint64))
	s.Error(err)
	s.T().Log(err)
}

func (s *lsnSuite) TestScanToNil() {
	var lsnPtr *LSN
	err := lsnPtr.Scan("16/B374D848")
	s.NoError(err)
}

func (s *lsnSuite) TestValueInterface() {
	lsn := LSN(97500059720)
	driverValue, err := lsn.Value()
	s.NoError(err)
	lsnStr, ok := driverValue.(string)
	s.R().True(ok)
	s.Equal("00000016/B374D848", lsnStr)
}

const (
	slotName     = "pglogrepl_test"
	outputPlugin = "pgoutput"
)

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func createDockerInstance(t *testing.T) (cleanup func(), dbURL string) {
	ctr, err := testcontainers.Run(t.Context(), "postgres:16",
		testcontainers.WithExposedPorts("5432/tcp"),
		testcontainers.WithEnv(map[string]string{
			"POSTGRES_PASSWORD": "secret",
			"POSTGRES_USER":     "user_name",
			"POSTGRES_DB":       "dbname",
		}),
		testcontainers.WithCmd("postgres", "-c", "wal_level=logical"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	databaseURL := fmt.Sprintf("user=user_name password=secret dbname=dbname sslmode=disable host=%s port=%s replication=database", host, mp.Port())

	var db *sql.DB
	require.Eventually(t, func() bool {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return false
		}
		return db.Ping() == nil
	}, 2*time.Minute, time.Second)

	cleanup = func() {
		// Container cleanup is handled by testcontainers.CleanupContainer
	}

	return cleanup, databaseURL
}

func TestIntegrationIdentifySystem(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*100)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	assert.NotEmpty(t, sysident.SystemID, 0)
	assert.Greater(t, sysident.Timeline, int32(0))

	xlogPositionIsPositive := sysident.XLogPos > 0
	assert.True(t, xlogPositionIsPositive)
	assert.NotEmpty(t, sysident.DBName, 0)
}

func TestIntegrationCreateReplicationSlot(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)
	_, _, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false})
	require.NoError(t, err)
}

func TestIntegrationDropReplicationSlot(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, _, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false})
	require.NoError(t, err)

	err = DropReplicationSlot(ctx, conn, slotName, DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, _, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false})
	require.NoError(t, err)
}

func TestIntegrationCopyReplicationSlot(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	lsn, _, err := CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)
	t.Log("initial lsn", lsn)

	lsn, err = CopyReplicationSlot(ctx, conn, slotName, "foo", false)
	require.NoError(t, err)
	t.Log("copied lsn", lsn)

	err = DropReplicationSlot(ctx, conn, slotName, DropReplicationSlotOptions{})
	require.NoError(t, err)
}

func TestIntegrationCreatePublication(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	createSchema := func(t *testing.T, name string) {
		t.Helper()
		_, err := conn.Exec(t.Context(), fmt.Sprintf("CREATE SCHEMA %s;", name)).ReadAll()
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.Exec(cleanupCtx, fmt.Sprintf("DROP SCHEMA %s CASCADE;", name)).ReadAll()
		})
	}

	t.Run("creates a FOR ALL TABLES publication when tables is empty", func(t *testing.T) {
		// FOR ALL TABLES publications are database-scoped, not
		// schema-scoped, so assert.Empty below only holds while no other
		// user table exists anywhere in the database. That's true here
		// only because this subtest runs first, subtests run sequentially
		// (none call t.Parallel()), and every later subtest drops its own
		// schema on cleanup before returning.
		const publicationName = "pub_all_tables_empty"

		err := CreatePublication(t.Context(), conn, publicationName, []TableFQN{})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.Empty(t, tables)
		assert.True(t, forAllTables)
	})

	t.Run("narrowing an existing FOR ALL TABLES publication to an explicit table list narrows it", func(t *testing.T) {
		const (
			publicationName = "pub_narrow"
			schema          = `"sch_narrow"`
		)
		createSchema(t, schema)

		multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.orders (id serial PRIMARY KEY, name text);", schema))
		_, err := multiReader.ReadAll()
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{})
		require.NoError(t, err)

		// Checked directly against pg_publication rather than via
		// GetPublicationTables as it's a better source of truth.
		isForAllTables := func() bool {
			rows, err := conn.Exec(t.Context(), fmt.Sprintf(
				"SELECT puballtables FROM pg_publication WHERE pubname = '%s';", publicationName,
			)).ReadAll()
			require.NoError(t, err)
			require.Len(t, rows[0].Rows, 1)
			return string(rows[0].Rows[0][0]) == "t"
		}
		require.True(t, isForAllTables())

		// user updates tables in config and restarts connector - since
		// Postgres can't ALTER a FOR ALL TABLES publication down to a
		// named table list, this drops and recreates the publication as a
		// named-table one containing just "orders".
		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{{schema, `"orders"`}})
		require.NoError(t, err)
		assert.False(t, isForAllTables(), "narrowing an existing FOR ALL TABLES publication to an explicit table list should actually take effect, not silently leave it as FOR ALL TABLES")
	})

	t.Run("moving from a named-table publication to FOR ALL TABLES empties the publication instead (bug)", func(t *testing.T) {
		const (
			publicationName = "pub_named_to_all_bug"
			schema          = `"sch_named_to_all_bug"`
		)
		createSchema(t, schema)

		for _, name := range []string{"widgets", "gadgets"} {
			multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", schema, name))
			_, err := multiReader.ReadAll()
			require.NoError(t, err)
		}

		err := CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"widgets"`},
			{schema, `"gadgets"`},
		})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.Len(t, tables, 2)
		assert.False(t, forAllTables)

		// Checked directly against pg_publication rather than via
		// GetPublicationTables: once the publication is emptied out below,
		// pg_publication_tables for it returns zero rows regardless of
		// whether the publication is genuinely FOR ALL TABLES or is just a
		// named-table publication left with no members, so
		// GetPublicationTables can't distinguish the two outcomes here.
		isForAllTables := func() bool {
			rows, err := conn.Exec(t.Context(), fmt.Sprintf(
				"SELECT puballtables FROM pg_publication WHERE pubname = '%s';", publicationName,
			)).ReadAll()
			require.NoError(t, err)
			require.Len(t, rows[0].Rows, 1)
			return string(rows[0].Rows[0][0]) == "t"
		}
		require.False(t, isForAllTables())

		// user changes config to publish everything (empty tables list) and
		// restarts the connector - this should convert the publication to
		// FOR ALL TABLES, but instead it silently drops every existing table
		// from it, leaving a named-table publication with zero members:
		// nothing at all gets replicated, with no error raised.
		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{})
		require.NoError(t, err)
		assert.True(t, isForAllTables(), "moving an existing named-table publication to an empty table list should make it FOR ALL TABLES, not silently empty it out")
	})

	t.Run("creates a named-table publication with one table", func(t *testing.T) {
		const (
			publicationName = "pub_single_table"
			schema          = `"sch_single_table"`
		)
		createSchema(t, schema)

		multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.single_table (id serial PRIMARY KEY, name text);", schema))
		_, err := multiReader.ReadAll()
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{{schema, `"single_table"`}})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.NotEmpty(t, tables)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, TableFQN{schema, `"single_table"`})
		assert.False(t, forAllTables)
	})

	t.Run("adds a table to an existing named-table publication", func(t *testing.T) {
		const (
			publicationName = "pub_add_table"
			schema          = `"sch_add_table"`
		)
		createSchema(t, schema)

		for _, name := range []string{"add_table1", "add_table2"} {
			multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", schema, name))
			_, err := multiReader.ReadAll()
			require.NoError(t, err)
		}

		err := CreatePublication(t.Context(), conn, publicationName, []TableFQN{{schema, `"add_table1"`}})
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"add_table2"`},
			{schema, `"add_table1"`},
		})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.NotEmpty(t, tables)
		assert.Len(t, tables, 2)
		assert.Contains(t, tables, TableFQN{schema, `"add_table1"`})
		assert.Contains(t, tables, TableFQN{schema, `"add_table2"`})
		assert.False(t, forAllTables)
	})

	t.Run("removes a table from an existing named-table publication", func(t *testing.T) {
		const (
			publicationName = "pub_remove_table"
			schema          = `"sch_remove_table"`
		)
		createSchema(t, schema)

		for _, name := range []string{"remove_table1", "remove_table2"} {
			multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", schema, name))
			_, err := multiReader.ReadAll()
			require.NoError(t, err)
		}

		err := CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"remove_table1"`},
			{schema, `"remove_table2"`},
		})
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"remove_table1"`},
		})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.NotEmpty(t, tables)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, TableFQN{schema, `"remove_table1"`})
		assert.False(t, forAllTables)
	})

	t.Run("adds and removes tables in the same call", func(t *testing.T) {
		const (
			publicationName = "pub_add_remove"
			schema          = `"sch_add_remove"`
		)
		createSchema(t, schema)

		for _, name := range []string{"addremove_old", "addremove_new"} {
			multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", schema, name))
			_, err := multiReader.ReadAll()
			require.NoError(t, err)
		}

		err := CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"addremove_old"`},
		})
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{
			{schema, `"addremove_new"`},
		})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
		require.NoError(t, err)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, TableFQN{schema, `"addremove_new"`})
		assert.False(t, forAllTables)
	})

	t.Run("supports quoted, case-sensitive schema and table identifiers", func(t *testing.T) {
		const (
			publicationQuotedIdentifiers = "quoted_identifiers"
			caseSensitiveSchema          = `"FooBar"`
			caseSensitiveTable           = `"Foo"`
			caseSensitiveTable2          = `"Bar"`
		)

		createSchema(t, caseSensitiveSchema)

		multiReader := conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable))
		_, err := multiReader.ReadAll()
		require.NoError(t, err)

		multiReader = conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable2))
		_, err = multiReader.ReadAll()
		require.NoError(t, err)

		err = CreatePublication(t.Context(), conn, publicationQuotedIdentifiers, []TableFQN{
			{caseSensitiveSchema, caseSensitiveTable},
			{caseSensitiveSchema, caseSensitiveTable2},
		})
		require.NoError(t, err)

		// Remove one table with a quoted identifier from the publication.
		err = CreatePublication(t.Context(), conn, publicationQuotedIdentifiers, []TableFQN{
			{caseSensitiveSchema, caseSensitiveTable},
		})
		require.NoError(t, err)

		tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationQuotedIdentifiers)
		require.NoError(t, err)
		assert.Len(t, tables, 1)
		assert.Contains(t, tables, TableFQN{`"FooBar"`, `"Foo"`})
		assert.False(t, forAllTables)
	})
}

func TestIntegrationStartReplication(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	// create publication
	publicationName := "test_publication"
	err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{})
	require.NoError(t, err)

	_, _, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false})
	require.NoError(t, err)

	err = StartReplication(ctx, conn, slotName, sysident.XLogPos, StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names 'test_publication'",
			"messages 'true'",
		},
	})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(dbURL)
		require.NoError(t, err)
		delete(config.RuntimeParams, "replication")

		conn, err := pgconn.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer closeConn(t, conn)

		_, err = conn.Exec(ctx, `
create table t(id int primary key, name text);

insert into t values (1, 'foo');
insert into t values (2, 'bar');
insert into t values (3, 'baz');

update t set name='quz' where id=3;

delete from t where id=2;

drop table t;
`).ReadAll()
		require.NoError(t, err)
	}()

	rxKeepAlive := func() PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	relations := map[uint32]*RelationMessage{}
	typeMap := pgtype.NewMap()

	rxXLogData := func() XLogData {
		var cdMsg *pgproto3.CopyData
		// Discard keepalive messages
		for {
			msg, err := conn.ReceiveMessage(ctx)
			require.NoError(t, err)
			var ok bool
			cdMsg, ok = msg.(*pgproto3.CopyData)
			require.True(t, ok)
			if cdMsg.Data[0] != PrimaryKeepaliveMessageByteID {
				break
			}
		}
		require.Equal(t, byte(XLogDataByteID), cdMsg.Data[0])
		xld, err := ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	decodeWALData := func(data []byte, relations map[uint32]*RelationMessage, typeMap *pgtype.Map, unchangedToastValue any) (*StreamMessage, error) {
		m, err := Parse(data)
		if err != nil {
			return nil, err
		}
		return toStreamMessage(m, relations, typeMap, unchangedToastValue)
	}

	rxKeepAlive()
	xld := rxXLogData()
	begin, _, err := isBeginMessage(xld.WALData)
	require.NoError(t, err)
	assert.True(t, begin)

	xld = rxXLogData()
	var streamMessage *StreamMessage
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	assert.Nil(t, streamMessage)

	xld = rxXLogData()
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	jsonData, err := json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"insert","schema":"public","table":"t","lsn":null,"data":{"id":1, "name":"foo"}}`, string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"insert","schema":"public","table":"t","lsn":null,"data":{"id":2,"name":"bar"}}`, string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"insert","schema":"public","table":"t","lsn":null,"data":{"id":3,"name":"baz"}}`, string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"update","schema":"public","table":"t","lsn":null,"data":{"id":3,"name":"quz"}}`, string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodeWALData(xld.WALData, relations, typeMap, nil)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.JSONEq(t, `{"operation":"delete","schema":"public","table":"t","lsn":null,"data":{"id":2,"name":null}}`, string(jsonData))
	xld = rxXLogData()

	commit, _, err := isCommitMessage(xld.WALData)
	require.NoError(t, err)
	assert.True(t, commit)
}

func TestIntegrationSendStandbyStatusUpdate(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	err = SendStandbyStatusUpdate(ctx, conn, StandbyStatusUpdate{WALWritePosition: sysident.XLogPos})
	require.NoError(t, err)
}

func TestLSNStringLexicographicalOrder(t *testing.T) {
	ordered := []uint64{
		0,
		1,
		42,
		math.MaxInt16 - 1,
		math.MaxInt16,
		math.MaxInt16 + 1,
		math.MaxInt32 - 1,
		math.MaxInt32,
		math.MaxInt32 + 1,
		math.MaxInt64 - 1,
		math.MaxInt64,
		math.MaxInt64 + 1,
		math.MaxUint64 - 1,
		math.MaxUint64,
	}
	slices.SortFunc(ordered, func(a, b uint64) int {
		aStr := LSN(a).String()
		bStr := LSN(b).String()
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		} else {
			return 0
		}
	})
	require.IsIncreasing(t, ordered)
}
