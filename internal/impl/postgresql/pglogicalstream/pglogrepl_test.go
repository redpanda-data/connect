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
	"strings"
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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
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

	publicationName := "test_publication"
	schema := `"public"`
	err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{})
	require.NoError(t, err)

	tables, forAllTables, err := GetPublicationTables(t.Context(), conn, publicationName)
	require.NoError(t, err)
	assert.Empty(t, tables)
	assert.True(t, forAllTables)

	multiReader := conn.Exec(t.Context(), "CREATE TABLE test_table (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	publicationWithTables := "test_pub_with_tables"
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{{schema, `"test_table"`}})
	require.NoError(t, err)

	// A FOR ALL TABLES publication lists every table, and is still reported
	// as FOR ALL TABLES.
	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationName)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.True(t, forAllTables)

	// A FOR ALL TABLES publication cannot be narrowed to a table list, so
	// syncing it to one fails instead of streaming every table.
	err = CreatePublication(t.Context(), conn, publicationName, []TableFQN{{schema, `"test_table"`}})
	require.ErrorIs(t, err, errAllTablesPublication)
	_, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationName)
	require.NoError(t, err)
	assert.True(t, forAllTables)

	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.False(t, forAllTables)

	// Add more tables to publication
	multiReader = conn.Exec(t.Context(), "CREATE TABLE test_table2 (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	// Pass more tables to the publication
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table2"},
		{schema, "test_table"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, forAllTables)

	// Remove one table from the publication
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.False(t, forAllTables)

	// Add one table and remove one at the same time
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table2"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, forAllTables)

	// Create a schema with a quoted identifier
	caseSensitiveSchema := `"FooBar"`
	multiReader = conn.Exec(t.Context(), fmt.Sprintf("CREATE SCHEMA %s;", caseSensitiveSchema))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	caseSensitiveTable := `"Foo"`
	multiReader = conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	caseSensitiveTable2 := `"Bar"`
	multiReader = conn.Exec(t.Context(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable2))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	// Pass tables to the schema with quoted identifiers
	publicationQuotedIdentifiers := "quoted_identifiers"
	err = CreatePublication(t.Context(), conn, publicationQuotedIdentifiers, []TableFQN{
		{caseSensitiveSchema, caseSensitiveTable},
		{caseSensitiveSchema, caseSensitiveTable2},
	})
	require.NoError(t, err)

	// Remove one table with a quoted identifier from the publication
	err = CreatePublication(t.Context(), conn, publicationQuotedIdentifiers, []TableFQN{
		{caseSensitiveSchema, caseSensitiveTable},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(t.Context(), conn, publicationQuotedIdentifiers)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{`"FooBar"`, `"Foo"`})
	assert.False(t, forAllTables)

	// Without publish_via_partition_root, pg_publication_tables lists a
	// partitioned table's partitions, not the table. Syncing against that
	// view would try to drop partitions that are not members and to add the
	// table that already is, failing every reconnect.
	for _, sql := range []string{
		"CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);",
		"CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');",
		"CREATE TABLE events_2027 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');",
	} {
		_, err = conn.Exec(t.Context(), sql).ReadAll()
		require.NoError(t, err)
	}
	events := []TableFQN{{schema, `"events"`}}
	require.NoError(t, CreatePublication(t.Context(), conn, "pub_partitioned", events))
	require.NoError(t, CreatePublication(t.Context(), conn, "pub_partitioned", events), "a reconnect must not fail")
	members, err := getPublicationMembers(t.Context(), conn, "pub_partitioned")
	require.NoError(t, err)
	assert.Equal(t, events, members, "the publication still lists exactly the partitioned table")
}

func TestIntegrationValidatePublication(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*10)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	exec := func(sql string) {
		t.Helper()
		_, err := conn.Exec(ctx, sql).ReadAll()
		require.NoError(t, err)
	}
	validate := func(publication string, tables []TableFQN) error {
		t.Helper()
		filtered, err := ValidatePublication(ctx, conn, publication, tables)
		assert.Empty(t, filtered)
		return err
	}
	exec("CREATE TABLE orders (id serial PRIMARY KEY, note text);")
	exec("CREATE TABLE users (id serial PRIMARY KEY);")
	exec("CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);")
	exec("CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');")

	schema := `"public"`
	orders := TableFQN{schema, `"orders"`}
	users := TableFQN{schema, `"users"`}
	events := TableFQN{schema, `"events"`}

	err = validate("pub_missing", []TableFQN{orders})
	require.ErrorIs(t, err, errPublicationNotFound)

	exec("CREATE PUBLICATION pub_some FOR TABLE orders, events;")
	require.NoError(t, validate("pub_some", []TableFQN{orders}))
	require.NoError(t, validate("pub_some", nil))
	// Without publish_via_partition_root the publication lists the
	// partition, not the partitioned table.
	require.NoError(t, validate("pub_some", []TableFQN{events}))

	err = validate("pub_some", []TableFQN{orders, users, {schema, `"no_such_table"`}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"public"."users"`)
	assert.Contains(t, err.Error(), `"public"."no_such_table"`)
	assert.NotContains(t, err.Error(), `"public"."orders"`)

	// Validation must never alter the publication.
	tables, _, err := GetPublicationTables(ctx, conn, "pub_some")
	require.NoError(t, err)
	assert.NotContains(t, tables, users)

	// A publication listing only some partitions does not cover the
	// partitioned table, but does cover the partition it lists.
	exec("CREATE TABLE events_2027 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');")
	exec("CREATE PUBLICATION pub_partial FOR TABLE events_2026;")
	err = validate("pub_partial", []TableFQN{events})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"public"."events"`)
	require.NoError(t, validate("pub_partial", []TableFQN{{schema, `"events_2026"`}}))

	// pub_some lists the partitioned table, so a partition attached later is
	// covered too.
	require.NoError(t, validate("pub_some", []TableFQN{events}))

	exec("CREATE PUBLICATION pub_root FOR TABLE events WITH (publish_via_partition_root = true);")
	require.NoError(t, validate("pub_root", []TableFQN{events, {schema, `"events_2027"`}}))

	exec("CREATE PUBLICATION pub_all FOR ALL TABLES;")
	require.NoError(t, validate("pub_all", []TableFQN{orders, users, events}))

	// A publication that leaves out updates or deletes would silently drop
	// them, so it fails, naming the option.
	exec("CREATE PUBLICATION pub_inserts FOR TABLE orders WITH (publish = 'insert');")
	err = validate("pub_inserts", []TableFQN{orders})
	require.ErrorIs(t, err, errPublicationOperations)
	assert.Contains(t, err.Error(), "does not publish update, delete")
	assert.Contains(t, err.Error(), "publish")

	// A row filter or a column list (PostgreSQL 15 or later; the container
	// runs 16) still streams, so it is reported rather than failed.
	exec("CREATE PUBLICATION pub_rowfilter FOR TABLE orders WHERE (id > 10), users;")
	filtered, err := ValidatePublication(ctx, conn, "pub_rowfilter", []TableFQN{orders, users})
	require.NoError(t, err)
	assert.Equal(t, []TableFQN{orders}, filtered)
	exec("CREATE PUBLICATION pub_columns FOR TABLE orders (id), users;")
	filtered, err = ValidatePublication(ctx, conn, "pub_columns", []TableFQN{orders, users})
	require.NoError(t, err)
	assert.Equal(t, []TableFQN{orders}, filtered)
	exec("CREATE PUBLICATION pub_all_columns FOR TABLE orders (id, note);")
	filtered, err = ValidatePublication(ctx, conn, "pub_all_columns", []TableFQN{orders})
	require.NoError(t, err)
	assert.Empty(t, filtered, "a column list naming every column filters nothing")
}

// TestIntegrationBackfillRequiresTablePublishedAsItself checks the
// incremental snapshot only accepts a table whose changes stream under its
// own name, unfiltered, against the publication layouts a DBA can build.
func TestIntegrationBackfillRequiresTablePublishedAsItself(t *testing.T) {
	integration.CheckSkip(t)

	_, dbURL := createDockerInstance(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	// A plain query connection, as the incremental snapshot uses.
	db, err := sql.Open("postgres", strings.Replace(dbURL, " replication=database", "", 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	exec := func(sql string) {
		t.Helper()
		_, err := db.ExecContext(ctx, sql)
		require.NoError(t, err)
	}
	exec("CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);")
	exec("CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');")
	exec("CREATE TABLE events_2027 PARTITION OF events FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');")
	exec("CREATE TABLE orders (id int PRIMARY KEY, note text);")

	newStream := func(slot string) *Stream {
		return &Stream{
			slotName:                     slot,
			snapshotSchema:               "public",
			externallyManagedPublication: true,
			logger:                       service.MockResources().Logger(),
			incSnapshot: incrementalSnapshot{
				conn:    db,
				pkCache: map[string][]string{},
			},
		}
	}
	events := incrementalsnapshot.TableID{Schema: "public", Table: "events"}
	events2027 := incrementalsnapshot.TableID{Schema: "public", Table: "events_2027"}
	orders := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	// Every leaf is listed, so the parent counts as covered, but its changes
	// stream under the leaf names even with publish_via_partition_root.
	exec("CREATE PUBLICATION pglog_stream_leaves FOR TABLE events_2026, events_2027 WITH (publish_via_partition_root = true);")
	s := newStream("leaves")
	require.ErrorIs(t, s.checkBackfillable(ctx, events), errSignalRejected)
	require.NoError(t, s.checkBackfillable(ctx, events2027), "a leaf listed as itself streams as itself")

	// The root published via the root streams a leaf's changes as the root.
	exec("CREATE PUBLICATION pglog_stream_root FOR TABLE events WITH (publish_via_partition_root = true);")
	s = newStream("root")
	require.NoError(t, s.checkBackfillable(ctx, events))
	err = s.checkBackfillable(ctx, events2027)
	require.ErrorIs(t, err, errSignalRejected)
	assert.Contains(t, err.Error(), "under its own name")

	// The root without the option streams under the leaf names.
	exec("CREATE PUBLICATION pglog_stream_noroot FOR TABLE events;")
	s = newStream("noroot")
	require.ErrorIs(t, s.checkBackfillable(ctx, events), errSignalRejected)
	require.NoError(t, s.checkBackfillable(ctx, events2027))

	// A row filter or column list streams only part of the table.
	exec("CREATE PUBLICATION pglog_stream_filtered FOR TABLE orders WHERE (id > 10);")
	err = newStream("filtered").checkBackfillable(ctx, orders)
	require.ErrorIs(t, err, errSignalRejected)
	assert.Contains(t, err.Error(), "row filter or a column list")
	exec("CREATE PUBLICATION pglog_stream_columns FOR TABLE orders (id);")
	require.ErrorIs(t, newStream("columns").checkBackfillable(ctx, orders), errSignalRejected)
	exec("CREATE PUBLICATION pglog_stream_plain FOR TABLE orders;")
	require.NoError(t, newStream("plain").checkBackfillable(ctx, orders))

	// FOR ALL TABLES lists every table, partitions as leaves.
	exec("CREATE PUBLICATION pglog_stream_all FOR ALL TABLES;")
	s = newStream("all")
	require.NoError(t, s.checkBackfillable(ctx, orders))
	require.ErrorIs(t, s.checkBackfillable(ctx, events), errSignalRejected)
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
