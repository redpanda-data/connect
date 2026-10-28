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

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
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

	tables, err := GetPublicationTables(t.Context(), conn, publicationName)
	require.NoError(t, err)
	assert.Empty(t, tables)
	assert.True(t, publicationIsForAllTables(t, conn, publicationName))

	multiReader := conn.Exec(t.Context(), "CREATE TABLE test_table (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	publicationWithTables := "test_pub_with_tables"
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{{schema, `"test_table"`}})
	require.NoError(t, err)

	tables, err = GetPublicationTables(t.Context(), conn, publicationName)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.True(t, publicationIsForAllTables(t, conn, publicationName))

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

	tables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, publicationIsForAllTables(t, conn, publicationWithTables))

	// Remove one table from the publication
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table"},
	})
	require.NoError(t, err)

	tables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.False(t, publicationIsForAllTables(t, conn, publicationWithTables))

	// Add one table and remove one at the same time
	err = CreatePublication(t.Context(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table2"},
	})
	require.NoError(t, err)

	tables, err = GetPublicationTables(t.Context(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, publicationIsForAllTables(t, conn, publicationWithTables))

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

	tables, err = GetPublicationTables(t.Context(), conn, publicationQuotedIdentifiers)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{`"FooBar"`, `"Foo"`})
	assert.False(t, publicationIsForAllTables(t, conn, publicationQuotedIdentifiers))
}

// publicationIsForAllTables reads pg_publication.puballtables, which is the
// only reliable way to tell a FOR ALL TABLES publication from a FOR TABLE one:
// pg_publication_tables lists every table for the former and nothing for the
// latter once all of its members have been dropped.
func publicationIsForAllTables(t *testing.T, conn *pgconn.PgConn, publicationName string) bool {
	t.Helper()
	q, err := sanitize.SQLQuery("SELECT puballtables FROM pg_publication WHERE pubname = $1;", publicationName)
	require.NoError(t, err)
	rows, err := conn.Exec(t.Context(), q).ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, rows[0].Rows, 1, "publication %s must exist", publicationName)
	return string(rows[0].Rows[0][0]) == "t"
}

// An empty table list means "replicate every table". When the publication
// already exists as FOR TABLE, it must be recreated FOR ALL TABLES rather than
// having every member dropped, which would leave an empty publication that
// streams nothing.
func TestIntegrationCreatePublicationSwitchesToAllTables(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*30)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = conn.Exec(ctx, `
		CREATE TABLE listed (id serial PRIMARY KEY, name text);
		CREATE TABLE unlisted (id serial PRIMARY KEY, name text);
	`).ReadAll()
	require.NoError(t, err)

	schema := `"public"`
	listed := TableFQN{Schema: schema, Table: `"listed"`}
	unlisted := TableFQN{Schema: schema, Table: `"unlisted"`}

	t.Run("FOR TABLE publication with members", func(t *testing.T) {
		pub := "pub_with_members"
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE listed;", pub)).ReadAll()
		require.NoError(t, err)
		require.False(t, publicationIsForAllTables(t, conn, pub))

		require.NoError(t, CreatePublication(ctx, conn, pub, nil))

		assert.True(t, publicationIsForAllTables(t, conn, pub))
		tables, err := GetPublicationTables(ctx, conn, pub)
		require.NoError(t, err)
		assert.Contains(t, tables, listed)
		assert.Contains(t, tables, unlisted)

		// Already FOR ALL TABLES: a second call must leave it alone.
		require.NoError(t, CreatePublication(ctx, conn, pub, nil))
		assert.True(t, publicationIsForAllTables(t, conn, pub))
	})

	t.Run("FOR TABLE publication emptied of every member", func(t *testing.T) {
		// This is what the bug used to leave behind, so a restart must repair it.
		pub := "pub_without_members"
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE listed; ALTER PUBLICATION %s DROP TABLE listed;", pub, pub)).ReadAll()
		require.NoError(t, err)
		require.False(t, publicationIsForAllTables(t, conn, pub))

		require.NoError(t, CreatePublication(ctx, conn, pub, nil))

		assert.True(t, publicationIsForAllTables(t, conn, pub))
		tables, err := GetPublicationTables(ctx, conn, pub)
		require.NoError(t, err)
		assert.Contains(t, tables, listed)
		assert.Contains(t, tables, unlisted)
	})

	t.Run("FOR TABLE publication with a table list is altered in place", func(t *testing.T) {
		pub := "pub_altered"
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE listed;", pub)).ReadAll()
		require.NoError(t, err)

		require.NoError(t, CreatePublication(ctx, conn, pub, []TableFQN{unlisted}))

		assert.False(t, publicationIsForAllTables(t, conn, pub))
		tables, err := GetPublicationTables(ctx, conn, pub)
		require.NoError(t, err)
		assert.Equal(t, []TableFQN{unlisted}, tables)
	})

	t.Run("emptied FOR TABLE publication with a table list gets the tables added", func(t *testing.T) {
		// An empty member list used to be mistaken for FOR ALL TABLES and left
		// alone, so the configured tables never streamed.
		pub := "pub_emptied_then_listed"
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE listed; ALTER PUBLICATION %s DROP TABLE listed;", pub, pub)).ReadAll()
		require.NoError(t, err)

		require.NoError(t, CreatePublication(ctx, conn, pub, []TableFQN{listed}))

		assert.False(t, publicationIsForAllTables(t, conn, pub))
		tables, err := GetPublicationTables(ctx, conn, pub)
		require.NoError(t, err)
		assert.Equal(t, []TableFQN{listed}, tables)
	})

	t.Run("recreated publication keeps its options", func(t *testing.T) {
		// publish_via_partition_root is what the incremental snapshot needs for
		// partitioned tables, so losing it on recreation would break them.
		pub := "pub_with_options"
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE listed WITH (publish = 'insert', publish_via_partition_root = true);", pub)).ReadAll()
		require.NoError(t, err)

		require.NoError(t, CreatePublication(ctx, conn, pub, nil))

		assert.True(t, publicationIsForAllTables(t, conn, pub))
		q, err := sanitize.SQLQuery("SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot FROM pg_publication WHERE pubname = $1;", pub)
		require.NoError(t, err)
		rows, err := conn.Exec(ctx, q).ReadAll()
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Len(t, rows[0].Rows, 1)
		var got []string
		for _, v := range rows[0].Rows[0] {
			got = append(got, string(v))
		}
		assert.Equal(t, []string{"t", "f", "f", "f", "t"}, got)
	})

	t.Run("recreating without superuser explains what to do", func(t *testing.T) {
		pub := "pub_owned_by_non_superuser"
		_, err := conn.Exec(ctx, fmt.Sprintf(`
			CREATE ROLE pub_owner LOGIN REPLICATION PASSWORD 'secret';
			GRANT CREATE ON DATABASE dbname TO pub_owner;
			CREATE PUBLICATION %s FOR TABLE listed;
			ALTER PUBLICATION %s OWNER TO pub_owner;
		`, pub, pub)).ReadAll()
		require.NoError(t, err)

		ownerConn, err := pgconn.Connect(ctx, strings.Replace(dbURL, "user=user_name", "user=pub_owner", 1))
		require.NoError(t, err)
		defer closeConn(t, ownerConn)

		err = CreatePublication(ctx, ownerConn, pub, nil)
		require.ErrorContains(t, err, "recreate it FOR ALL TABLES as a superuser, or list the tables to replicate in tables")

		// The failed recreation rolls back, so the publication is untouched.
		assert.False(t, publicationIsForAllTables(t, conn, pub))
		tables, err := GetPublicationTables(ctx, conn, pub)
		require.NoError(t, err)
		assert.Equal(t, []TableFQN{listed}, tables)
	})
}

// The docs suggest creating the publication ahead of time, so that the
// replication user does not need to own it. When it already lists the
// configured tables, CreatePublication must not run ALTER PUBLICATION, which
// only the owner may run.
func TestIntegrationCreatePublicationLeavesMatchingPublicationAlone(t *testing.T) {
	integration.CheckSkip(t)

	cleanup, dbURL := createDockerInstance(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second*30)
	defer cancel()

	owner, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, owner)

	_, err = owner.Exec(ctx, `
		CREATE TABLE listed (id serial PRIMARY KEY, name text);
		CREATE PUBLICATION pub_precreated FOR TABLE listed;
		CREATE ROLE replicator LOGIN REPLICATION PASSWORD 'secret';
	`).ReadAll()
	require.NoError(t, err)

	replicatorURL := strings.Replace(dbURL, "user=user_name", "user=replicator", 1)
	require.NotEqual(t, dbURL, replicatorURL)
	replicator, err := pgconn.Connect(ctx, replicatorURL)
	require.NoError(t, err)
	defer closeConn(t, replicator)

	listed := TableFQN{Schema: `"public"`, Table: `"listed"`}
	require.NoError(t, CreatePublication(ctx, replicator, "pub_precreated", []TableFQN{listed}))
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
