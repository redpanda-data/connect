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

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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

func (s *lsnSuite) Equal(e, a interface{}, args ...interface{}) {
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

const slotName = "pglogrepl_test"
const outputPlugin = "pgoutput"

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func createDockerInstance(t *testing.T) (*dockertest.Pool, *dockertest.Resource, string) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16",
		Env: []string{
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_USER=user_name",
			"POSTGRES_DB=dbname",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})

	require.NoError(t, err)
	require.NoError(t, resource.Expire(120))

	hostAndPort := resource.GetHostPort("5432/tcp")
	hostAndPortSplited := strings.Split(hostAndPort, ":")
	databaseURL := fmt.Sprintf("user=user_name password=secret dbname=dbname sslmode=disable host=%s port=%s replication=database", hostAndPortSplited[0], hostAndPortSplited[1])

	var db *sql.DB
	pool.MaxWait = 120 * time.Second
	err = pool.Retry(func() error {
		if db, err = sql.Open("postgres", databaseURL); err != nil {
			return err
		}

		if err = db.Ping(); err != nil {
			return err
		}

		return err
	})
	require.NoError(t, err)

	return pool, resource, databaseURL
}

func TestIntegrationIdentifySystem(t *testing.T) {
	integration.CheckSkip(t)

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
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

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)
	_, _, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false})
	require.NoError(t, err)
}

func TestIntegrationDropReplicationSlot(t *testing.T) {
	integration.CheckSkip(t)

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	publicationName := "test_publication"
	schema := `"public"`
	err = CreatePublication(context.Background(), conn, publicationName, []TableFQN{})
	require.NoError(t, err)

	tables, forAllTables, err := GetPublicationTables(context.Background(), conn, publicationName)
	require.NoError(t, err)
	assert.Empty(t, tables)
	assert.True(t, forAllTables)

	multiReader := conn.Exec(context.Background(), "CREATE TABLE test_table (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	publicationWithTables := "test_pub_with_tables"
	err = CreatePublication(context.Background(), conn, publicationWithTables, []TableFQN{{schema, `"test_table"`}})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationName)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.False(t, forAllTables)

	// Add more tables to publication
	multiReader = conn.Exec(context.Background(), "CREATE TABLE test_table2 (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	// Pass more tables to the publication
	err = CreatePublication(context.Background(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table2"},
		{schema, "test_table"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 2)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, forAllTables)

	// Remove one table from the publication
	err = CreatePublication(context.Background(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{schema, `"test_table"`})
	assert.False(t, forAllTables)

	// Add one table and remove one at the same time
	err = CreatePublication(context.Background(), conn, publicationWithTables, []TableFQN{
		{schema, "test_table2"},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, TableFQN{schema, `"test_table2"`})
	assert.False(t, forAllTables)

	// Create a schema with a quoted identifier
	caseSensitiveSchema := `"FooBar"`
	multiReader = conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s;", caseSensitiveSchema))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	caseSensitiveTable := `"Foo"`
	multiReader = conn.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	caseSensitiveTable2 := `"Bar"`
	multiReader = conn.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s.%s (id serial PRIMARY KEY, name text);", caseSensitiveSchema, caseSensitiveTable2))
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	// Pass tables to the schema with quoted identifiers
	publicationQuotedIdentifiers := "quoted_identifiers"
	err = CreatePublication(context.Background(), conn, publicationQuotedIdentifiers, []TableFQN{
		{caseSensitiveSchema, caseSensitiveTable},
		{caseSensitiveSchema, caseSensitiveTable2},
	})
	require.NoError(t, err)

	// Remove one table with a quoted identifier from the publication
	err = CreatePublication(context.Background(), conn, publicationQuotedIdentifiers, []TableFQN{
		{caseSensitiveSchema, caseSensitiveTable},
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationQuotedIdentifiers)
	require.NoError(t, err)
	assert.Len(t, tables, 1)
	assert.Contains(t, tables, TableFQN{`"FooBar"`, `"Foo"`})
	assert.False(t, forAllTables)
}

func TestIntegrationStartReplication(t *testing.T) {
	integration.CheckSkip(t)

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, dbURL)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	// create publication
	publicationName := "test_publication"
	err = CreatePublication(context.Background(), conn, publicationName, []TableFQN{})
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

	pool, resource, dbURL := createDockerInstance(t)
	defer func() {
		err := pool.Purge(resource)
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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
