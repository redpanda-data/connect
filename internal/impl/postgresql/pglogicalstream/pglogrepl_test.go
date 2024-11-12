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
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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
	lsnText := "16/B374D848"
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
	s.Equal("16/B374D848", lsnStr)
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

func TestIdentifySystem(t *testing.T) {
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

func TestCreateReplicationSlot(t *testing.T) {
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

	result, err := CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false, SnapshotAction: "export"}, 16, nil)
	require.NoError(t, err)

	assert.Equal(t, slotName, result.SlotName)
}

func TestDropReplicationSlot(t *testing.T) {
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

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false}, 16, nil)
	require.NoError(t, err)

	err = DropReplicationSlot(ctx, conn, slotName, DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false}, 16, nil)
	require.NoError(t, err)
}

func TestCreatePublication(t *testing.T) {
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
	err = CreatePublication(context.Background(), conn, publicationName, []string{})
	require.NoError(t, err)

	tables, forAllTables, err := GetPublicationTables(context.Background(), conn, publicationName)
	require.NoError(t, err)
	assert.Empty(t, tables)
	assert.True(t, forAllTables)

	multiReader := conn.Exec(context.Background(), "CREATE TABLE test_table (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	publicationWithTables := "test_pub_with_tables"
	err = CreatePublication(context.Background(), conn, publicationWithTables, []string{"test_table"})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationName)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, "test_table")
	assert.False(t, forAllTables)

	// add more tables to publication
	multiReader = conn.Exec(context.Background(), "CREATE TABLE test_table2 (id serial PRIMARY KEY, name text);")
	_, err = multiReader.ReadAll()
	require.NoError(t, err)

	// Pass more tables to the publication
	err = CreatePublication(context.Background(), conn, publicationWithTables, []string{
		"test_table2",
		"test_table",
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, "test_table")
	assert.Contains(t, tables, "test_table2")
	assert.False(t, forAllTables)

	// Removing one table from the publication
	err = CreatePublication(context.Background(), conn, publicationWithTables, []string{
		"test_table",
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, "test_table")
	assert.False(t, forAllTables)

	// Add one table and remove one at the same time
	err = CreatePublication(context.Background(), conn, publicationWithTables, []string{
		"test_table2",
	})
	require.NoError(t, err)

	tables, forAllTables, err = GetPublicationTables(context.Background(), conn, publicationWithTables)
	require.NoError(t, err)
	assert.NotEmpty(t, tables)
	assert.Contains(t, tables, "test_table2")
	assert.False(t, forAllTables)

}

func TestStartReplication(t *testing.T) {
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
	err = CreatePublication(context.Background(), conn, publicationName, []string{})
	require.NoError(t, err)

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: false, SnapshotAction: "export"}, 16, nil)
	require.NoError(t, err)

	err = StartReplication(ctx, conn, slotName, sysident.XLogPos, StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names 'test_publication'",
			"messages 'true'",
		},
		Mode: LogicalReplication,
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

	rxKeepAlive()
	xld := rxXLogData()
	begin, err := isBeginMessage(xld.WALData)
	require.NoError(t, err)
	assert.True(t, begin)

	xld = rxXLogData()
	var streamMessage *StreamMessageChanges
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	assert.Nil(t, streamMessage)

	xld = rxXLogData()
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	jsonData, err := json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.Equal(t, "{\"operation\":\"insert\",\"schema\":\"public\",\"table\":\"t\",\"data\":{\"id\":1,\"name\":\"foo\"}}", string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.Equal(t, "{\"operation\":\"insert\",\"schema\":\"public\",\"table\":\"t\",\"data\":{\"id\":2,\"name\":\"bar\"}}", string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.Equal(t, "{\"operation\":\"insert\",\"schema\":\"public\",\"table\":\"t\",\"data\":{\"id\":3,\"name\":\"baz\"}}", string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.Equal(t, "{\"operation\":\"update\",\"schema\":\"public\",\"table\":\"t\",\"data\":{\"id\":3,\"name\":\"quz\"}}", string(jsonData))

	xld = rxXLogData()
	streamMessage, err = decodePgOutput(xld.WALData, relations, typeMap)
	require.NoError(t, err)
	jsonData, err = json.Marshal(&streamMessage)
	require.NoError(t, err)
	assert.Equal(t, "{\"operation\":\"delete\",\"schema\":\"public\",\"table\":\"t\",\"data\":{\"id\":2,\"name\":null}}", string(jsonData))
	xld = rxXLogData()

	var commit bool
	commit, _, err = isCommitMessage(xld.WALData)
	require.NoError(t, err)
	assert.True(t, commit)
}

func TestSendStandbyStatusUpdate(t *testing.T) {
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
