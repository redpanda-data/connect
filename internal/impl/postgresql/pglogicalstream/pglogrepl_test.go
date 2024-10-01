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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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
const outputPlugin = "test_decoding"

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func TestIdentifySystem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	assert.Greater(t, len(sysident.SystemID), 0)
	assert.True(t, sysident.Timeline > 0)
	assert.True(t, sysident.XLogPos > 0)
	assert.Greater(t, len(sysident.DBName), 0)
}

func TestGetHistoryFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	config, err := pgconn.ParseConfig(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	config.RuntimeParams["replication"] = "on"

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = TimelineHistory(ctx, conn, 0)
	require.Error(t, err)

	_, err = TimelineHistory(ctx, conn, 1)
	require.Error(t, err)

	if sysident.Timeline > 1 {
		// This test requires a Postgres with at least 1 timeline increase (promote, or recover)...
		tlh, err := TimelineHistory(ctx, conn, sysident.Timeline)
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf("%08X.history", sysident.Timeline)
		assert.Equal(t, expectedFileName, tlh.FileName)
		assert.Greater(t, len(tlh.Content), 0)
	}
}

func TestCreateReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	result, err := CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	assert.Equal(t, slotName, result.SlotName)
	assert.Equal(t, outputPlugin, result.OutputPlugin)
}

func TestDropReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = DropReplicationSlot(ctx, conn, slotName, DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = CreateReplicationSlot(ctx, conn, slotName, outputPlugin, CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = StartReplication(ctx, conn, slotName, sysident.XLogPos, StartReplicationOptions{})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
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
	assert.Equal(t, "BEGIN", string(xld.WALData[:5]))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:1 name[text]:'foo'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:2 name[text]:'bar'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:3 name[text]:'baz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: UPDATE: id[integer]:3 name[text]:'quz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: DELETE: id[integer]:2", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "COMMIT", string(xld.WALData[:6]))
}

func TestSendStandbyStatusUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := IdentifySystem(ctx, conn)
	require.NoError(t, err)

	err = SendStandbyStatusUpdate(ctx, conn, StandbyStatusUpdate{WALWritePosition: sysident.XLogPos})
	require.NoError(t, err)
}
