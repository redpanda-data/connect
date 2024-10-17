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
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
	var lsn pglogrepl.LSN
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
	var lsnPtr *pglogrepl.LSN
	err := lsnPtr.Scan("16/B374D848")
	s.NoError(err)
}

func (s *lsnSuite) TestValueInterface() {
	lsn := pglogrepl.LSN(97500059720)
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

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
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

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.TimelineHistory(ctx, conn, 0)
	require.Error(t, err)

	_, err = pglogrepl.TimelineHistory(ctx, conn, 1)
	require.Error(t, err)

	if sysident.Timeline > 1 {
		// This test requires a Postgres with at least 1 timeline increase (promote, or recover)...
		tlh, err := pglogrepl.TimelineHistory(ctx, conn, sysident.Timeline)
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

	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
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

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = pglogrepl.DropReplicationSlot(ctx, conn, slotName, pglogrepl.DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{})
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

	rxKeepAlive := func() pglogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() pglogrepl.XLogData {
		var cdMsg *pgproto3.CopyData
		// Discard keepalive messages
		for {
			msg, err := conn.ReceiveMessage(ctx)
			require.NoError(t, err)
			var ok bool
			cdMsg, ok = msg.(*pgproto3.CopyData)
			require.True(t, ok)
			if cdMsg.Data[0] != pglogrepl.PrimaryKeepaliveMessageByteID {
				break
			}
		}
		require.Equal(t, byte(pglogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := pglogrepl.ParseXLogData(cdMsg.Data[1:])
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

func TestStartReplicationPhysical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "", pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{Mode: pglogrepl.PhysicalReplication})
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
create table mytable(id int primary key, name text);
drop table mytable;
`).ReadAll()
		require.NoError(t, err)
	}()

	_ = func() pglogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() pglogrepl.XLogData {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := pglogrepl.ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	xld := rxXLogData()
	assert.Contains(t, string(xld.WALData), "mytable")

	copyDoneResult, err := pglogrepl.SendStandbyCopyDone(ctx, conn)
	require.NoError(t, err)
	assert.Nil(t, copyDoneResult)
}

func TestBaseBackup(t *testing.T) {
	// base backup test could take a long time. Therefore it can be disabled.
	envSkipTest := os.Getenv("PGLOGREPL_SKIP_BASE_BACKUP")
	if envSkipTest != "" {
		skipTest, err := strconv.ParseBool(envSkipTest)
		if err != nil {
			t.Error(err)
		} else if skipTest {
			return
		}
	}

	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	options := pglogrepl.BaseBackupOptions{
		NoVerifyChecksums: true,
		Progress:          true,
		Label:             "pglogrepltest",
		Fast:              true,
		WAL:               true,
		NoWait:            true,
		MaxRate:           1024,
		TablespaceMap:     true,
	}
	startRes, err := pglogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
	require.GreaterOrEqual(t, startRes.TimelineID, int32(1))

	//Write the tablespaces
	for i := 0; i < len(startRes.Tablespaces)+1; i++ {
		f, err := os.CreateTemp("", fmt.Sprintf("pglogrepl_test_tbs_%d.tar", i))
		require.NoError(t, err)
		err = pglogrepl.NextTableSpace(context.Background(), conn)
		var message pgproto3.BackendMessage
	L:
		for {
			message, err = conn.ReceiveMessage(context.Background())
			require.NoError(t, err)
			switch msg := message.(type) {
			case *pgproto3.CopyData:
				_, err := f.Write(msg.Data)
				require.NoError(t, err)
			case *pgproto3.CopyDone:
				break L
			default:
				t.Errorf("Received unexpected message: %#v\n", msg)
			}
		}
		err = f.Close()
		require.NoError(t, err)
	}

	stopRes, err := pglogrepl.FinishBaseBackup(context.Background(), conn)
	require.NoError(t, err)
	require.Equal(t, startRes.TimelineID, stopRes.TimelineID)
	require.Equal(t, len(stopRes.Tablespaces), 0)
	require.Less(t, uint64(startRes.LSN), uint64(stopRes.LSN))
	_, err = pglogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
}

func TestSendStandbyStatusUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: sysident.XLogPos})
	require.NoError(t, err)
}
