// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// pgoutput frame encoders (proto_version 1, text tuple format). These mirror
// the Decode methods in replication_message.go byte for byte.

func appendCString(dst []byte, s string) []byte {
	dst = append(dst, s...)
	return append(dst, 0)
}

// encodeRelation builds a RelationMessage. Every column is typed by OID from
// colOIDs; the first column is flagged as the key.
func encodeRelation(relID uint32, namespace, name string, cols []string, colOIDs []uint32) []byte {
	buf := []byte{byte(MessageTypeRelation)}
	buf = binary.BigEndian.AppendUint32(buf, relID)
	buf = appendCString(buf, namespace)
	buf = appendCString(buf, name)
	buf = append(buf, 'd') // replica identity: default
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(cols)))
	for i, c := range cols {
		flags := byte(0)
		if i == 0 {
			flags = 1
		}
		buf = append(buf, flags)
		buf = appendCString(buf, c)
		buf = binary.BigEndian.AppendUint32(buf, colOIDs[i])
		buf = binary.BigEndian.AppendUint32(buf, math.MaxUint32) // atttypmod -1
	}
	return buf
}

func encodeBegin(finalLSN LSN, xid uint32) []byte {
	buf := []byte{byte(MessageTypeBegin)}
	buf = binary.BigEndian.AppendUint64(buf, uint64(finalLSN))
	buf = binary.BigEndian.AppendUint64(buf, uint64(timeToPgTime(time.Now())))
	return binary.BigEndian.AppendUint32(buf, xid)
}

func encodeCommit(commitLSN, txnEndLSN LSN) []byte {
	buf := []byte{byte(MessageTypeCommit), 0} // flags = 0
	buf = binary.BigEndian.AppendUint64(buf, uint64(commitLSN))
	buf = binary.BigEndian.AppendUint64(buf, uint64(txnEndLSN))
	return binary.BigEndian.AppendUint64(buf, uint64(timeToPgTime(time.Now())))
}

// encodeInsert builds an InsertMessage whose tuple carries every value in text format.
func encodeInsert(relID uint32, values []string) []byte {
	buf := []byte{byte(MessageTypeInsert)}
	buf = binary.BigEndian.AppendUint32(buf, relID)
	buf = append(buf, 'N')
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = append(buf, TupleDataTypeText)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

// encodeXLogData wraps a pgoutput message in the replication-protocol XLogData
// frame ('w' + WALStart + ServerWALEnd + ServerTime + data), as ParseXLogData expects.
func encodeXLogData(walStart LSN, walData []byte) []byte {
	buf := make([]byte, 0, 25+len(walData))
	buf = append(buf, XLogDataByteID)
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart))
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart)+uint64(len(walData)))
	buf = binary.BigEndian.AppendUint64(buf, uint64(timeToPgTime(time.Now())))
	return append(buf, walData...)
}

// ---------------------------------------------------------------------------
// fakeWalSender is the smallest Postgres backend that pgconn will connect to
// and that StartReplication will accept: it completes the startup handshake,
// answers START_REPLICATION with CopyBothResponse, replays the given CopyData
// frames, drains the client's standby status updates, then closes.

type fakeWalSender struct {
	ln     net.Listener
	frames [][]byte
	done   chan error
}

func newFakeWalSender(tb testing.TB, frames [][]byte) *fakeWalSender {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	f := &fakeWalSender{ln: ln, frames: frames, done: make(chan error, 1)}
	go func() { f.done <- f.serveOnce() }()
	tb.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeWalSender) addr() string { return f.ln.Addr().String() }

func (f *fakeWalSender) serveOnce() error {
	conn, err := f.ln.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)

	if _, err := be.ReceiveStartupMessage(); err != nil {
		return fmt.Errorf("startup: %w", err)
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("handshake flush: %w", err)
	}

	for {
		msg, err := be.Receive()
		if err != nil {
			return fmt.Errorf("waiting for START_REPLICATION: %w", err)
		}
		q, ok := msg.(*pgproto3.Query)
		if !ok {
			continue
		}
		if !strings.HasPrefix(q.String, "START_REPLICATION") {
			return fmt.Errorf("unexpected query %q", q.String)
		}
		break
	}
	be.Send(&pgproto3.CopyBothResponse{OverallFormat: 0})
	if err := be.Flush(); err != nil {
		return fmt.Errorf("copy-both flush: %w", err)
	}

	// The client sends StandbyStatusUpdate CopyData frames; drain them so its
	// writes never block. Backend reads and writes are independent buffers.
	go func() {
		for {
			if _, err := be.Receive(); err != nil {
				return
			}
		}
	}()

	const flushEvery = 64
	for i, fr := range f.frames {
		be.Send(&pgproto3.CopyData{Data: fr})
		if i%flushEvery == flushEvery-1 {
			if err := be.Flush(); err != nil {
				return fmt.Errorf("frame flush: %w", err)
			}
		}
	}
	return be.Flush()
}

// ---------------------------------------------------------------------------

// BenchmarkStreamMessages drives Stream.streamMessages against a fake walsender
// replaying b.N inserted rows (~1200 B each, orders-cdc shape) grouped 100 per
// transaction, with a consumer goroutine draining the messages channel.
// Reports ns/row, MB/s (via SetBytes) and allocations.
//
// The pre-built frame slice holds roughly 1.2 KB per row for the whole of
// b.N, so at the documented -benchtime 200000x that is about 245 MB resident
// for the frames alone; do not raise it by 10x casually. The measured time
// also includes the fake sender's pgproto3 encoding and loopback syscalls, so
// ns/row is a pipeline figure rather than a pure reader cost.
//
//	go test ./internal/impl/postgresql/pglogicalstream/ -run '^$' -bench BenchmarkStreamMessages -benchmem -benchtime 200000x -count 3
func BenchmarkStreamMessages(b *testing.B) {
	const (
		relID      = uint32(16385)
		rowsPerTxn = 100
		payloadLen = 1100
	)
	cols := []string{"id", "amount", "created_at", "payload"}
	colOIDs := []uint32{20 /* int8 */, 1700 /* numeric */, 1184 /* timestamptz */, 25 /* text */}
	payload := strings.Repeat("x", payloadLen)

	txns := (b.N + rowsPerTxn - 1) / rowsPerTxn
	frames := make([][]byte, 0, 1+txns*(rowsPerTxn+2))
	lsn := LSN(1 << 32)
	startLSN := lsn
	next := func(wal []byte) []byte {
		fr := encodeXLogData(lsn, wal)
		lsn += LSN(len(wal))
		return fr
	}
	frames = append(frames, next(encodeRelation(relID, "public", "orders", cols, colOIDs)))
	var insertBytes int
	row := 0
	for t := range txns {
		frames = append(frames, next(encodeBegin(lsn, uint32(t+1))))
		for range rowsPerTxn {
			row++
			wal := encodeInsert(relID, []string{
				strconv.Itoa(row), "1234.56", "2026-09-10 12:00:00+00", payload,
			})
			if row <= b.N {
				insertBytes += len(wal)
			}
			frames = append(frames, next(wal))
		}
		frames = append(frames, next(encodeCommit(lsn, lsn+1)))
	}

	srv := newFakeWalSender(b, frames)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, err := pgconn.ParseConfig("postgres://bench@" + srv.addr() + "/bench?sslmode=disable")
	require.NoError(b, err)
	pgConn, err := pgconn.ConnectConfig(ctx, cfg)
	require.NoError(b, err)
	defer func() { _ = pgConn.Close(context.Background()) }()

	s := &Stream{
		pgConn:                pgConn,
		shutSig:               shutdown.NewSignaller(),
		messages:              make(chan []StreamMessage, streamChannelDepth),
		errors:                make(chan error, 1),
		slotName:              "bench",
		standbyMessageTimeout: time.Second,
		streamMaxRows:         streamBatchMaxRows,
	}
	require.NoError(b, StartReplication(ctx, pgConn, "bench", startLSN, StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names 'bench'"},
	}))

	gotAll := make(chan struct{})
	go func() {
		n := 0
		for batch := range s.messages {
			n += len(batch)
			if n >= b.N {
				close(gotAll)
				return
			}
		}
	}()

	b.SetBytes(int64(insertBytes / b.N))
	b.ResetTimer()
	streamDone := make(chan error, 1)
	go func() { streamDone <- s.streamMessages(startLSN) }()

	// Wait for the consumer to see b.N rows, but guard against the fake
	// walsender or streamMessages failing silently: a non-nil serveOnce error
	// fails the benchmark immediately, a nil one (the fake finished replaying
	// all frames, which can happen before the consumer reaches b.N) is
	// expected and just drops out of the select, an early exit from
	// streamMessages itself is also a failure, and an overall timeout catches
	// any other hang with a clear message instead of blocking forever.
	srvDone := srv.done
	timeout := time.After(2 * time.Minute)
waitForRows:
	for {
		select {
		case <-gotAll:
			break waitForRows
		case err := <-srvDone:
			if err != nil {
				b.Fatalf("fake walsender: %v", err)
			}
			srvDone = nil
		case err := <-streamDone:
			b.Fatalf("streamMessages exited before delivering %d rows: %v", b.N, err)
		case <-timeout:
			b.Fatal("timed out waiting for streamMessages to deliver b.N rows")
		}
	}
	b.StopTimer()

	s.shutSig.TriggerSoftStop()
	select {
	case <-streamDone: // nil on soft stop, or a wrapped error (e.g. a connection reset, not necessarily EOF) from the blocked channel send once the fake closes; both fine
	case <-time.After(10 * time.Second):
		b.Fatal("streamMessages did not stop")
	}
}
