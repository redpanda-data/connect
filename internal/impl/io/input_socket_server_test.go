package io_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestSocketServerBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketServerRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)
		wg.Done()
	}()

	readNextMsg := func(reject bool) (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res error
			if reject {
				res = errors.New("test err")
			}
			require.NoError(t, tran.Ack(ctx, res))
		case <-time.After(time.Second * 5):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg(false)
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg(true)
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	expRemaining := []string{"bar", "baz"}
	actRemaining := []string{}

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).AsBytes()))

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).AsBytes()))

	sort.Strings(actRemaining)
	assert.Equal(t, expRemaining, actRemaining)

	wg.Wait()
	conn.Close()
}

func TestSocketServerWriteClosed(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.TriggerStopConsuming()
	assert.NoError(t, rdr.WaitForClose(ctx))

	_, cerr := conn.Write([]byte("bar\n"))
	require.Error(t, cerr)

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketServerRecon(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		conn.Close()
		conn, cerr = net.Dial("unix", addr.String())
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	expMsgs := map[string]struct{}{
		"foo": {},
		"bar": {},
		"baz": {},
	}

	for i := 0; i < 3; i++ {
		msg, err := readNextMsg()
		require.NoError(t, err)

		act := string(msg.Get(0).AsBytes())
		assert.Contains(t, expMsgs, act)

		delete(expMsgs, act)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketServerMpart(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketServerMpartCDelim(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n@@"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz\n")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketServerMpartSdown(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		conn.Close()
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
}

func TestSocketUDPServerBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketUDPServerRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func(reject bool) (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res error
			if reject {
				res = errors.New("test err")
			}
			require.NoError(t, tran.Ack(ctx, res))
		case <-time.After(time.Second * 5):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg(false)
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg(true)
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	expRemaining := []string{"bar", "baz"}
	actRemaining := []string{}

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).AsBytes()))

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).AsBytes()))

	sort.Strings(actRemaining)
	assert.Equal(t, expRemaining, actRemaining)

	wg.Wait()
	conn.Close()
}

func TestUDPServerWriteToClosed(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.TriggerStopConsuming()
	assert.NoError(t, rdr.WaitForClose(ctx))

	// Just make sure data written doesn't panic
	_, _ = conn.Write([]byte("bar\n"))

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketUDPServerReconnect(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		conn.Close()

		conn, cerr = net.Dial("udp", addr.String())
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketUDPServerCustomDelim(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n@@"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz\n")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestSocketUDPServerShutdown(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		conn.Close()
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
}

func TestTCPSocketServerBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestTCPSocketServerReconnect(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		conn.Close()

		conn, cerr = net.Dial("tcp", addr.String())
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	expMsgs := map[string]struct{}{
		"foo": {},
		"bar": {},
		"baz": {},
	}

	for i := 0; i < 3; i++ {
		msg, err := readNextMsg()
		require.NoError(t, err)

		act := string(msg.Get(0).AsBytes())
		assert.Contains(t, expMsgs, act)
		delete(expMsgs, act)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketServerMultipart(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestTCPSocketServerMultipartCustomDelim(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("@"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n@@"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz\n")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}

func TestTCPSocketServerMultipartShutdown(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(ctx))
	}()

	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		conn.Close()
		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(ctx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
}

func TestTLSSocketServerBasic(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := input.NewConfig()
	conf.Type = "socket_server"
	conf.SocketServer.Network = "tls"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.TLS.SelfSigned = true

	rdr, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	addr := rdr.(interface{ Addr() net.Addr }).Addr()

	defer func() {
		rdr.TriggerStopConsuming()
		assert.NoError(t, rdr.WaitForClose(tCtx))
	}()

	conn, err := tls.Dial("tcp", addr.String(), &tls.Config{
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	exp := [][]byte{[]byte("foo")}
	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("bar")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	exp = [][]byte{[]byte("baz")}
	msg, err = readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, exp, message.GetAllBytes(msg))

	wg.Wait()
	conn.Close()
}
