package input

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestSocketServerBasic(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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

func TestSocketServerRetries(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func(reject bool) (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res error
			if reject {
				res = errors.New("test err")
			}
			require.NoError(t, tran.Ack(tCtx, res))
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
	actRemaining = append(actRemaining, string(msg.Get(0).Get()))

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).Get()))

	sort.Strings(actRemaining)
	assert.Equal(t, expRemaining, actRemaining)

	wg.Wait()
	conn.Close()
}

func TestSocketServerWriteClosed(t *testing.T) {
	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second*3))

	_, cerr := conn.Write([]byte("bar\n"))
	require.Error(t, cerr)

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketServerRecon(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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

		act := string(msg.Get(0).Get())
		assert.Contains(t, expMsgs, act)

		delete(expMsgs, act)
	}

	wg.Wait()
	conn.Close()
}

func TestSocketServerMpart(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tmpDir := t.TempDir()

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "b.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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

func TestSocketUDPServerRetries(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func(reject bool) (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res error
			if reject {
				res = errors.New("test err")
			}
			require.NoError(t, tran.Ack(tCtx, res))
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
	actRemaining = append(actRemaining, string(msg.Get(0).Get()))

	msg, err = readNextMsg(false)
	require.NoError(t, err)
	require.Equal(t, 1, msg.Len())
	actRemaining = append(actRemaining, string(msg.Get(0).Get()))

	sort.Strings(actRemaining)
	assert.Equal(t, expRemaining, actRemaining)

	wg.Wait()
	conn.Close()
}

func TestUDPServerWriteToClosed(t *testing.T) {
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	_ = conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second*3))

	// Just make sure data written doesn't panic
	_, _ = conn.Write([]byte("bar\n"))

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketUDPServerReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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

func TestSocketUDPServerCustomDelim(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
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

func TestTCPSocketServerReconnect(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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

		act := string(msg.Get(0).Get())
		assert.Contains(t, expMsgs, act)
		delete(expMsgs, act)
	}

	wg.Wait()
	conn.Close()
}

func TestTCPSocketServerMultipart(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	defer func() {
		rdr.CloseAsync()
		assert.NoError(t, rdr.WaitForClose(time.Second))
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

	readNextMsg := func() (*message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
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
