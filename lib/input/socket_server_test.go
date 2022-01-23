package input

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSocketServerBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)
		wg.Done()
	}()

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)
		wg.Done()
	}()

	readNextMsg := func(reject bool) (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res types.Response = response.NewAck()
			if reject {
				res = response.NewError(errors.New("test err"))
			}
			select {
			case tran.ResponseChan <- res:
			case <-time.After(time.Second * 5):
				return nil, errors.New("timed out")
			}
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

func TestSocketServerWriteToClosed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conn, err := net.Dial("unix", conf.SocketServer.Address)
	require.NoError(t, err)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second*3))

	_, cerr := conn.Write([]byte("bar\n"))
	require.Error(t, cerr)

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketServerReconnect(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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

func TestSocketServerMultipart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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

func TestSocketServerMultipartCustomDelim(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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

func TestSocketServerMultipartShutdown(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_socket_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	conf := NewConfig()
	conf.SocketServer.Network = "unix"
	conf.SocketServer.Address = filepath.Join(tmpDir, "benthos.sock")
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func(reject bool) (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			var res types.Response = response.NewAck()
			if reject {
				res = response.NewError(errors.New("test err"))
			}
			select {
			case tran.ResponseChan <- res:
			case <-time.After(time.Second * 5):
				return nil, errors.New("timed out")
			}
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

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	addr := rdr.(*SocketServer).Addr()

	conn, err := net.Dial("udp", addr.String())
	require.NoError(t, err)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second*3))

	// Just make sure data written doesn't panic
	_, _ = conn.Write([]byte("bar\n"))

	_, open := <-rdr.TransactionChan()
	assert.False(t, open)

	conn.Close()
}

func TestSocketUDPServerReconnect(t *testing.T) {
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "udp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

		_, cerr := conn.Write([]byte("foo\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("bar\n"))
		require.NoError(t, cerr)

		_, cerr = conn.Write([]byte("baz\n"))
		require.NoError(t, cerr)

		wg.Done()
	}()

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "delim:@/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
	conf := NewConfig()
	conf.SocketServer.Network = "tcp"
	conf.SocketServer.Address = "127.0.0.1:0"
	conf.SocketServer.Codec = "lines/multipart"

	rdr, err := NewSocketServer(conf, nil, log.Noop(), metrics.Noop())
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
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))

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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-rdr.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
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
