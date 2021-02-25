package input

import (
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestUDPServerBasic(t *testing.T) {
	conf := NewConfig()
	conf.UDPServer.Address = "127.0.0.1:0"

	rdr, err := NewUDPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	addr := rdr.(*UDPServer).Addr()

	defer func() {
		rdr.CloseAsync()
		if err := rdr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestUDPServerReconnect(t *testing.T) {
	conf := NewConfig()
	conf.UDPServer.Address = "127.0.0.1:0"

	rdr, err := NewUDPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	addr := rdr.(*UDPServer).Addr()

	defer func() {
		rdr.CloseAsync()
		if err := rdr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		_, cerr := conn.Write([]byte("foo\n"))
		if cerr != nil {
			t.Error(cerr)
		}
		conn.Close()
		conn, cerr = net.Dial("udp", addr.String())
		if cerr != nil {
			t.Error(cerr)
		}
		if _, cerr = conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr = conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestUDPServerCustomDelim(t *testing.T) {
	conf := NewConfig()
	conf.UDPServer.Address = "127.0.0.1:0"
	conf.UDPServer.Delim = "@"

	rdr, err := NewUDPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	addr := rdr.(*UDPServer).Addr()

	defer func() {
		rdr.CloseAsync()
		if err := rdr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("@")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n@@")); cerr != nil {
			t.Error(cerr)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz\n")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
	conn.Close()
}

func TestUDPServerShutdown(t *testing.T) {
	conf := NewConfig()
	conf.UDPServer.Address = "127.0.0.1:0"

	rdr, err := NewUDPServer(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	addr := rdr.(*UDPServer).Addr()

	defer func() {
		rdr.CloseAsync()
		if err := rdr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
		if _, cerr := conn.Write([]byte("foo\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("bar\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("\n")); cerr != nil {
			t.Error(cerr)
		}
		if _, cerr := conn.Write([]byte("baz\n")); cerr != nil {
			t.Error(cerr)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("bar")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	exp = [][]byte{[]byte("baz")}
	if msg, err = readNextMsg(); err != nil {
		t.Fatal(err)
	}
	if act := message.GetAllBytes(msg); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong message contents: %s != %s", act, exp)
	}

	wg.Wait()
}
