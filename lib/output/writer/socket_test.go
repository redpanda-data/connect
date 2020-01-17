package writer

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSocketBasic(t *testing.T) {
	ln, err := net.Listen("unix", "/tmp/benthos.sock")
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	conf := NewSocketConfig()
	conf.Network = ln.Addr().Network()
	conf.Address = ln.Addr().String()

	wtr, err := NewSocket(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.Connect(); cerr != nil {
			t.Fatal(cerr)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.Write(message.New([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.New([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.New([][]byte{[]byte("baz")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\nbar\nbaz\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestSocketMultipart(t *testing.T) {
	ln, err := net.Listen("unix", "/tmp/benthos.sock")
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	conf := NewSocketConfig()
	conf.Network = ln.Addr().Network()
	conf.Address = ln.Addr().String()

	wtr, err := NewSocket(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.Connect(); cerr != nil {
			t.Fatal(cerr)
		}
	}()

	conn, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.Write(message.New([][]byte{[]byte("foo"), []byte("bar"), []byte("baz")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.New([][]byte{[]byte("qux")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\nbar\nbaz\n\nqux\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}
