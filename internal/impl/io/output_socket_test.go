package io

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	ooutput "github.com/benthosdev/benthos/v4/internal/old/output"
)

func TestSocketBasic(t *testing.T) {
	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "benthos.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = ln.Addr().Network()
	conf.Address = ln.Addr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
			t.Error(cerr)
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
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("baz")})); err != nil {
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
	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "benthos.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = ln.Addr().Network()
	conf.Address = ln.Addr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
			t.Error(cerr)
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
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar"), []byte("baz")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("qux")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\nbar\nbaz\nqux\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

type wrapPacketConn struct {
	r net.PacketConn
}

func (w *wrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.r.ReadFrom(p)
	return
}

func TestUDPSocketBasic(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		if conn, err = net.ListenPacket("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer conn.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = "udp"
	conf.Address = conn.LocalAddr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
		t.Fatal(cerr)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(&wrapPacketConn{r: conn})
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("baz")})); err != nil {
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

func TestUDPSocketMultipart(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		if conn, err = net.ListenPacket("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer conn.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = "udp"
	conf.Address = conn.LocalAddr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
		t.Fatal(cerr)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(&wrapPacketConn{r: conn})
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar"), []byte("baz")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("qux")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\nbar\nbaz\nqux\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestTCPSocketBasic(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = "tcp"
	conf.Address = ln.Addr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
			t.Error(cerr)
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
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("baz")})); err != nil {
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

func TestTCPSocketMultipart(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if ln, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer ln.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = "tcp"
	conf.Address = ln.Addr().String()

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
			t.Error(cerr)
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
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo"), []byte("bar"), []byte("baz")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("qux")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\nbar\nbaz\nqux\n"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}

func TestSocketCustomDelimeter(t *testing.T) {
	tmpDir := t.TempDir()

	ln, err := net.Listen("unix", filepath.Join(tmpDir, "benthos.sock"))
	if err != nil {
		t.Fatalf("failed to listen on address: %v", err)
	}
	defer ln.Close()

	conf := ooutput.NewSocketConfig()
	conf.Network = ln.Addr().Network()
	conf.Address = ln.Addr().String()
	conf.Codec = "delim:\t"

	wtr, err := newSocketWriter(conf, mock.NewManager(), log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		if cerr := wtr.ConnectWithContext(context.Background()); cerr != nil {
			t.Error(cerr)
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
		_ = conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		_, _ = buf.ReadFrom(conn)
		wg.Done()
	}()

	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte("baz\t")})); err != nil {
		t.Error(err)
	}
	wtr.CloseAsync()
	wg.Wait()

	exp := "foo\tbar\n\tbaz\t"
	if act := buf.String(); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	conn.Close()
}
