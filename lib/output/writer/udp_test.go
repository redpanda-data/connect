package writer

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestUDPBasic(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		if conn, err = net.ListenPacket("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer conn.Close()

	conf := NewUDPConfig()
	conf.Address = conn.LocalAddr().String()

	wtr, err := NewUDP(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if cerr := wtr.Connect(); cerr != nil {
		t.Fatal(cerr)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		buf.ReadFrom(&wrapPacketConn{r: conn})
		wg.Done()
	}()

	if err = wtr.Write(message.QuickBatch([][]byte{[]byte("foo")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.QuickBatch([][]byte{[]byte("bar\n")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.QuickBatch([][]byte{[]byte("baz")})); err != nil {
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

func TestUDPMultipart(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		if conn, err = net.ListenPacket("tcp6", "[::1]:0"); err != nil {
			t.Fatalf("failed to listen on a port: %v", err)
		}
	}
	defer conn.Close()

	conf := NewUDPConfig()
	conf.Address = conn.LocalAddr().String()

	wtr, err := NewUDP(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := wtr.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	if cerr := wtr.Connect(); cerr != nil {
		t.Fatal(cerr)
	}

	var buf bytes.Buffer

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		buf.ReadFrom(&wrapPacketConn{r: conn})
		wg.Done()
	}()

	if err = wtr.Write(message.QuickBatch([][]byte{[]byte("foo"), []byte("bar"), []byte("baz")})); err != nil {
		t.Error(err)
	}
	if err = wtr.Write(message.QuickBatch([][]byte{[]byte("qux")})); err != nil {
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
