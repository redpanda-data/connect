// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

type wrapPacketConn struct {
	r net.PacketConn
}

func (w *wrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.r.ReadFrom(p)
	return
}

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

	wtr, err := NewUDP(conf, nil, log.Noop(), metrics.Noop())
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

	wtr, err := NewUDP(conf, nil, log.Noop(), metrics.Noop())
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
