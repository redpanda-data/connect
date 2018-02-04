// Copyright (c) 2018 Ashley Jeffs
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

package output

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

type writerCantConnect struct{}

func (w writerCantConnect) Connect() error { return writer.ErrNotConnected }
func (w writerCantConnect) Write(msg types.Message) error {
	return writer.ErrNotConnected
}
func (w writerCantConnect) CloseAsync() {}
func (w writerCantConnect) WaitForClose(time.Duration) error {
	return nil
}

func TestWriterCantConnect(t *testing.T) {
	w, err := NewWriter(
		"foo", writerCantConnect{},
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	if err = w.StartReceiving(make(chan types.Message)); err != nil {
		t.Error(err)
	}
	if err = w.StartReceiving(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	// We will fail to connect but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

type writerCantSend struct {
	connected int
}

func (w *writerCantSend) Connect() error {
	w.connected++
	return nil
}
func (w *writerCantSend) Write(msg types.Message) error {
	return writer.ErrNotConnected
}
func (w *writerCantSend) CloseAsync() {}
func (w *writerCantSend) WaitForClose(time.Duration) error {
	return nil
}

func TestWriterCantSend(t *testing.T) {
	writerImpl := &writerCantSend{}

	w, err := NewWriter(
		"foo", writerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Message)

	if err = w.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err = w.StartReceiving(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		res, open := <-w.ResponseChan()
		if open {
			if act, exp := res.Error(), writer.ErrNotConnected; exp != act {
				t.Errorf("Received unexpected response: %v != %v", act, exp)
			}
		}
		wg.Done()
	}()

	select {
	case msgChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	wg.Wait()

	if writerImpl.connected <= 1 {
		t.Errorf("Connected wasn't called enough times: %v", writerImpl.connected)
	}
}

func TestWriterCantSendClosed(t *testing.T) {
	writerImpl := &writerCantSend{}

	w, err := NewWriter(
		"foo", writerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Message)

	if err = w.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}

	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestWriterCantSendClosedChan(t *testing.T) {
	writerImpl := &writerCantSend{}

	w, err := NewWriter(
		"foo", writerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Message)

	if err = w.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}

	close(msgChan)
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

type writerCanReconnect struct {
	failSendFirst bool
	connSuccesses int

	connected int
}

func (w *writerCanReconnect) Connect() error {
	w.connected++
	if w.connSuccesses <= 0 {
		return writer.ErrNotConnected
	}
	w.connSuccesses--
	return nil
}
func (w *writerCanReconnect) Write(msg types.Message) error {
	if !w.failSendFirst {
		w.failSendFirst = true
		return writer.ErrNotConnected
	}
	return nil
}
func (w *writerCanReconnect) CloseAsync() {}
func (w *writerCanReconnect) WaitForClose(time.Duration) error {
	return nil
}

func TestWriterCanReconnect(t *testing.T) {
	writerImpl := &writerCanReconnect{connSuccesses: 100}

	w, err := NewWriter(
		"foo", writerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Message)

	if err = w.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err = w.StartReceiving(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		res, open := <-w.ResponseChan()
		if open {
			if act, exp := res.Error(), error(nil); exp != act {
				t.Errorf("unexpected error: %v != %v", act, exp)
			}
		}
		wg.Done()
	}()

	select {
	case msgChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	wg.Wait()

	if writerImpl.connected <= 1 {
		t.Errorf("Connected wasn't called enough times: %v", writerImpl.connected)
	}
}

func TestWriterCantReconnect(t *testing.T) {
	writerImpl := &writerCanReconnect{connSuccesses: 1}

	w, err := NewWriter(
		"foo", writerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	msgChan := make(chan types.Message)

	if err = w.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err = w.StartReceiving(nil); err == nil {
		t.Error("Expected error from duplicate receiver call")
	}

	select {
	case msgChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	w.CloseAsync()
	if err = w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if writerImpl.connected <= 1 {
		t.Errorf("Connected wasn't called enough times: %v", writerImpl.connected)
	}
}

//------------------------------------------------------------------------------
