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

package input

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

type mockReader struct {
	msgToSnd types.Message
	ackRcvd  error

	connChan chan error
	readChan chan error
	ackChan  chan error
}

func newMockReader() *mockReader {
	return &mockReader{
		connChan: make(chan error),
		readChan: make(chan error),
		ackChan:  make(chan error),
	}
}

func (r *mockReader) Connect() error {
	return <-r.connChan
}
func (r *mockReader) Read() (types.Message, error) {
	if err := <-r.readChan; err != nil {
		return types.Message{}, err
	}
	return r.msgToSnd, nil
}
func (r *mockReader) Acknowledge(err error) error {
	r.ackRcvd = err
	return <-r.ackChan
}
func (r *mockReader) CloseAsync() {}
func (r *mockReader) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type readerCantConnect struct{}

func (r readerCantConnect) Connect() error { return types.ErrNotConnected }
func (r readerCantConnect) Read() (types.Message, error) {
	return types.Message{}, types.ErrNotConnected
}
func (r readerCantConnect) Acknowledge(err error) error {
	return types.ErrNotConnected
}
func (r readerCantConnect) CloseAsync() {}
func (r readerCantConnect) WaitForClose(time.Duration) error {
	return nil
}

func TestReaderCantConnect(t *testing.T) {
	t.Parallel()

	r, err := NewReader(
		"foo", readerCantConnect{},
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	if err = r.StartListening(make(chan types.Response)); err != nil {
		t.Error(err)
	}
	if err = r.StartListening(nil); err == nil {
		t.Error("Expected error from duplicate listen call")
	}

	// We will fail to connect but should still exit immediately.
	r.CloseAsync()
	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

type readerCantRead struct {
	connected int
}

func (r *readerCantRead) Connect() error {
	r.connected++
	return nil
}
func (r *readerCantRead) Read() (types.Message, error) {
	return types.Message{}, types.ErrNotConnected
}
func (r *readerCantRead) Acknowledge(err error) error {
	return types.ErrNotConnected
}
func (r *readerCantRead) CloseAsync() {}
func (r *readerCantRead) WaitForClose(time.Duration) error {
	return nil
}

func TestReaderCantRead(t *testing.T) {
	t.Parallel()

	readerImpl := &readerCantRead{}

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()
	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if readerImpl.connected < 1 {
		t.Errorf("Connected wasn't called enough times: %v", readerImpl.connected)
	}
}

//------------------------------------------------------------------------------

func TestReaderTypeClosedOnConn(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderTypeClosedOnReconn(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.connChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderTypeClosedOnReread(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- types.ErrTypeClosed:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------

func TestReaderCanReconnect(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	select {
	case _, open := <-r.MessageChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderFailsReconnect(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second * 2):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	select {
	case _, open := <-r.MessageChan():
		if !open {
			t.Fatal("Closed early")
		}
	case <-time.After(time.Second * 2):
		t.Error("Timed out")
	}

	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
		}
	}()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderCloseDuringReconnect(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case readerImpl.readChan <- types.ErrNotConnected:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.connChan <- types.ErrNotConnected:
		case <-time.After(time.Second):
		}
	}()

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestReaderHappyPath(t *testing.T) {
	t.Parallel()

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	readerImpl := newMockReader()
	readerImpl.msgToSnd = types.Message{
		Parts: exp,
	}
	readerImpl.ackRcvd = errors.New("ack not received")

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	select {
	case msg, open := <-r.MessageChan():
		if !open {
			t.Fatal("Chan closed")
		}
		if act := msg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if readerImpl.ackRcvd != nil {
		t.Error(readerImpl.ackRcvd)
	}
}

func TestReaderSadPath(t *testing.T) {
	t.Parallel()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("test error")

	readerImpl := newMockReader()
	readerImpl.msgToSnd = types.Message{
		Parts: exp,
	}
	readerImpl.ackRcvd = errors.New("ack not received")

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	go func() {
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}()

	select {
	case msg, open := <-r.MessageChan():
		if !open {
			t.Fatal("Chan closed")
		}
		if act := msg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	select {
	case resChan <- types.NewSimpleResponse(expErr):
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if actErr := readerImpl.ackRcvd; expErr != actErr {
		t.Errorf("Wrong response received: %v != %v", actErr, expErr)
	}
}

func TestReaderSkipAcks(t *testing.T) {
	t.Parallel()

	exp := [][]byte{[]byte("foo"), []byte("bar")}
	expErr := errors.New("ack not received")

	readerImpl := newMockReader()
	readerImpl.msgToSnd = types.Message{
		Parts: exp,
	}
	readerImpl.ackRcvd = expErr

	r, err := NewReader(
		"foo", readerImpl,
		log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}

	resChan := make(chan types.Response)

	if err = r.StartListening(resChan); err != nil {
		t.Error(err)
	}

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	for i := 0; i < 3; i++ {
		go func() {
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Fatal("Timed out")
			}
		}()

		select {
		case msg, open := <-r.MessageChan():
			if !open {
				t.Fatal("Chan closed")
			}
			if act := msg.Parts; !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong message returned: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Fatalf("Timed out at attempt: %v", i)
		}

		select {
		case resChan <- types.NewUnacknowledgedResponse():
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}

	// We will be failing to send but should still exit immediately.
	r.CloseAsync()

	if err = r.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if actErr := readerImpl.ackRcvd; expErr != actErr {
		t.Errorf("Wrong response received: %v != %v", actErr, expErr)
	}
}

//------------------------------------------------------------------------------
