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

package reader

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockReader struct {
	msgToSnd types.Message
	ackRcvd  error

	connChan         chan error
	readChan         chan error
	ackChan          chan error
	closeAsyncChan   chan struct{}
	waitForCloseChan chan error
}

func newMockReader() *mockReader {
	return &mockReader{
		connChan:         make(chan error),
		readChan:         make(chan error),
		ackChan:          make(chan error),
		closeAsyncChan:   make(chan struct{}),
		waitForCloseChan: make(chan error),
	}
}

func (r *mockReader) Connect() error {
	return <-r.connChan
}
func (r *mockReader) Read() (types.Message, error) {
	if err := <-r.readChan; err != nil {
		return nil, err
	}
	return r.msgToSnd, nil
}
func (r *mockReader) Acknowledge(err error) error {
	r.ackRcvd = err
	return <-r.ackChan
}
func (r *mockReader) CloseAsync() {
	<-r.closeAsyncChan
}
func (r *mockReader) WaitForClose(time.Duration) error {
	return <-r.waitForCloseChan
}

//------------------------------------------------------------------------------

func TestPreserverClose(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()
	pres := NewPreserver(readerImpl)

	exp := errors.New("foo error")

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		if err := pres.Connect(); err != nil {
			t.Error(err)
		}
		pres.CloseAsync()
		if act := pres.WaitForClose(time.Second); act != exp {
			t.Errorf("Wrong error returned: %v != %v", act, exp)
		}
		wg.Done()
	}()

	select {
	case readerImpl.connChan <- nil:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.closeAsyncChan <- struct{}{}:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	select {
	case readerImpl.waitForCloseChan <- exp:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	wg.Wait()
}

func TestPreserverHappy(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()
	pres := NewPreserver(readerImpl)

	expParts := [][]byte{
		[]byte("foo"),
	}

	go func() {
		select {
		case readerImpl.connChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		for _, p := range expParts {
			readerImpl.msgToSnd = message.New([][]byte{p})
			select {
			case readerImpl.readChan <- nil:
			case <-time.After(time.Second):
				t.Error("Timed out")
			}
		}
	}()

	if err := pres.Connect(); err != nil {
		t.Error(err)
	}

	for _, exp := range expParts {
		msg, err := pres.Read()
		if err != nil {
			t.Fatal(err)
		}
		if act := msg.Get(0).Get(); !reflect.DeepEqual(act, exp) {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}
}

func TestPreserverErrorProp(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()
	pres := NewPreserver(readerImpl)

	expErr := errors.New("foo")

	go func() {
		select {
		case readerImpl.connChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
		select {
		case readerImpl.ackChan <- expErr:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}()

	if actErr := pres.Connect(); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
	if _, actErr := pres.Read(); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
	if _, actErr := pres.Read(); actErr != nil {
		t.Fatal(actErr)
	}
	if actErr := pres.Acknowledge(nil); expErr != actErr {
		t.Errorf("Wrong error returned: %v != %v", actErr, expErr)
	}
}

//------------------------------------------------------------------------------

func TestPreserverBuffer(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()
	pres := NewPreserver(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgToSnd = message.New(
			[][]byte{[]byte(content)},
		)
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}
	sendAck := func() {
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	// Send message normally.
	exp := "msg 1"
	exp2 := "msg 2"
	exp3 := "msg 3"

	go sendMsg(exp)
	msg, err := pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}

	// Prime second message.
	go sendMsg(exp2)

	// Fail previous message, expecting it to be resent.
	pres.Acknowledge(errors.New("failed"))
	msg, err = pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}

	// Read the primed message.
	msg, err = pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp2 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp2)
	}

	// Fail both messages, expecting them to be resent.
	pres.Acknowledge(errors.New("failed again"))

	// Read both messages.
	msg, err = pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp)
	}
	msg, err = pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp2 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp2)
	}

	// Prime a new message and also an acknowledgement.
	go sendMsg(exp3)
	go sendAck()

	// Ack all messages.
	pres.Acknowledge(nil)

	msg, err = pres.Read()
	if err != nil {
		t.Fatal(err)
	}
	if act := string(msg.Get(0).Get()); exp3 != act {
		t.Errorf("Wrong message returned: %v != %v", act, exp3)
	}
}

func TestPreserverBufferBatchedAcks(t *testing.T) {
	t.Parallel()

	readerImpl := newMockReader()
	pres := NewPreserver(readerImpl)

	sendMsg := func(content string) {
		readerImpl.msgToSnd = message.New(
			[][]byte{[]byte(content)},
		)
		select {
		case readerImpl.readChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}
	sendAck := func() {
		select {
		case readerImpl.ackChan <- nil:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	messages := []string{
		"msg 1",
		"msg 2",
		"msg 3",
	}

	for _, exp := range messages {
		go sendMsg(exp)
		msg, err := pres.Read()
		if err != nil {
			t.Fatal(err)
		}
		if act := string(msg.Get(0).Get()); exp != act {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}

	// Fail all messages, expecting them to be resent.
	pres.Acknowledge(errors.New("failed again"))

	for _, exp := range messages {
		// If we ack all messages now, this shouldnt be propagated to underlying
		// until the resends are completed.
		pres.Acknowledge(nil)

		msg, err := pres.Read()
		if err != nil {
			t.Fatal(err)
		}
		if act := string(msg.Get(0).Get()); exp != act {
			t.Errorf("Wrong message returned: %v != %v", act, exp)
		}
	}

	// Ack all messages.
	go pres.Acknowledge(nil)

	sendAck()
}

//------------------------------------------------------------------------------
