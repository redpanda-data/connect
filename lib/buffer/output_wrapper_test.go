// Copyright (c) 2014 Ashley Jeffs
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

package buffer

import (
	"errors"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/buffer/impl"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func TestBasicMemoryBuffer(t *testing.T) {
	var incr, total uint8 = 100, 50

	msgChan := make(chan types.Message)
	resChan := make(chan types.Response)

	b := NewOutputWrapper(impl.NewMemory(impl.MemoryConfig{
		Limit: int(incr+15) * int(total),
	}), metrics.DudType{})
	if err := b.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}
	if err := b.StartReceiving(msgChan); err != nil {
		t.Error(err)
		return
	}

	var i uint8

	// Check correct flow no blocking
	for ; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = byte(i)

		select {
		// Send to buffer
		case msgChan <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v send", i)
			return
		}

		// Instant response from buffer
		select {
		case res := <-b.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v response", i)
			return
		}

		// Receive on output
		select {
		case outMsg := <-b.MessageChan():
			if actual := uint8(outMsg.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of unbuffered message receive: %v != %v", actual, i)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v read", i)
			return
		}

		// Response from output
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered response send back %v", i)
			return
		}
	}

	for i = 0; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = byte(i)

		select {
		case msgChan <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v send", i)
			return
		}
		select {
		case res := <-b.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v response", i)
			return
		}
	}

	// Should have reached limit here
	msgBytes := make([][]byte, 1)
	msgBytes[0] = make([]byte, int(incr))

	select {
	case msgChan <- types.Message{Parts: msgBytes}:
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for final buffered message send")
		return
	}

	// Response should block until buffer is relieved
	select {
	case res := <-b.ResponseChan():
		if res.Error() != nil {
			t.Error(res.Error())
		} else {
			t.Errorf("Overflowed response returned before timeout")
		}
		return
	case <-time.After(100 * time.Millisecond):
	}

	// Extract last message
	select {
	case val := <-b.MessageChan():
		if actual := uint8(val.Parts[0][0]); actual != 0 {
			t.Errorf("Wrong order receipt of buffered message receive: %v != %v", actual, 0)
		}
		resChan <- types.NewSimpleResponse(nil)
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for final buffered message read")
		return
	}

	// Response from the last attempt should no longer be blocking
	select {
	case res := <-b.ResponseChan():
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Final buffered response blocked")
	}

	// Extract all other messages
	for i = 1; i < total; i++ {
		select {
		case outMsg := <-b.MessageChan():
			if actual := uint8(outMsg.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of buffered message: %v != %v", actual, i)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v read", i)
			return
		}

		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered response send back %v", i)
			return
		}
	}

	// Get final message
	select {
	case <-b.MessageChan():
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for buffered message %v read", i)
		return
	}

	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for buffered response send back %v", i)
		return
	}

	b.CloseAsync()
	b.WaitForClose(time.Second)

	close(resChan)
	close(msgChan)
}

func TestBufferClosing(t *testing.T) {
	var incr, total uint8 = 100, 5

	msgChan := make(chan types.Message)
	resChan := make(chan types.Response)

	b := NewOutputWrapper(impl.NewMemory(impl.MemoryConfig{
		Limit: int(incr+15) * int(total),
	}), metrics.DudType{})
	if err := b.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}
	if err := b.StartReceiving(msgChan); err != nil {
		t.Error(err)
		return
	}

	var i uint8

	// Populate buffer with some messages
	for i = 0; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = byte(i)

		select {
		case msgChan <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v send", i)
			return
		}
		select {
		case res := <-b.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v response", i)
			return
		}
	}

	// Close input, this should prompt the stack buffer to CloseOnceEmpty().
	close(msgChan)

	// Receive all of those messages from the buffer
	for i = 0; i < total; i++ {
		select {
		case val := <-b.MessageChan():
			if actual := uint8(val.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of buffered message receive: %v != %v", actual, i)
			}
			resChan <- types.NewSimpleResponse(nil)
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for final buffered message read")
			return
		}
	}

	// The buffer should now be closed, therefore so should our read channel.
	select {
	case _, open := <-b.MessageChan():
		if open {
			t.Error("Reader channel still open after clearing buffer")
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for final buffered message read")
		return
	}

	// Should already be shut down.
	b.WaitForClose(time.Second)

	close(resChan)
}

func TestOutputWrapperErrProp(t *testing.T) {
	msgChan := make(chan types.Message)
	resChan := make(chan types.Response)

	b := NewOutputWrapper(impl.NewMemory(impl.NewMemoryConfig()), metrics.DudType{})
	if err := b.StartReceiving(msgChan); err != nil {
		t.Error(err)
		return
	}
	if err := b.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	msg := types.NewMessage()
	msg.Parts = append(msg.Parts, []byte(`hello world`))

	select {
	case msgChan <- msg:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}
	select {
	case res, open := <-b.ResponseChan():
		if !open {
			t.Error("buffer closed early")
			return
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for result")
	}

	select {
	case _, open := <-b.MessageChan():
		if !open {
			t.Error("buffer closed early")
			return
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}

	errTest := errors.New("test error")
	go func() {
		select {
		case resChan <- types.NewSimpleResponse(errTest):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for error response")
		}
	}()

	select {
	case errs, open := <-b.ErrorsChan():
		if !open {
			t.Error("buffer closed early")
			return
		}
		if exp, act := 1, len(errs); exp != act {
			t.Errorf("Wrong # of errors returned: %v != %v", exp, act)
		}
		if exp, act := errTest, errs[0]; exp != act {
			t.Errorf("Wrong error returned: %v != %v", exp, act)
		}
	case <-time.After(time.Second * 5):
		t.Error("Timed out waiting for errors returned")
	}

	select {
	case _, open := <-b.MessageChan():
		if !open {
			t.Error("buffer closed early")
			return
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for error response")
	}

	close(resChan)
	close(msgChan)

	b.CloseAsync()
	if err := b.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

/*
func TestSyncBuffer(t *testing.T) {
	msgChan := make(chan types.Message)
	resChan := make(chan types.Response)

	b := NewOutputWrapper(1)
	if err := b.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}
	if err := b.StartReceiving(msgChan); err != nil {
		t.Error(err)
		return
	}

	checkNoWrite := func() error {
		select {
		case msgChan <- types.Message{Parts: [][]byte{}}:
			return errors.New("Message sent without response")
		case <-time.After(time.Millisecond * 100):
		}
		return nil
	}

	checkNoRead := func() error {
		select {
		case <-b.ResponseChan():
			return errors.New("Response received without full read")
		case <-time.After(time.Millisecond * 100):
		}
		return nil
	}

	// Send a message
	select {
	case msgChan <- types.Message{Parts: [][]byte{[]byte("test")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for first message send")
		return
	}

	// Ensure no response back
	if err := checkNoRead(); err != nil {
		t.Error(err)
	}
	// Check that we cant send another
	if err := checkNoWrite(); err != nil {
		t.Error(err)
	}

	// Receive on output
	select {
	case <-b.MessageChan():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for first message read")
		return
	}

	// Ensure no response back
	if err := checkNoRead(); err != nil {
		t.Error(err)
	}
	// Check that we cant send another
	if err := checkNoWrite(); err != nil {
		t.Error(err)
	}

	// Response from output
	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for first response send back")
		return
	}

	// Response from buffer
	select {
	case <-b.ResponseChan():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message response")
		return
	}

	// We should now be able to send a new message
	select {
	case msgChan <- types.Message{Parts: [][]byte{[]byte("test")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for second message send")
		return
	}

	b.CloseAsync()
	if err := b.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
*/

/*
func TestEmptyMemoryBuffer(t *testing.T) {
	var incr, total uint8 = 100, 50

	msgChan := make(chan types.Message)
	resChan := make(chan types.Response)

	b := NewOutputWrapper(1)
	if err := b.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}
	if err := b.StartReceiving(msgChan); err != nil {
		t.Error(err)
		return
	}

	var i uint8

	// Check correct flow no blocking
	for ; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = byte(i)

		select {
		// Send to buffer
		case msgChan <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v send", i)
			return
		}

		// Receive on output
		select {
		case outMsg := <-b.MessageChan():
			if actual := uint8(outMsg.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of unbuffered message receive: %v != %v", actual, i)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v read", i)
			return
		}

		// Response from output
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered response send back %v", i)
			return
		}

		// Response from buffer
		select {
		case res := <-b.ResponseChan():
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v response", i)
			return
		}
	}

	b.CloseAsync()
	b.WaitForClose(time.Second)

	close(resChan)
	close(msgChan)
}
*/

//------------------------------------------------------------------------------
