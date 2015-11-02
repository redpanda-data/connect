/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package agent

import (
	"testing"
	"time"

	"github.com/jeffail/benthos/output"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

func TestBasicBufferedAgent(t *testing.T) {
	var incr, total uint8 = 100, 50

	out := output.MockType{
		ResChan: make(chan types.Response),
	}

	b := NewBuffered(&out, int(incr)*int(total))

	var i uint8

	// Check correct flow no blocking
	for ; i < total; i++ {
		msgBytes := make([][]byte, 1)
		msgBytes[0] = make([]byte, int(incr))
		msgBytes[0][0] = byte(i)

		select {
		// Send to agent
		case b.MessageChan() <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v send", i)
			return
		}

		// Instant response from agent
		select {
		case err := <-b.ResponseChan():
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v response", i)
			return
		}

		// Receive on output
		select {
		case outMsg := <-out.Messages:
			if actual := uint8(outMsg.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of unbuffered message receive: %v != %v", actual, i)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for unbuffered message %v read", i)
			return
		}

		// Response from output
		select {
		case out.ResChan <- types.Response(nil):
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
		case b.MessageChan() <- types.Message{Parts: msgBytes}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v send", i)
			return
		}
		select {
		case err := <-b.ResponseChan():
			if err != nil {
				t.Error(err)
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
	case b.MessageChan() <- types.Message{Parts: msgBytes}:
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for final buffered message send")
		return
	}

	// Response should block until buffer is relieved
	select {
	case err := <-b.ResponseChan():
		if err != nil {
			t.Error(err)
		} else {
			t.Errorf("Overflowed response returned before timeout")
		}
		return
	case <-time.After(100 * time.Millisecond):
	}

	// Extract last message
	select {
	case <-out.Messages:
		out.ResChan <- types.Response(nil)
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for final buffered message read")
		return
	}

	// Response from the last attempt should no longer be blocking
	select {
	case err := <-b.ResponseChan():
		if err != nil {
			t.Error(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Final buffered response blocked")
	}

	// Extract all other messages
	for i = 1; i < total; i++ {
		select {
		case outMsg := <-out.Messages:
			if actual := uint8(outMsg.Parts[0][0]); actual != i {
				t.Errorf("Wrong order receipt of buffered message: %v != %v", actual, i)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered message %v read", i)
			return
		}

		select {
		case out.ResChan <- types.Response(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for buffered response send back %v", i)
			return
		}
	}

	// Get final message
	select {
	case <-out.Messages:
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for buffered message %v read", i)
		return
	}

	select {
	case out.ResChan <- types.Response(nil):
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for buffered response send back %v", i)
		return
	}
}

//--------------------------------------------------------------------------------------------------
