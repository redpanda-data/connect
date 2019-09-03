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

package processor

import (
	"errors"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/response"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type passthrough struct {
	called int
}

func (p *passthrough) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.called++
	return []types.Message{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *passthrough) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *passthrough) WaitForClose(timeout time.Duration) error {
	return nil
}

func TestExecuteAllBasic(t *testing.T) {
	procs := []types.Processor{
		&passthrough{},
		&passthrough{},
	}

	msg := message.New([][]byte{[]byte("test message")})
	msgs, res := ExecuteAll(procs, msg)
	if res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := 1, len(msgs); exp != act {
		t.Fatalf("Wrong count of messages: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "test message", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	for _, proc := range procs {
		if exp, act := 1, proc.(*passthrough).called; exp != act {
			t.Errorf("Wrong call count from processor: %v != %v", act, exp)
		}
	}
}

func TestExecuteAllBasicBatch(t *testing.T) {
	procs := []types.Processor{
		&passthrough{},
		&passthrough{},
	}

	msg := message.New([][]byte{
		[]byte("test message 1"),
		[]byte("test message 2"),
		[]byte("test message 3"),
	})
	msgs, res := ExecuteAll(procs, msg)
	if res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := 1, len(msgs); exp != act {
		t.Fatalf("Wrong count of messages: %v != %v", act, exp)
	}
	if exp, act := 3, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "test message 1", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	for _, proc := range procs {
		if exp, act := 1, proc.(*passthrough).called; exp != act {
			t.Errorf("Wrong call count from processor: %v != %v", act, exp)
		}
	}
}

func TestExecuteAllMulti(t *testing.T) {
	procs := []types.Processor{
		&passthrough{},
		&passthrough{},
	}

	msg1 := message.New([][]byte{[]byte("test message 1")})
	msg2 := message.New([][]byte{[]byte("test message 2")})
	msgs, res := ExecuteAll(procs, msg1, msg2)
	if res != nil {
		t.Fatal(res.Error())
	}
	if exp, act := 2, len(msgs); exp != act {
		t.Fatalf("Wrong count of messages: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[1].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "test message 1", string(msgs[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "test message 2", string(msgs[1].Get(0).Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	for _, proc := range procs {
		if exp, act := 2, proc.(*passthrough).called; exp != act {
			t.Errorf("Wrong call count from processor: %v != %v", act, exp)
		}
	}
}

type dropped struct {
	called int
}

func (p *dropped) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.called++
	return nil, response.NewUnack()
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *dropped) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *dropped) WaitForClose(timeout time.Duration) error {
	return nil
}

func TestExecuteAllBuffered(t *testing.T) {
	procs := []types.Processor{
		&passthrough{},
		&dropped{},
		&passthrough{},
	}

	msg1 := message.New([][]byte{[]byte("test message 1")})
	msg2 := message.New([][]byte{[]byte("test message 2")})
	msgs, res := ExecuteAll(procs, msg1, msg2)
	if len(msgs) > 0 {
		t.Fatal("received messages after drop")
	}
	if res == nil || !res.SkipAck() {
		t.Fatal("received non unack response")
	}
	if exp, act := 2, procs[0].(*passthrough).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
	if exp, act := 2, procs[1].(*dropped).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
	if exp, act := 0, procs[2].(*passthrough).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
}

type errored struct {
	called int
}

func (p *errored) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.called++
	return nil, response.NewError(errors.New("test error"))
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *errored) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *errored) WaitForClose(timeout time.Duration) error {
	return nil
}

func TestExecuteAllErrored(t *testing.T) {
	procs := []types.Processor{
		&passthrough{},
		&errored{},
		&passthrough{},
	}

	msg1 := message.New([][]byte{[]byte("test message 1")})
	msg2 := message.New([][]byte{[]byte("test message 2")})
	msgs, res := ExecuteAll(procs, msg1, msg2)
	if len(msgs) > 0 {
		t.Fatal("received messages after drop")
	}
	if res == nil || res.Error() == nil {
		t.Fatal("received non noack response")
	}
	if exp, act := 2, procs[0].(*passthrough).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
	if exp, act := 1, procs[1].(*errored).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
	if exp, act := 0, procs[2].(*passthrough).called; exp != act {
		t.Errorf("Wrong call count from processor: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
