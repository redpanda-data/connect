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
