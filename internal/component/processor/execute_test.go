package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/message"
)

type passthrough struct {
	called int
}

func (p *passthrough) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	p.called++
	return []message.Batch{msg}, nil
}

func (p *passthrough) Close(ctx context.Context) error {
	return nil
}

func TestExecuteAllBasic(t *testing.T) {
	procs := []V1{
		&passthrough{},
		&passthrough{},
	}

	tCtx := context.Background()

	msg := message.QuickBatch([][]byte{[]byte("test message")})
	msgs, res := ExecuteAll(tCtx, procs, msg)
	if res != nil {
		t.Fatal(res)
	}
	if exp, act := 1, len(msgs); exp != act {
		t.Fatalf("Wrong count of messages: %v != %v", act, exp)
	}
	if exp, act := 1, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "test message", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	for _, proc := range procs {
		if exp, act := 1, proc.(*passthrough).called; exp != act {
			t.Errorf("Wrong call count from processor: %v != %v", act, exp)
		}
	}
}

func TestExecuteAllBasicBatch(t *testing.T) {
	tCtx := context.Background()

	procs := []V1{
		&passthrough{},
		&passthrough{},
	}

	msg := message.QuickBatch([][]byte{
		[]byte("test message 1"),
		[]byte("test message 2"),
		[]byte("test message 3"),
	})
	msgs, res := ExecuteAll(tCtx, procs, msg)
	if res != nil {
		t.Fatal(res)
	}
	if exp, act := 1, len(msgs); exp != act {
		t.Fatalf("Wrong count of messages: %v != %v", act, exp)
	}
	if exp, act := 3, msgs[0].Len(); exp != act {
		t.Fatalf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "test message 1", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	for _, proc := range procs {
		if exp, act := 1, proc.(*passthrough).called; exp != act {
			t.Errorf("Wrong call count from processor: %v != %v", act, exp)
		}
	}
}

func TestExecuteAllMulti(t *testing.T) {
	tCtx := context.Background()

	procs := []V1{
		&passthrough{},
		&passthrough{},
	}

	msg1 := message.QuickBatch([][]byte{[]byte("test message 1")})
	msg2 := message.QuickBatch([][]byte{[]byte("test message 2")})
	msgs, res := ExecuteAll(tCtx, procs, msg1, msg2)
	if res != nil {
		t.Fatal(res)
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
	if exp, act := "test message 1", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "test message 2", string(msgs[1].Get(0).AsBytes()); exp != act {
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

func (p *errored) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	p.called++
	return nil, errors.New("test error")
}

func (p *errored) Close(ctx context.Context) error {
	return nil
}

func TestExecuteAllErrored(t *testing.T) {
	tCtx := context.Background()

	procs := []V1{
		&passthrough{},
		&errored{},
		&passthrough{},
	}

	msg1 := message.QuickBatch([][]byte{[]byte("test message 1")})
	msg2 := message.QuickBatch([][]byte{[]byte("test message 2")})
	msgs, res := ExecuteAll(tCtx, procs, msg1, msg2)
	if len(msgs) > 0 {
		t.Fatal("received messages after drop")
	}
	if res == nil {
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
