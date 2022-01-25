package pipeline

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestPoolBasic(t *testing.T) {
	mockProc := &mockMsgProcessor{dropChan: make(chan bool)}

	go func() {
		mockProc.dropChan <- true
	}()

	constr := func(i *int) (types.Pipeline, error) {
		return NewProcessor(
			log.Noop(),
			metrics.Noop(),
			mockProc,
		), nil
	}

	proc, err := newTestPool(
		constr, 1,
		log.Noop(),
		metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err := proc.Consume(tChan); err != nil {
		t.Fatal(err)
	}
	if err := proc.Consume(tChan); err == nil {
		t.Error("Expected error from dupe receiving")
	}

	msg := message.New([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	// First message should be dropped and return immediately
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case _, open := <-proc.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		} else {
			t.Fatal("Message was not dropped")
		}
	case res, open := <-resChan:
		if !open {
			t.Fatal("Closed early")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Do not drop next message
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	var procT types.Transaction
	var open bool

	// Receive new message
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, message.GetAllBytes(procT.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Respond without error
	go func() {
		select {
		case procT.ResponseChan <- response.NewAck():
		case <-time.After(time.Second * 5):
			t.Error("Timed out")
		}
	}()

	// Receive response
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestPoolMultiMsgs(t *testing.T) {
	mockProc := &mockMultiMsgProcessor{N: 3}

	constr := func(i *int) (types.Pipeline, error) {
		return NewProcessor(
			log.Noop(),
			metrics.Noop(),
			mockProc,
		), nil
	}

	proc, err := newTestPool(
		constr, 1,
		log.Noop(),
		metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)
	if err := proc.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	for j := 0; j < 10; j++ {
		expMsgs := map[string]struct{}{}
		for i := 0; i < mockProc.N; i++ {
			expMsgs[fmt.Sprintf("test%v", i)] = struct{}{}
		}

		// Send message
		select {
		case tChan <- types.NewTransaction(message.New(nil), resChan):
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}

		for i := 0; i < mockProc.N; i++ {
			// Receive messages
			var procT types.Transaction
			var open bool
			select {
			case procT, open = <-proc.TransactionChan():
				if !open {
					t.Error("Closed early")
				}
				act := string(procT.Payload.Get(0).Get())
				if _, exists := expMsgs[act]; !exists {
					t.Errorf("Unexpected result: %v", act)
				} else {
					delete(expMsgs, act)
				}
			case <-time.After(time.Second * 5):
				t.Fatal("Timed out")
			}

			// Respond with no error
			select {
			case procT.ResponseChan <- response.NewAck():
			case <-time.After(time.Second * 5):
				t.Fatal("Timed out")
			}

		}

		// Receive response
		select {
		case res, open := <-resChan:
			if !open {
				t.Error("Closed early")
			} else if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}

		if len(expMsgs) != 0 {
			t.Errorf("Expected messages were not received: %v", expMsgs)
		}
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestPoolMultiThreads(t *testing.T) {
	conf := NewConfig()
	conf.Threads = 2
	conf.Processors = append(conf.Processors, processor.NewConfig())

	proc, err := New(
		conf, nil,
		log.Noop(),
		metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)
	if err := proc.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	for j := 0; j < conf.Threads; j++ {
		// Send message
		select {
		case tChan <- types.NewTransaction(msg, resChan):
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}
	}
	for j := 0; j < conf.Threads; j++ {
		// Receive messages
		var procT types.Transaction
		var open bool
		select {
		case procT, open = <-proc.TransactionChan():
			if !open {
				t.Error("Closed early")
			}
			if exp, act := [][]byte{[]byte("one"), []byte("two")}, message.GetAllBytes(procT.Payload); !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong message received: %s != %s", act, exp)
			}
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}

		go func(tran types.Transaction) {
			// Respond with no error
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second * 5):
				t.Error("Timed out")
			}
		}(procT)
	}
	for j := 0; j < conf.Threads; j++ {
		// Receive response
		select {
		case res, open := <-resChan:
			if !open {
				t.Error("Closed early")
			} else if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestPoolMultiNaturalClose(t *testing.T) {
	conf := NewConfig()
	conf.Threads = 2
	conf.Processors = append(conf.Processors, processor.NewConfig())

	proc, err := New(
		conf, nil,
		log.Noop(),
		metrics.Noop(),
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan types.Transaction)
	if err := proc.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	close(tChan)

	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
