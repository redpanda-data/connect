package reader

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type mockSyncReader struct {
	msgsToSnd []types.Message
	ackRcvd   error

	connChan         chan error
	readChan         chan error
	ackChan          chan error
	closeAsyncChan   chan struct{}
	waitForCloseChan chan error
}

func newMockSyncReader() *mockSyncReader {
	return &mockSyncReader{
		connChan:         make(chan error),
		readChan:         make(chan error),
		ackChan:          make(chan error),
		closeAsyncChan:   make(chan struct{}),
		waitForCloseChan: make(chan error),
	}
}

func (r *mockSyncReader) ConnectWithContext(ctx context.Context) error {
	cerr, open := <-r.connChan
	if !open {
		return types.ErrNotConnected
	}
	return cerr
}

func (r *mockSyncReader) ReadNextWithContext(ctx context.Context) (types.Message, error) {
	select {
	case <-ctx.Done():
		return nil, types.ErrTimeout
	case err, open := <-r.readChan:
		if !open {
			return nil, types.ErrNotConnected
		}
		if err != nil {
			return nil, err
		}
	}

	var nextMsg types.Message = message.New(nil)
	if len(r.msgsToSnd) > 0 {
		nextMsg = r.msgsToSnd[0]
		r.msgsToSnd = r.msgsToSnd[1:]
	}

	return nextMsg.DeepCopy(), nil
}

func (r *mockSyncReader) AcknowledgeWithContext(ctx context.Context, err error) error {
	r.ackRcvd = err
	return <-r.ackChan
}

func (r *mockSyncReader) CloseAsync() {
	<-r.closeAsyncChan
}

func (r *mockSyncReader) WaitForClose(time.Duration) error {
	return <-r.waitForCloseChan
}

//------------------------------------------------------------------------------

func TestSyncBatcherHappy(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	testMsgs := []string{}
	for i := 0; i < 10; i++ {
		testMsgs = append(testMsgs, fmt.Sprintf("test %v", i))
	}
	rdr := newMockSyncReader()
	for _, str := range testMsgs {
		rdr.msgsToSnd = append(rdr.msgsToSnd, message.New([][]byte{[]byte(str)}))
	}

	conf := batch.NewPolicyConfig()
	conf.Count = 5
	batcher, err := NewSyncBatcher(conf, rdr, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		batcher.CloseAsync()
		deadline, _ := ctx.Deadline()
		if err = batcher.WaitForClose(time.Until(deadline)); err != nil {
			t.Error(err)
		}
	}()

	lastErr := errors.New("test error")
	go func() {
		rdr.connChan <- nil
		for i := 0; i < 5; i++ {
			rdr.readChan <- nil
		}
		rdr.ackChan <- nil
		for i := 0; i < 5; i++ {
			rdr.readChan <- nil
		}
		rdr.ackChan <- lastErr
		rdr.closeAsyncChan <- struct{}{}
		rdr.waitForCloseChan <- nil
	}()

	if err = batcher.ConnectWithContext(ctx); err != nil {
		t.Fatal(err)
	}

	msg, err := batcher.ReadNextWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 5 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != nil {
		t.Error(err)
	}

	if msg, err = batcher.ReadNextWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 5 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i+5), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != lastErr {
		t.Errorf("Expected '%v', received: %v", lastErr, err)
	}
}

func TestSyncBatcherSadThenHappy(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	testMsgs := []string{}
	for i := 0; i < 10; i++ {
		testMsgs = append(testMsgs, fmt.Sprintf("test %v", i))
	}
	rdr := newMockSyncReader()
	for _, str := range testMsgs {
		rdr.msgsToSnd = append(rdr.msgsToSnd, message.New([][]byte{[]byte(str)}))
	}

	conf := batch.NewPolicyConfig()
	conf.Count = 5
	batcher, err := NewSyncBatcher(conf, rdr, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		batcher.CloseAsync()
		deadline, _ := ctx.Deadline()
		if err = batcher.WaitForClose(time.Until(deadline)); err != nil {
			t.Error(err)
		}
	}()

	firstReadErr := errors.New("reading failed 1")
	secondReadErr := errors.New("reading failed 2")
	go func() {
		rdr.connChan <- nil
		rdr.readChan <- firstReadErr
		for i := 0; i < 5; i++ {
			rdr.readChan <- nil
		}
		rdr.ackChan <- nil
		for i := 0; i < 2; i++ {
			rdr.readChan <- nil
		}
		rdr.readChan <- secondReadErr
		for i := 0; i < 3; i++ {
			rdr.readChan <- nil
		}
		rdr.ackChan <- nil
		rdr.closeAsyncChan <- struct{}{}
		rdr.waitForCloseChan <- nil
	}()

	if err = batcher.ConnectWithContext(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err = batcher.ReadNextWithContext(ctx); err != firstReadErr {
		t.Fatalf("Expected '%v', received: %v", firstReadErr, err)
	}

	msg, err := batcher.ReadNextWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 5 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != nil {
		t.Error(err)
	}

	if _, err = batcher.ReadNextWithContext(ctx); err != secondReadErr {
		t.Fatalf("Expected '%v', received: %v", secondReadErr, err)
	}

	if msg, err = batcher.ReadNextWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 5 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i+5), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != nil {
		t.Error(err)
	}
}

func TestSyncBatcherTimeout(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond)
	defer done()

	rdr := newMockSyncReader()

	conf := batch.NewPolicyConfig()
	conf.Count = 5
	batcher, err := NewSyncBatcher(conf, rdr, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		batcher.CloseAsync()
		if err = batcher.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		rdr.connChan <- nil
		rdr.readChan <- types.ErrTimeout
		rdr.closeAsyncChan <- struct{}{}
		close(rdr.waitForCloseChan)
	}()

	if err = batcher.ConnectWithContext(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err = batcher.ReadNextWithContext(ctx); err != types.ErrTimeout {
		t.Fatalf("Expected '%v', received: %v", types.ErrTimeout, err)
	}
}

func TestSyncBatcherTimedBatches(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	testMsgs := []string{}
	for i := 0; i < 10; i++ {
		testMsgs = append(testMsgs, fmt.Sprintf("test %v", i))
	}
	rdr := newMockSyncReader()
	for _, str := range testMsgs {
		rdr.msgsToSnd = append(rdr.msgsToSnd, message.New([][]byte{[]byte(str)}))
	}

	conf := batch.NewPolicyConfig()
	conf.Count = 8
	conf.Period = "500ms"
	batcher, err := NewSyncBatcher(conf, rdr, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		batcher.CloseAsync()
		deadline, _ := ctx.Deadline()
		if err = batcher.WaitForClose(time.Until(deadline)); err != nil {
			t.Error(err)
		}
	}()

	go func() {
		rdr.connChan <- nil
		// Only send two messages through.
		for i := 0; i < 2; i++ {
			rdr.readChan <- nil
		}
		rdr.readChan <- types.ErrTimeout
		rdr.ackChan <- nil
		for i := 0; i < 8; i++ {
			rdr.readChan <- nil
		}
		rdr.ackChan <- nil
		rdr.closeAsyncChan <- struct{}{}
		rdr.waitForCloseChan <- nil
	}()

	if err = batcher.ConnectWithContext(ctx); err != nil {
		t.Fatal(err)
	}

	msg, err := batcher.ReadNextWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 2 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != nil {
		t.Error(err)
	}

	if msg, err = batcher.ReadNextWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	if msg.Len() != 8 {
		t.Errorf("Wrong batch count: %v", msg.Len())
	}
	msg.Iter(func(i int, part types.Part) error {
		if exp, act := fmt.Sprintf("test %v", i+2), string(part.Get()); exp != act {
			t.Errorf("Wrong message contents: %v != %v", act, exp)
		}
		return nil
	})
	if err = batcher.AcknowledgeWithContext(ctx, nil); err != nil {
		t.Error(err)
	}
}
