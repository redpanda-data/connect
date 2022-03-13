package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ output.Streamed = &Try{}

func TestTryDoubleClose(t *testing.T) {
	oTM, err := NewTry([]output.Streamed{&MockOutputType{}}, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	// This shouldn't cause a panic
	oTM.CloseAsync()
	oTM.CloseAsync()
}

//------------------------------------------------------------------------------

func TestTryHappyPath(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	outputs := []output.Streamed{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := NewTry(outputs, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		go func() {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[0].TChan:
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[1].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
			require.NoError(t, ts.Ack(tCtx, nil))
		}()

		select {
		case res := <-resChan:
			if res != nil {
				t.Error(res)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryHappyishPath(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	outputs := []output.Streamed{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := NewTry(outputs, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		go func() {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[0].TChan:
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[1].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
			require.NoError(t, ts.Ack(tCtx, errors.New("test err")))

			select {
			case ts = <-mockOutputs[1].TChan:
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[0].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
			require.NoError(t, ts.Ack(tCtx, nil))
		}()

		select {
		case res := <-resChan:
			if res != nil {
				t.Error(res)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryAllFail(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	outputs := []output.Streamed{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := NewTry(outputs, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for broker send")
		}

		testErr := errors.New("test error")
		go func() {
			for j := 0; j < 3; j++ {
				var ts message.Transaction
				select {
				case ts = <-mockOutputs[j%3].TChan:
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-mockOutputs[(j+1)%3].TChan:
					t.Errorf("Received message in wrong order: %v != %v", j%3, (j+1)%3)
					return
				case <-mockOutputs[(j+2)%3].TChan:
					t.Errorf("Received message in wrong order: %v != %v", j%3, (j+2)%3)
					return
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				require.NoError(t, ts.Ack(tCtx, testErr))
			}
		}()

		select {
		case res := <-resChan:
			if exp, act := testErr, res; exp != act {
				t.Errorf("Wrong error returned: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryAllFailParallel(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	outputs := []output.Streamed{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan message.Transaction)

	oTM, err := NewTry(outputs, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	oTM = oTM.WithMaxInFlight(50)
	if err = oTM.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	resChans := make([]chan error, 10)
	for i := range resChans {
		resChans[i] = make(chan error)
	}

	tallies := [3]int32{}

	wg, wgStart := sync.WaitGroup{}, sync.WaitGroup{}
	testErr := errors.New("test error")
	startChan := make(chan struct{})
	for _, resChan := range resChans {
		wg.Add(1)
		wgStart.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				var ts message.Transaction
				var index int
				select {
				case ts = <-mockOutputs[j%3].TChan:
					index = j % 3
				case ts = <-mockOutputs[(j+1)%3].TChan:
					index = (j + 1) % 3
				case ts = <-mockOutputs[(j+2)%3].TChan:
					index = (j + 2) % 3
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					if j == 0 {
						wgStart.Done()
					}
					return
				}
				atomic.AddInt32(&tallies[index], 1)
				if j == 0 {
					wgStart.Done()
				}

				<-startChan
				require.NoError(t, ts.Ack(tCtx, testErr))
			}
		}()
		select {
		case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for broker send")
		}
	}
	wgStart.Wait()
	close(startChan)

	for _, resChan := range resChans {
		select {
		case res := <-resChan:
			if exp, act := testErr, res; exp != act {
				t.Errorf("Wrong error returned: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
		}
	}

	wg.Wait()
	for _, tally := range tallies {
		if int(tally) != len(resChans) {
			t.Errorf("Wrong count of propagated messages: %v", tally)
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
