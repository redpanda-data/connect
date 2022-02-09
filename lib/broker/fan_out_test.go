package broker

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.Consumer = &FanOut{}
var _ types.Closable = &FanOut{}

//------------------------------------------------------------------------------

func TestBasicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	if !oTM.Connected() {
		t.Error("Not connected")
	}

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		resChanSlice := []chan<- response.Error{}
		for j := 0; j < nOutputs; j++ {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[j].TChan:
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
				resChanSlice = append(resChanSlice, ts.ResponseChan)
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
		}
		for j := 0; j < nOutputs; j++ {
			select {
			case resChanSlice[j] <- response.NewError(nil):
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to broker")
				return
			}
		}
		select {
		case res := <-resChan:
			if res.AckError() != nil {
				t.Errorf("Received unexpected errors from broker: %v", res.AckError())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestFanOutBackPressure(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := []types.Output{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(outputs, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	doneChan := make(chan struct{})
	go func() {
		defer wg.Done()
		// Consume as fast as possible from mock one
		for {
			select {
			case ts := <-mockOne.TChan:
				select {
				case ts.ResponseChan <- response.NewError(nil):
				case <-doneChan:
					return
				}
			case <-doneChan:
				return
			}
		}
	}()

	i := 0
bpLoop:
	for ; i < 1000; i++ {
		select {
		case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
		case <-time.After(time.Millisecond * 200):
			break bpLoop
		}
	}
	if i > 500 {
		t.Error("We shouldn't be capable of dumping this many messages into a blocked broker")
	}

	close(readChan)
	close(doneChan)
	wg.Wait()
}

func TestFanOutAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := []types.Output{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err == nil {
		t.Error("Expected error on duplicate receive call")
	}

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}
	var ts1, ts2 message.Transaction
	select {
	case ts1 = <-mockOne.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case ts2 = <-mockTwo.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case ts1.ResponseChan <- response.NewError(nil):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case ts2.ResponseChan <- response.NewError(errors.New("this is a test")):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case <-mockOne.TChan:
		t.Error("Received duplicate message to mockOne")
	case ts2 = <-mockTwo.TChan:
	case <-resChan:
		t.Error("Received premature response from broker")
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockTwo")
		return
	}
	select {
	case ts2.ResponseChan <- response.NewError(nil):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case res := <-resChan:
		if res.AckError() != nil {
			t.Errorf("Fan out returned error %v", res.AckError())
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out responding to broker")
		return
	}

	close(readChan)

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestFanOutShutDownFromErrorResponse(t *testing.T) {
	outputs := []types.Output{}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	var ts message.Transaction
	var open bool
	select {
	case ts, open = <-mockOutput.TChan:
		if !open {
			t.Error("fan out output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	select {
	case ts.ResponseChan <- response.NewError(errors.New("test")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for res send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromReceive(t *testing.T) {
	outputs := []types.Output{}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.TChan:
		if !open {
			t.Error("fan out output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromSend(t *testing.T) {
	outputs := []types.Output{}
	mockOutput := &MockOutputType{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------

func BenchmarkBasicFanOut(b *testing.B) {
	nOutputs, nMsgs := 3, b.N

	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		b.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		b.Error(err)
		return
	}

	content := [][]byte{[]byte("hello world")}
	rChanSlice := make([]chan<- response.Error, nOutputs)

	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- message.NewTransaction(message.QuickBatch(content), resChan)
		for j := 0; j < nOutputs; j++ {
			ts := <-mockOutputs[j].TChan
			rChanSlice[j] = ts.ResponseChan
		}
		for j := 0; j < nOutputs; j++ {
			rChanSlice[j] <- response.NewError(nil)
		}
		res := <-resChan
		if res.AckError() != nil {
			b.Errorf("Received unexpected errors from broker: %v", res.AckError())
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
