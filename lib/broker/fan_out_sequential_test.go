package broker

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

//------------------------------------------------------------------------------

func TestBasicFanOutSequential(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []output.Streamed{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOutSequential(outputs, log.Noop(), metrics.Noop())
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

func TestFanOutSequentialAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := []output.Streamed{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOutSequential(outputs, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
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
	case ts1.ResponseChan <- response.NewError(nil):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case ts2 = <-mockTwo.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
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

func TestFanOutSequentialBlock(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := []output.Streamed{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewFanOutSequential(outputs, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
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
	case ts1.ResponseChan <- response.NewError(errors.New("this is a test")):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case ts1 = <-mockOne.TChan:
	case <-mockTwo.TChan:
		t.Error("Received premature message to mockTwo")
	case <-resChan:
		t.Error("Received premature response from broker")
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
	case ts2 = <-mockTwo.TChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
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

//------------------------------------------------------------------------------
