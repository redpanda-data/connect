package broker

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ types.Consumer = &DynamicFanOut{}
var _ types.Closable = &DynamicFanOut{}

func TestBasicDynamicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := map[string]DynamicOutput{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs[fmt.Sprintf("out-%v", i)] = mockOutputs[i]
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewDynamicFanOut(
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

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		wg := sync.WaitGroup{}
		wg.Add(nOutputs)
		for j := 0; j < nOutputs; j++ {
			go func(index int) {
				defer wg.Done()
				var ts types.Transaction
				select {
				case ts = <-mockOutputs[index].TChan:
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewError(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(j)
		}
		select {
		case readChan <- types.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		wg.Wait()
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

func TestDynamicFanOutChangeOutputs(t *testing.T) {
	nOutputs := 10

	outputs := map[string]*MockOutputType{}
	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewDynamicFanOut(
		nil, log.Noop(), metrics.Noop(),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		newOutput := &MockOutputType{}
		newOutputName := fmt.Sprintf("output-%v", i)

		outputs[newOutputName] = newOutput
		if err := oTM.SetOutput(newOutputName, newOutput, time.Second); err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *MockOutputType) {
				defer wg.Done()
				var ts types.Transaction
				select {
				case ts = <-out.TChan:
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewError(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

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

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *MockOutputType) {
				defer wg.Done()
				var ts types.Transaction
				select {
				case ts = <-out.TChan:
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewError(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

		select {
		case res := <-resChan:
			if res.AckError() != nil {
				t.Errorf("Received unexpected errors from broker: %v", res.AckError())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}

		oldOutputName := fmt.Sprintf("output-%v", i)
		if err := oTM.SetOutput(oldOutputName, nil, time.Second); err != nil {
			t.Fatal(err)
		}
		delete(outputs, oldOutputName)
	}

	oTM.CloseAsync()

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestDynamicFanOutAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := map[string]DynamicOutput{
		"first":  &mockOne,
		"second": &mockTwo,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewDynamicFanOut(
		outputs, log.Noop(), metrics.Noop(),
	)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))
	assert.NotNil(t, oTM.Consume(readChan), "Expected error on duplicate receive call")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		var ts types.Transaction
		select {
		case ts = <-mockOne.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(nil):
		case <-mockOne.TChan:
			t.Error("Received duplicate message to mockOne")
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()
	go func() {
		defer wg.Done()
		var ts types.Transaction
		select {
		case ts = <-mockTwo.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(errors.New("this is a test")):
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
		select {
		case ts = <-mockTwo.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockTwo")
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(nil):
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()

	select {
	case readChan <- types.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for broker send")
	}

	wg.Wait()

	select {
	case res := <-resChan:
		if res.AckError() != nil {
			t.Errorf("Fan out returned error %v", res.AckError())
		}
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
	}

	close(readChan)

	assert.NoError(t, oTM.WaitForClose(time.Second*5))
}

func TestDynamicFanOutStartEmpty(t *testing.T) {
	mockOne := MockOutputType{}

	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	outputs := map[string]DynamicOutput{}

	oTM, err := NewDynamicFanOut(outputs, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	oTM.WithMaxInFlight(10)
	require.NoError(t, oTM.Consume(readChan))
	assert.NotNil(t, oTM.Consume(readChan), "Expected error on duplicate receive call")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		select {
		case readChan <- types.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for broker send")
		}
	}()

	require.NoError(t, oTM.SetOutput("first", &mockOne, time.Second))

	go func() {
		defer wg.Done()
		var ts types.Transaction
		select {
		case ts = <-mockOne.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(nil):
		case <-mockOne.TChan:
			t.Error("Received duplicate message to mockOne")
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()

	wg.Wait()

	select {
	case res := <-resChan:
		if res.AckError() != nil {
			t.Errorf("Fan out returned error %v", res.AckError())
		}
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
	}

	close(readChan)

	assert.NoError(t, oTM.WaitForClose(time.Second*5))
}

func TestDynamicFanOutShutDownFromErrorResponse(t *testing.T) {
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	outputAddedList := []string{}
	outputRemovedList := []string{}

	oTM, err := NewDynamicFanOut(
		outputs, log.Noop(), metrics.Noop(),
		OptDynamicFanOutSetOnAdd(func(label string) {
			outputAddedList = append(outputAddedList, label)
		}), OptDynamicFanOutSetOnRemove(func(label string) {
			outputRemovedList = append(outputRemovedList, label)
		}),
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
	case readChan <- types.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	var ts types.Transaction
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

	if exp, act := []string{"test"}, outputAddedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of added outputs: %v != %v", act, exp)
	}
	if exp, act := []string{}, outputRemovedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of removed outputs: %v != %v", act, exp)
	}
}

func TestDynamicFanOutShutDownFromReceive(t *testing.T) {
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewDynamicFanOut(
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
	case readChan <- types.NewTransaction(message.QuickBatch(nil), resChan):
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

func TestDynamicFanOutShutDownFromSend(t *testing.T) {
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan response.Error)

	oTM, err := NewDynamicFanOut(
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
	case readChan <- types.NewTransaction(message.QuickBatch(nil), resChan):
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
