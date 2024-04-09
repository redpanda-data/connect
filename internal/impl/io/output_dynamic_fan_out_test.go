package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ output.Streamed = &dynamicFanOutOutputBroker{}

func TestBasicDynamicFanOut(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nOutputs, nMsgs := 10, 1000

	outputs := map[string]output.Streamed{}
	mockOutputs := []*mock.OutputChanneled{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
		outputs[fmt.Sprintf("out-%v", i)] = mockOutputs[i]
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newDynamicFanOutOutputBroker(outputs, log.Noop(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}
		wg := sync.WaitGroup{}
		for j := 0; j < nOutputs; j++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				var ts message.Transaction
				select {
				case ts = <-mockOutputs[index].TChan:
					if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
					}
				case <-time.After(time.Second):
					t.Error("Timed out waiting for broker propagate", index)
				}
				require.NoError(t, ts.Ack(tCtx, nil))
			}(j)
		}
		wg.Wait()
		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))
}

func TestDynamicFanOutChangeOutputs(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nOutputs := 10

	outputs := map[string]*mock.OutputChanneled{}
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newDynamicFanOutOutputBroker(nil, log.Noop(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		newOutput := &mock.OutputChanneled{}
		newOutputName := fmt.Sprintf("output-%v", i)

		outputs[newOutputName] = newOutput
		require.NoError(t, oTM.SetOutput(context.Background(), newOutputName, newOutput))

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *mock.OutputChanneled) {
				defer wg.Done()
				var ts message.Transaction
				select {
				case ts = <-out.TChan:
					if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).AsBytes(), content[0])
					}
				case <-time.After(time.Second):
					t.Error("Timed out waiting for broker propagate")
					return
				}
				require.NoError(t, ts.Ack(tCtx, nil))
			}(k, v)
		}

		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}

		wg.Wait()

		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *mock.OutputChanneled) {
				defer wg.Done()
				var ts message.Transaction
				select {
				case ts = <-out.TChan:
					if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).AsBytes(), content[0])
					}
				case <-time.After(time.Second):
					t.Error("Timed out waiting for broker propagate")
					return
				}
				require.NoError(t, ts.Ack(tCtx, nil))
			}(k, v)
		}

		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}

		wg.Wait()

		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}

		oldOutputName := fmt.Sprintf("output-%v", i)
		require.NoError(t, oTM.SetOutput(context.Background(), oldOutputName, nil))
		delete(outputs, oldOutputName)
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))
}

func TestDynamicFanOutAtLeastOnce(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockOne := mock.OutputChanneled{}
	mockTwo := mock.OutputChanneled{}

	outputs := map[string]output.Streamed{
		"first":  &mockOne,
		"second": &mockTwo,
	}
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newDynamicFanOutOutputBroker(outputs, log.Noop(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))
	assert.Error(t, oTM.Consume(readChan), "Expected error on duplicate receive call")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		var ts message.Transaction
		select {
		case ts = <-mockOne.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}()
	go func() {
		defer wg.Done()
		var ts message.Transaction
		select {
		case ts = <-mockTwo.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		require.NoError(t, ts.Ack(tCtx, errors.New("this is a test")))
	}()

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for broker send")
	}

	wg.Wait()

	select {
	case res := <-resChan:
		require.EqualError(t, res, "this is a test")
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))
}

func TestDynamicFanOutStartEmpty(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockOne := mock.OutputChanneled{}

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	outputs := map[string]output.Streamed{}

	oTM, err := newDynamicFanOutOutputBroker(outputs, log.Noop(), nil, nil)
	require.NoError(t, err)

	require.NoError(t, oTM.Consume(readChan))
	assert.Error(t, oTM.Consume(readChan), "Expected error on duplicate receive call")

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		select {
		case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
		case <-time.After(time.Second):
			t.Error("Timed out waiting for broker send")
		}
	}()

	require.NoError(t, oTM.SetOutput(context.Background(), "first", &mockOne))

	go func() {
		defer wg.Done()
		var ts message.Transaction
		select {
		case ts = <-mockOne.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}()

	wg.Wait()

	select {
	case res := <-resChan:
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
	}

	close(readChan)
	require.NoError(t, oTM.WaitForClose(tCtx))
}

func TestDynamicFanOutShutDownFromErrorResponse(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockOutput := &mock.OutputChanneled{}
	outputs := map[string]output.Streamed{
		"test": mockOutput,
	}
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	outputAddedList := []string{}
	outputRemovedList := []string{}

	oTM, err := newDynamicFanOutOutputBroker(
		outputs, log.Noop(),
		func(label string) {
			outputAddedList = append(outputAddedList, label)
		},
		func(label string) {
			outputRemovedList = append(outputRemovedList, label)
		},
	)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	var ts message.Transaction
	var open bool
	select {
	case ts, open = <-mockOutput.TChan:
		require.True(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}

	require.NoError(t, ts.Ack(tCtx, errors.New("test")))

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))

	select {
	case _, open := <-mockOutput.TChan:
		require.False(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}

	if exp, act := []string{"test"}, outputAddedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of added outputs: %v != %v", act, exp)
	}
	if exp, act := []string{}, outputRemovedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of removed outputs: %v != %v", act, exp)
	}
}

func TestDynamicFanOutShutDownFromReceive(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutput := &mock.OutputChanneled{}
	outputs := map[string]output.Streamed{
		"test": mockOutput,
	}
	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := newDynamicFanOutOutputBroker(outputs, log.Noop(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.TChan:
		require.True(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))

	select {
	case _, open := <-mockOutput.TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestDynamicFanOutShutDownFromSend(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockOutput := &mock.OutputChanneled{}
	outputs := map[string]output.Streamed{
		"test": mockOutput,
	}
	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := newDynamicFanOutOutputBroker(outputs, log.Noop(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))

	select {
	case _, open := <-mockOutput.TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}
