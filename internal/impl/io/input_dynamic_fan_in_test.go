package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ input.Streamed = &dynamicFanInInput{}

func TestStaticBasicDynamicFanIn(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nInputs, nMsgs := 10, 1000

	Inputs := map[string]input.Streamed{}
	mockInputs := []*mock.Input{}
	resChan := make(chan error)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := newDynamicFanInInput(Inputs, log.Noop(), nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- message.NewTransaction(message.QuickBatch(content), resChan):
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			go func() {
				var ts message.Transaction
				select {
				case ts = <-fanIn.TransactionChan():
					if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
					return
				}
				require.NoError(t, ts.Ack(tCtx, nil))
			}()
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for response to input: %v, %v", i, j)
				return
			}
		}
	}

	fanIn.TriggerStopConsuming()
	require.NoError(t, fanIn.WaitForClose(tCtx))
}

func TestBasicDynamicFanIn(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nMsgs := 1000

	inputOne := &mock.Input{
		TChan: make(chan message.Transaction),
	}
	inputTwo := &mock.Input{
		TChan: make(chan message.Transaction),
	}

	fanIn, err := newDynamicFanInInput(nil, log.Noop(), nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	if err = fanIn.SetInput(ctx, "foo", inputOne); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	sendAllTestMessages := func(input *mock.Input, label string) {
		rChan := make(chan error)
		for i := 0; i < nMsgs; i++ {
			content := [][]byte{[]byte(fmt.Sprintf("%v-%v", label, i))}
			input.TChan <- message.NewTransaction(message.QuickBatch(content), rChan)
			select {
			case <-rChan:
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for response to input: %v", i)
				return
			}
		}
		wg.Done()
	}

	wg.Add(2)
	go sendAllTestMessages(inputOne, "inputOne")
	go sendAllTestMessages(inputTwo, "inputTwo")

	for i := 0; i < nMsgs; i++ {
		var ts message.Transaction
		expContent := fmt.Sprintf("inputOne-%v", i)
		select {
		case ts = <-fanIn.TransactionChan():
			if string(ts.Payload.Get(0).AsBytes()) != expContent {
				t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}

	if err = fanIn.SetInput(ctx, "foo", inputTwo); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nMsgs; i++ {
		var ts message.Transaction
		expContent := fmt.Sprintf("inputTwo-%v", i)
		select {
		case ts = <-fanIn.TransactionChan():
			if string(ts.Payload.Get(0).AsBytes()) != expContent {
				t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}

	wg.Wait()

	fanIn.TriggerStopConsuming()
	require.NoError(t, fanIn.WaitForClose(tCtx))
}

func TestStaticDynamicFanInShutdown(t *testing.T) {
	nInputs := 10

	Inputs := map[string]input.Streamed{}
	mockInputs := []*mock.Input{}

	expInputAddedList := []string{}
	expInputRemovedList := []string{}
	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		label := fmt.Sprintf("testinput%v", i)
		Inputs[label] = mockInputs[i]
		expInputAddedList = append(expInputAddedList, label)
		expInputRemovedList = append(expInputRemovedList, label)
	}

	var mapMut sync.Mutex
	inputAddedList := []string{}
	inputRemovedList := []string{}

	fanIn, err := newDynamicFanInInput(
		Inputs, log.Noop(),
		func(ctx context.Context, label string) {
			mapMut.Lock()
			inputAddedList = append(inputAddedList, label)
			mapMut.Unlock()
		},
		func(ctx context.Context, label string) {
			mapMut.Lock()
			inputRemovedList = append(inputRemovedList, label)
			mapMut.Unlock()
		},
	)
	if err != nil {
		t.Error(err)
		return
	}

	for _, mockIn := range mockInputs {
		select {
		case _, open := <-mockIn.TransactionChan():
			if !open {
				t.Error("fan in closed early")
			} else {
				t.Error("fan in sent unexpected message")
			}
		default:
		}
	}

	fanIn.TriggerStopConsuming()

	// All inputs should be closed.
	for _, mockIn := range mockInputs {
		select {
		case _, open := <-mockIn.TransactionChan():
			if open {
				t.Error("fan in sent unexpected message")
			}
		case <-time.After(time.Second):
			t.Error("fan in failed to close an input")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, fanIn.WaitForClose(ctx))
	done()

	mapMut.Lock()

	sort.Strings(expInputAddedList)
	sort.Strings(inputAddedList)
	sort.Strings(expInputRemovedList)
	sort.Strings(inputRemovedList)

	if exp, act := expInputAddedList, inputAddedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of added inputs: %v != %v", act, exp)
	}
	if exp, act := expInputRemovedList, inputRemovedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of removed inputs: %v != %v", act, exp)
	}

	mapMut.Unlock()
}

func TestStaticDynamicFanInAsync(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nInputs, nMsgs := 10, 1000

	Inputs := map[string]input.Streamed{}
	mockInputs := []*mock.Input{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := newDynamicFanInInput(Inputs, log.Noop(), nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer fanIn.TriggerStopConsuming()

	wg := sync.WaitGroup{}
	wg.Add(nInputs)

	for j := 0; j < nInputs; j++ {
		go func(index int) {
			rChan := make(chan error)
			for i := 0; i < nMsgs; i++ {
				content := [][]byte{[]byte(fmt.Sprintf("hello world %v %v", i, index))}
				select {
				case mockInputs[index].TChan <- message.NewTransaction(message.QuickBatch(content), rChan):
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-rChan:
					if expected, actual := string(content[0]), res.Error(); expected != actual {
						t.Errorf("Wrong response: %v != %v", expected, actual)
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for response to input: %v, %v", i, index)
					return
				}
			}
			wg.Done()
		}(j)
	}

	for i := 0; i < nMsgs*nInputs; i++ {
		var ts message.Transaction
		select {
		case ts = <-fanIn.TransactionChan():
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		require.NoError(t, ts.Ack(tCtx, errors.New(string(ts.Payload.Get(0).AsBytes()))))
	}

	wg.Wait()
}

//------------------------------------------------------------------------------
