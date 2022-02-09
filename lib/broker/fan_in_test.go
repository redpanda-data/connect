package broker

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.Producer = &FanIn{}
var _ types.Closable = &FanIn{}

//------------------------------------------------------------------------------

func TestBasicFanIn(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	resChan := make(chan response.Error)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- types.NewTransaction(message.QuickBatch(content), resChan):
			case <-time.After(time.Second * 5):
				t.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			go func() {
				var ts types.Transaction
				select {
				case ts = <-fanIn.TransactionChan():
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				}
				select {
				case ts.ResponseChan <- response.NewError(nil):
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for response to broker: %v, %v", i, j)
				}
			}()
			select {
			case <-resChan:
			case <-time.After(time.Second * 5):
				t.Errorf("Timed out waiting for response to input: %v, %v", i, j)
				return
			}
		}
	}

	fanIn.CloseAsync()

	if err := fanIn.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestFanInShutdown(t *testing.T) {
	nInputs := 10

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.Noop())
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
		close(mockIn.TChan)
	}

	select {
	case <-fanIn.TransactionChan():
	case <-time.After(time.Second * 5):
		t.Error("fan in failed to close")
	}
}

func TestFanInAsync(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(nInputs)

	for j := 0; j < nInputs; j++ {
		go func(index int) {
			rChan := make(chan response.Error)
			for i := 0; i < nMsgs; i++ {
				content := [][]byte{[]byte(fmt.Sprintf("hello world %v %v", i, index))}
				select {
				case mockInputs[index].TChan <- types.NewTransaction(message.QuickBatch(content), rChan):
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-rChan:
					if expected, actual := string(content[0]), res.AckError().Error(); expected != actual {
						t.Errorf("Wrong response: %v != %v", expected, actual)
					}
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for response to input: %v, %v", i, index)
					return
				}
			}
			wg.Done()
		}(j)
	}

	for i := 0; i < nMsgs*nInputs; i++ {
		var ts types.Transaction
		select {
		case ts = <-fanIn.TransactionChan():
		case <-time.After(time.Second * 5):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(errors.New(string(ts.Payload.Get(0).Get()))):
		case <-time.After(time.Second * 5):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	wg.Wait()
}

func BenchmarkBasicFanIn(b *testing.B) {
	nInputs := 10

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}
	resChan := make(chan response.Error)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.Noop())
	if err != nil {
		b.Error(err)
		return
	}

	defer func() {
		fanIn.CloseAsync()
		fanIn.WaitForClose(time.Second)
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- types.NewTransaction(message.QuickBatch(content), resChan):
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			var ts types.Transaction
			select {
			case ts = <-fanIn.TransactionChan():
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					b.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				return
			}
			select {
			case ts.ResponseChan <- response.NewError(nil):
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for response to broker: %v, %v", i, j)
				return
			}
			select {
			case <-resChan:
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for response to input: %v, %v", i, j)
				return
			}
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
