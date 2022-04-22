package pure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ input.Streamed = &fanInInputBroker{}

func TestBasicFanIn(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nInputs, nMsgs := 10, 1000

	Inputs := []input.Streamed{}
	mockInputs := []*mock.Input{}

	resChan := make(chan error)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := newFanInInputBroker(Inputs)
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- message.NewTransaction(message.QuickBatch(content), resChan):
			case <-time.After(time.Second * 5):
				t.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			go func() {
				var ts message.Transaction
				select {
				case ts = <-fanIn.TransactionChan():
					if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				}
				require.NoError(t, ts.Ack(tCtx, nil))
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

	Inputs := []input.Streamed{}
	mockInputs := []*mock.Input{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := newFanInInputBroker(Inputs)
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
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nInputs, nMsgs := 10, 1000

	Inputs := []input.Streamed{}
	mockInputs := []*mock.Input{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := newFanInInputBroker(Inputs)
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(nInputs)

	for j := 0; j < nInputs; j++ {
		go func(index int) {
			rChan := make(chan error)
			for i := 0; i < nMsgs; i++ {
				content := [][]byte{[]byte(fmt.Sprintf("hello world %v %v", i, index))}
				select {
				case mockInputs[index].TChan <- message.NewTransaction(message.QuickBatch(content), rChan):
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-rChan:
					if expected, actual := string(content[0]), res.Error(); expected != actual {
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
		var ts message.Transaction
		select {
		case ts = <-fanIn.TransactionChan():
		case <-time.After(time.Second * 5):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		require.NoError(t, ts.Ack(tCtx, errors.New(string(ts.Payload.Get(0).Get()))))
	}

	wg.Wait()
}

func BenchmarkBasicFanIn(b *testing.B) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nInputs := 10

	Inputs := []input.Streamed{}
	mockInputs := []*mock.Input{}
	resChan := make(chan error)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &mock.Input{
			TChan: make(chan message.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := newFanInInputBroker(Inputs)
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
			case mockInputs[j].TChan <- message.NewTransaction(message.QuickBatch(content), resChan):
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			var ts message.Transaction
			select {
			case ts = <-fanIn.TransactionChan():
				if !bytes.Equal(ts.Payload.Get(0).Get(), content[0]) {
					b.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				return
			}
			require.NoError(b, ts.Ack(tCtx, nil))
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
