package pure

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ output.Streamed = &fanOutOutputBroker{}

func TestBasicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []output.Streamed{}
	mockOutputs := []*mock.OutputChanneled{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	assert.True(t, oTM.Connected())

	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		resFnSlice := []func(context.Context, error) error{}
		for j := 0; j < nOutputs; j++ {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[j].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
				resFnSlice = append(resFnSlice, ts.Ack)
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for broker propagate")
			}
		}

		for j := 0; j < nOutputs; j++ {
			require.NoError(t, resFnSlice[j](tCtx, err))
		}

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

func TestBasicFanOutMutations(t *testing.T) {
	mockOutputA := &mock.OutputChanneled{}
	mockOutputB := &mock.OutputChanneled{}
	outputs := []output.Streamed{
		mockOutputA,
		mockOutputB,
	}

	readChan := make(chan message.Transaction)

	oTM, err := newFanOutOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	assert.True(t, oTM.Connected())

	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	inMsg := message.NewPart(nil)
	inMsg.SetStructuredMut(map[string]any{
		"hello": "world",
	})

	inBatch := message.Batch{inMsg}
	select {
	case readChan <- message.NewTransactionFunc(inBatch, func(ctx context.Context, _ error) error {
		inStruct, err := inMsg.AsStructuredMut()
		require.NoError(t, err)

		assert.Equal(t, map[string]any{
			"hello": "world",
		}, inStruct)

		_, err = gabs.Wrap(inStruct).Set("quack", "moo")
		require.NoError(t, err)
		return nil
	}):
	case <-time.After(time.Second):
		t.Errorf("Timed out waiting for broker send")
		return
	}

	testMockOutput := func(mockOutput *mock.OutputChanneled) {
		var ts message.Transaction
		select {
		case ts = <-mockOutput.TChan:
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker propagate")
		}

		outStruct, err := ts.Payload.Get(0).AsStructuredMut()
		require.NoError(t, err)
		assert.Equal(t, map[string]any{
			"hello": "world",
		}, outStruct)

		_, err = gabs.Wrap(outStruct).Set("woof", "meow")
		require.NoError(t, err)
		require.NoError(t, ts.Ack(tCtx, nil))
	}

	testMockOutput(mockOutputA)
	testMockOutput(mockOutputB)

	inStruct, err := inMsg.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"hello": "world",
		"moo":   "quack",
	}, inStruct)

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))
}

func TestFanOutBackPressure(t *testing.T) {
	mockOne := mock.OutputChanneled{}
	mockTwo := mock.OutputChanneled{}

	outputs := []output.Streamed{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := newFanOutOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	wg := sync.WaitGroup{}
	wg.Add(1)
	doneChan := make(chan struct{})
	go func() {
		defer wg.Done()
		// Consume as fast as possible from mock one
		for {
			select {
			case ts := <-mockOne.TChan:
				require.NoError(t, ts.Ack(ctx, nil))
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
	done()
	close(doneChan)
	wg.Wait()
}

func TestFanOutShutDownFromReceive(t *testing.T) {
	outputs := []output.Streamed{}
	mockOutput := &mock.OutputChanneled{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutOutputBroker(outputs)
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
		// We do not ack the transaction
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(ctx))

	select {
	case _, open := <-mockOutput.TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromSend(t *testing.T) {
	outputs := []output.Streamed{}
	mockOutput := &mock.OutputChanneled{}
	outputs = append(outputs, mockOutput)
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for msg send")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(ctx))

	select {
	case _, open := <-mockOutput.TChan:
		assert.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------

func BenchmarkBasicFanOut(b *testing.B) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nOutputs, nMsgs := 3, b.N

	outputs := []output.Streamed{}
	mockOutputs := []*mock.OutputChanneled{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutOutputBroker(outputs)
	require.NoError(b, err)
	require.NoError(b, oTM.Consume(readChan))

	content := [][]byte{[]byte("hello world")}
	rFnSlice := make([]func(context.Context, error) error, nOutputs)

	b.ReportAllocs()
	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- message.NewTransaction(message.QuickBatch(content), resChan)
		for j := 0; j < nOutputs; j++ {
			ts := <-mockOutputs[j].TChan
			rFnSlice[j] = ts.Ack
		}
		for j := 0; j < nOutputs; j++ {
			require.NoError(b, rFnSlice[j](tCtx, nil))
		}
		res := <-resChan
		if res != nil {
			b.Errorf("Received unexpected errors from broker: %v", res)
		}
	}

	b.StopTimer()
}
