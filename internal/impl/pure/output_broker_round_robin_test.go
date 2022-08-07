package pure

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ output.Streamed = &roundRobinOutputBroker{}

func TestRoundRobinDoubleClose(t *testing.T) {
	oTM, err := newRoundRobinOutputBroker([]output.Streamed{})
	if err != nil {
		t.Error(err)
		return
	}

	// This shouldn't cause a panic
	oTM.TriggerCloseNow()
	oTM.TriggerCloseNow()
}

//------------------------------------------------------------------------------

func TestBasicRoundRobin(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	nMsgs := 1000

	outputs := []output.Streamed{}
	mockOutputs := []*mock.OutputChanneled{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error)

	oTM, err := newRoundRobinOutputBroker(outputs)
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
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		go func() {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[i%3].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
			case <-mockOutputs[(i+1)%3].TChan:
				t.Errorf("Received message in wrong order: %v != %v", i%3, (i+1)%3)
				return
			case <-mockOutputs[(i+2)%3].TChan:
				t.Errorf("Received message in wrong order: %v != %v", i%3, (i+2)%3)
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
				t.Errorf("Received unexpected errors from broker: %v", res)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.TriggerCloseNow()
	require.NoError(t, oTM.WaitForClose(tCtx))
}

//------------------------------------------------------------------------------

func BenchmarkBasicRoundRobin(b *testing.B) {
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
	resChan := make(chan error)

	oTM, err := newRoundRobinOutputBroker(outputs)
	if err != nil {
		b.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		b.Error(err)
		return
	}

	content := [][]byte{[]byte("hello world")}

	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- message.NewTransaction(message.QuickBatch(content), resChan)
		ts := <-mockOutputs[i%3].TChan
		require.NoError(b, ts.Ack(tCtx, nil))
		res := <-resChan
		if res != nil {
			b.Errorf("Received unexpected errors from broker: %v", res)
		}
	}

	b.StopTimer()
}
