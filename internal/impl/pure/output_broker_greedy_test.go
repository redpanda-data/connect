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

var _ output.Streamed = &greedyOutputBroker{}

func TestGreedyDoubleClose(t *testing.T) {
	oTM, err := newGreedyOutputBroker([]output.Streamed{})
	if err != nil {
		t.Error(err)
		return
	}

	// This shouldn't cause a panic
	oTM.TriggerCloseNow()
	oTM.TriggerCloseNow()
}

//------------------------------------------------------------------------------

func TestBasicGreedy(t *testing.T) {
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

	oTM, err := newGreedyOutputBroker(outputs)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	// Only read from a single output.
	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		go func() {
			var ts message.Transaction
			select {
			case ts = <-mockOutputs[0].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
			require.NoError(t, ts.Ack(tCtx, nil))
		}()

		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

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
	if err := oTM.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}
