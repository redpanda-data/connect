package pure

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestBasicFanOutSequential(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []output.Streamed{}
	mockOutputs := []*mock.OutputChanneled{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &mock.OutputChanneled{})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutSequentialOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	assert.True(t, oTM.Connected())

	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- message.NewTransaction(message.QuickBatch(content), resChan):
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for broker send")
		}
		for j := 0; j < nOutputs; j++ {
			select {
			case ts := <-mockOutputs[j].TChan:
				if !bytes.Equal(ts.Payload.Get(0).AsBytes(), content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).AsBytes(), content[0])
				}
				go func() {
					require.NoError(t, ts.Ack(tCtx, nil))
				}()
			case <-time.After(time.Second):
				t.Fatal("Timed out waiting for broker propagate", j)
			}
		}
		select {
		case res := <-resChan:
			require.NoError(t, res)
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	oTM.TriggerCloseNow()
	assert.NoError(t, oTM.WaitForClose(ctx))
}

func TestFanOutSequentialBlock(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	mockOne := mock.OutputChanneled{}
	mockTwo := mock.OutputChanneled{}

	outputs := []output.Streamed{&mockOne, &mockTwo}
	readChan := make(chan message.Transaction)
	resChan := make(chan error, 1)

	oTM, err := newFanOutSequentialOutputBroker(outputs)
	require.NoError(t, err)
	require.NoError(t, oTM.Consume(readChan))

	select {
	case readChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for broker send")
	}
	var ts1, ts2 message.Transaction
	select {
	case ts1 = <-mockOne.TChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for mockOne")
	}
	go func() {
		require.NoError(t, ts1.Ack(tCtx, nil))
	}()

	select {
	case ts2 = <-mockTwo.TChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for mockOne")
	}
	go func() {
		require.NoError(t, ts2.Ack(tCtx, nil))
	}()

	select {
	case res := <-resChan:
		require.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("Timed out responding to broker")
	}

	close(readChan)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()
	require.NoError(t, oTM.WaitForClose(ctx))
}
