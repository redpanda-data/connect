package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestOutputWrapperShutdown(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mOutput := &mock.OutputChanneled{
		TChan: make(<-chan message.Transaction),
	}

	mWrapped, err := wrapOutput(mOutput)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for ts := range mOutput.TChan {
			assert.NoError(t, ts.Ack(tCtx, nil))
		}
		wg.Done()
	}()

	// Trigger Async Shutdown
	go func() {
		time.Sleep(time.Millisecond * 50)
		mWrapped.TriggerStopConsuming()
	}()

	for i := 0; i < 1000; i++ {
		require.NoError(t, mWrapped.WriteTransaction(tCtx, message.NewTransactionFunc(message.Batch{
			message.NewPart([]byte("hello world")),
		}, func(ctx context.Context, err error) error {
			return nil
		})))
	}

	wg.Wait()
}
