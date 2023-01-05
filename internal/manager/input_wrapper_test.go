package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	bmock "github.com/benthosdev/benthos/v4/internal/manager/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputWrapperSwap(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	conf := input.NewConfig()
	conf.Type = "generate"
	conf.Generate.Interval = "10ms"
	conf.Generate.Mapping = `root.name = "from root generate"`

	bMgr := bmock.NewManager()

	iWrapped, err := bMgr.NewInput(conf)
	require.NoError(t, err)

	iWrapper := wrapInput(iWrapped)
	select {
	case tran, open := <-iWrapper.TransactionChan():
		require.True(t, open)
		assert.Equal(t, `{"name":"from root generate"}`, string(tran.Payload.Get(0).AsBytes()))
		assert.NoError(t, tran.Ack(ctx, nil))
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	for i := 0; i < 5; i++ {
		conf = input.NewConfig()
		conf.Type = "generate"
		conf.Generate.Interval = "10ms"
		conf.Generate.Mapping = fmt.Sprintf(`root.name = "from generate %v"`, i)

		go func() {
			assert.NoError(t, iWrapper.closeExistingInput(ctx, true))

			iWrapped, err = bMgr.NewInput(conf)
			assert.NoError(t, err)

			iWrapper.swapInput(iWrapped)
		}()

		expected := fmt.Sprintf(`{"name":"from generate %v"}`, i)
	consumeLoop:
		for {
			select {
			case tran, open := <-iWrapper.TransactionChan():
				require.True(t, open, i)

				actual := string(tran.Payload.Get(0).AsBytes())
				assert.NoError(t, tran.Ack(ctx, nil), i)
				if expected == actual {
					break consumeLoop
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err(), i)
			}
		}
	}

	iWrapper.TriggerStopConsuming()
	require.NoError(t, iWrapper.WaitForClose(ctx))
}
