package output

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type notBatchedOutput struct {
	out Type

	inChan  <-chan types.Transaction
	outChan chan types.Transaction

	ctx   context.Context
	close func()

	fullyCloseCtx context.Context
	fullyClose    func()

	closedChan chan struct{}
}

// OnlySinglePayloads expands message batches into individual payloads,
// respecting the max in flight of the wrapped output. This is a more efficient
// way of feeding messages into an output that handles its own batching
// mechanism internally, or does not support batching at all.
func OnlySinglePayloads(out Type) Type {
	n := &notBatchedOutput{
		out:        out,
		outChan:    make(chan types.Transaction),
		closedChan: make(chan struct{}),
	}
	n.ctx, n.close = context.WithCancel(context.Background())
	n.fullyCloseCtx, n.fullyClose = context.WithCancel(context.Background())
	return n
}

//------------------------------------------------------------------------------

func (n *notBatchedOutput) breakMessageOut(msg *message.Batch) error {
	var wg sync.WaitGroup

	var batchErr *batch.Error
	var batchErrMut sync.Mutex
	addBatchErr := func(i int, err error) {
		if err != nil {
			batchErrMut.Lock()
			if batchErr == nil {
				batchErr = batch.NewError(msg, err)
			}
			batchErr.Failed(i, err)
			batchErrMut.Unlock()
		}
	}

	if err := msg.Iter(func(i int, p *message.Part) error {
		index := i

		tmpResChan := make(chan types.Response)
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.Append(p)

		select {
		case n.outChan <- types.NewTransaction(tmpMsg, tmpResChan):
		case <-n.ctx.Done():
			if index == 0 {
				return component.ErrTypeClosed
			}
			addBatchErr(index, component.ErrTypeClosed)
			return nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			select {
			case res := <-tmpResChan:
				err = res.Error()
			case <-n.fullyCloseCtx.Done():
				err = component.ErrTypeClosed
			}
			addBatchErr(index, err)
		}()
		return nil
	}); err != nil {
		return err
	}

	wg.Wait()
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (n *notBatchedOutput) loop() {
	defer func() {
		n.out.CloseAsync()
		_ = n.out.WaitForClose(shutdown.MaximumShutdownWait())
		close(n.closedChan)
	}()

	for {
		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-n.inChan:
			if !open {
				return
			}
		case <-n.ctx.Done():
			return
		}

		if tran.Payload.Len() == 1 {
			select {
			case n.outChan <- tran:
			case <-n.ctx.Done():
				return
			}
		} else {
			var res types.Response = response.NewAck()
			if err := n.breakMessageOut(tran.Payload); err != nil {
				if err == component.ErrTypeClosed {
					return
				}
				res = response.NewError(err)
			}
			select {
			case tran.ResponseChan <- res:
			case <-n.fullyCloseCtx.Done():
				return
			}
		}
	}
}

//------------------------------------------------------------------------------

func (n *notBatchedOutput) Consume(ts <-chan types.Transaction) error {
	if n.inChan != nil {
		return component.ErrAlreadyStarted
	}
	if err := n.out.Consume(n.outChan); err != nil {
		return err
	}
	n.inChan = ts
	go n.loop()
	return nil
}

func (n *notBatchedOutput) MaxInFlight() (int, bool) {
	return output.GetMaxInFlight(n.out)
}

func (n *notBatchedOutput) Connected() bool {
	return n.out.Connected()
}

func (n *notBatchedOutput) CloseAsync() {
	n.close()
	n.fullyClose()
}

// WaitForClose blocks until the File output has closed down.
func (n *notBatchedOutput) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
