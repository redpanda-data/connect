package manager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

var _ input.Streamed = &InputWrapper{}

type inputCtrl struct {
	input         input.Streamed
	closedForSwap *int32
}

type InputWrapper struct {
	ctrl      *inputCtrl
	inputLock sync.Mutex

	tranChan chan message.Transaction
	shutSig  *shutdown.Signaller
}

func WrapInput(i input.Streamed) *InputWrapper {
	var s int32
	w := &InputWrapper{
		ctrl: &inputCtrl{
			input:         i,
			closedForSwap: &s,
		},
		tranChan: make(chan message.Transaction),
		shutSig:  shutdown.NewSignaller(),
	}
	go w.loop()
	return w
}

func (w *InputWrapper) CloseExistingInput(ctx context.Context, forSwap bool) error {
	w.inputLock.Lock()
	tmpInput := w.ctrl.input
	if forSwap {
		atomic.StoreInt32(w.ctrl.closedForSwap, 1)
	} else {
		atomic.StoreInt32(w.ctrl.closedForSwap, 0)
	}
	w.inputLock.Unlock()

	if tmpInput == nil {
		return nil
	}

	tmpInput.TriggerStopConsuming()
	return tmpInput.WaitForClose(ctx)
}

func (w *InputWrapper) SwapInput(i input.Streamed) {
	var s int32
	w.inputLock.Lock()
	w.ctrl = &inputCtrl{
		input:         i,
		closedForSwap: &s,
	}
	w.inputLock.Unlock()
}

func (w *InputWrapper) TransactionChan() <-chan message.Transaction {
	return w.tranChan
}

func (w *InputWrapper) Connected() bool {
	w.inputLock.Lock()
	con := w.ctrl.input != nil && w.ctrl.input.Connected()
	w.inputLock.Unlock()
	return con
}

func (w *InputWrapper) loop() {
	defer func() {
		w.inputLock.Lock()
		tmpInput := w.ctrl.input
		w.inputLock.Unlock()

		if tmpInput != nil {
			tmpInput.TriggerStopConsuming()
			_ = tmpInput.WaitForClose(context.Background())
		}

		close(w.tranChan)
		w.shutSig.ShutdownComplete()
	}()

	for {
		var tChan <-chan message.Transaction
		var closedForSwap *int32

		w.inputLock.Lock()
		if w.ctrl.input != nil {
			tChan = w.ctrl.input.TransactionChan()
			closedForSwap = w.ctrl.closedForSwap
		}
		w.inputLock.Unlock()

		var t message.Transaction
		var open bool

		if tChan != nil {
			select {
			case t, open = <-tChan:
				// If closed and is natural (not closed for swap) then exit
				// gracefully.
				if !open && atomic.LoadInt32(closedForSwap) == 0 {
					return
				}
			case <-w.shutSig.CloseAtLeisureChan():
				return
			}
		}

		if !open {
			select {
			case <-time.After(time.Millisecond * 100):
			case <-w.shutSig.CloseAtLeisureChan():
				return
			}
			continue
		}

		select {
		case w.tranChan <- t:
		case <-w.shutSig.CloseAtLeisureChan():
			ctx, done := w.shutSig.CloseNowCtx(context.Background())
			_ = t.Ack(ctx, component.ErrTypeClosed)
			done()
			return
		}
	}
}

func (w *InputWrapper) TriggerStopConsuming() {
	w.shutSig.CloseAtLeisure()
}

func (w *InputWrapper) TriggerCloseNow() {
	w.shutSig.CloseNow()
}

func (w *InputWrapper) WaitForClose(ctx context.Context) error {
	select {
	case <-w.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
