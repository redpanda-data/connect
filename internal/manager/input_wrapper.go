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

var _ input.Streamed = &inputWrapper{}

type inputCtrl struct {
	input         input.Streamed
	closedForSwap *int32
}

type inputWrapper struct {
	ctrl      *inputCtrl
	inputLock sync.Mutex

	tranChan chan message.Transaction
	shutSig  *shutdown.Signaller
}

func wrapInput(i input.Streamed) *inputWrapper {
	var s int32
	w := &inputWrapper{
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

func (w *inputWrapper) closeExistingInput(ctx context.Context) error {
	w.inputLock.Lock()
	tmpInput := w.ctrl.input
	atomic.StoreInt32(w.ctrl.closedForSwap, 1)
	w.inputLock.Unlock()

	if tmpInput == nil {
		return nil
	}

	deadline, hasDeadline := ctx.Deadline()
	tFor := time.Millisecond * 100
	if hasDeadline {
		tFor = time.Until(deadline)
	}

	tmpInput.CloseAsync()
	for {
		if err := tmpInput.WaitForClose(tFor); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (w *inputWrapper) swapInput(i input.Streamed) {
	var s int32
	w.inputLock.Lock()
	w.ctrl = &inputCtrl{
		input:         i,
		closedForSwap: &s,
	}
	w.inputLock.Unlock()
}

func (w *inputWrapper) TransactionChan() <-chan message.Transaction {
	return w.tranChan
}

func (w *inputWrapper) Connected() bool {
	w.inputLock.Lock()
	con := w.ctrl.input != nil && w.ctrl.input.Connected()
	w.inputLock.Unlock()
	return con
}

func (w *inputWrapper) loop() {
	defer func() {
		w.inputLock.Lock()
		tmpInput := w.ctrl.input
		w.inputLock.Unlock()

		if tmpInput != nil {
			tmpInput.CloseAsync()
			for {
				if err := tmpInput.WaitForClose(time.Second); err == nil {
					break
				}
			}
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

func (w *inputWrapper) CloseAsync() {
	w.shutSig.CloseAtLeisure()
}

func (w *inputWrapper) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		w.shutSig.CloseNow()
	}()
	select {
	case <-w.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
