package manager

import (
	"context"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	iinput "github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

var _ iinput.Streamed = &inputWrapper{}

type inputWrapper struct {
	input     iinput.Streamed
	inputLock sync.Mutex
	tranChan  chan message.Transaction

	shutSig *shutdown.Signaller
}

func wrapInput(i iinput.Streamed) *inputWrapper {
	w := &inputWrapper{
		input:    i,
		tranChan: make(chan message.Transaction),
		shutSig:  shutdown.NewSignaller(),
	}
	go w.loop()
	return w
}

func (w *inputWrapper) closeExistingInput(ctx context.Context) error {
	w.inputLock.Lock()
	tmpInput := w.input
	w.inputLock.Unlock()

	if tmpInput == nil {
		return nil
	}

	tmpInput.CloseAsync()
	for {
		if err := tmpInput.WaitForClose(time.Millisecond * 100); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (w *inputWrapper) swapInput(i iinput.Streamed) {
	w.inputLock.Lock()
	w.input = i
	w.inputLock.Unlock()
}

func (w *inputWrapper) TransactionChan() <-chan message.Transaction {
	return w.tranChan
}

func (w *inputWrapper) Connected() bool {
	w.inputLock.Lock()
	con := w.input != nil && w.input.Connected()
	w.inputLock.Unlock()
	return con
}

func (w *inputWrapper) loop() {
	defer func() {
		w.inputLock.Lock()
		tmpInput := w.input
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
		w.inputLock.Lock()
		if w.input != nil {
			tChan = w.input.TransactionChan()
		}
		w.inputLock.Unlock()

		var t message.Transaction
		var open bool

		if tChan != nil {
			select {
			case t, open = <-tChan:
			case <-w.shutSig.CloseAtLeisureChan():
				return
			}
		}

		// TODO: Should a natural shutdown be allowed to propagate here?
		if !open {
			select {
			case <-time.After(time.Second):
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
