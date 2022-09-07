package stream

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// Type creates and manages the lifetime of a Benthos stream.
type Type struct {
	conf Config

	inputLayer    input.Streamed
	bufferLayer   buffer.Streamed
	pipelineLayer processor.Pipeline
	outputLayer   output.Streamed

	manager bundle.NewManagement

	onClose func()
	closed  uint32
}

// New creates a new stream.Type.
func New(conf Config, mgr bundle.NewManagement, opts ...func(*Type)) (*Type, error) {
	t := &Type{
		conf:    conf,
		manager: mgr,
		onClose: func() {},
		closed:  0,
	}
	for _, opt := range opts {
		opt(t)
	}
	if err := t.start(); err != nil {
		return nil, err
	}

	healthCheck := func(w http.ResponseWriter, r *http.Request) {
		inputConnected := t.inputLayer.Connected()
		outputConnected := t.outputLayer.Connected()

		if atomic.LoadUint32(&t.closed) == 1 {
			http.Error(w, "Stream terminated", http.StatusNotFound)
			return
		}

		if inputConnected && outputConnected {
			_, _ = w.Write([]byte("OK"))
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		if !inputConnected {
			_, _ = w.Write([]byte("input not connected\n"))
		}
		if !outputConnected {
			_, _ = w.Write([]byte("output not connected\n"))
		}
	}
	t.manager.RegisterEndpoint(
		"/ready",
		"Returns 200 OK if all inputs and outputs are connected, otherwise a 503 is returned.",
		healthCheck,
	)
	return t, nil
}

//------------------------------------------------------------------------------

// OptOnClose sets a closure to be called when the stream closes.
func OptOnClose(onClose func()) func(*Type) {
	return func(t *Type) {
		t.onClose = onClose
	}
}

//------------------------------------------------------------------------------

// IsReady returns a boolean indicating whether both the input and output layers
// of the stream are connected.
func (t *Type) IsReady() bool {
	return t.inputLayer.Connected() && t.outputLayer.Connected()
}

func (t *Type) start() (err error) {
	// Constructors
	iMgr := t.manager.IntoPath("input")
	if t.inputLayer, err = iMgr.NewInput(t.conf.Input); err != nil {
		return
	}
	if t.conf.Buffer.Type != "none" {
		bMgr := t.manager.IntoPath("buffer")
		if t.bufferLayer, err = bMgr.NewBuffer(t.conf.Buffer); err != nil {
			return
		}
	}
	if tLen := len(t.conf.Pipeline.Processors); tLen > 0 {
		pMgr := t.manager.IntoPath("pipeline")
		if t.pipelineLayer, err = pipeline.New(t.conf.Pipeline, pMgr); err != nil {
			return
		}
	}
	oMgr := t.manager.IntoPath("output")
	if t.outputLayer, err = oMgr.NewOutput(t.conf.Output); err != nil {
		return
	}

	// Start chaining components
	var nextTranChan <-chan message.Transaction

	nextTranChan = t.inputLayer.TransactionChan()
	if t.bufferLayer != nil {
		if err = t.bufferLayer.Consume(nextTranChan); err != nil {
			return
		}
		nextTranChan = t.bufferLayer.TransactionChan()
	}
	if t.pipelineLayer != nil {
		if err = t.pipelineLayer.Consume(nextTranChan); err != nil {
			return
		}
		nextTranChan = t.pipelineLayer.TransactionChan()
	}
	if err = t.outputLayer.Consume(nextTranChan); err != nil {
		return
	}

	go func(out output.Streamed) {
		for {
			if err := out.WaitForClose(context.Background()); err == nil {
				t.onClose()
				atomic.StoreUint32(&t.closed, 1)
				return
			}
		}
	}(t.outputLayer)

	return nil
}

// StopGracefully attempts to close the stream in the most graceful way by only
// closing the input layer and waiting for all other layers to terminate by
// proxy. This should guarantee that all in-flight and buffered data is resolved
// before shutting down.
func (t *Type) StopGracefully(ctx context.Context) (err error) {
	t.inputLayer.TriggerStopConsuming()
	if err = t.inputLayer.WaitForClose(ctx); err != nil {
		return
	}

	// If we have a buffer then wait right here. We want to try and allow the
	// buffer to empty out before prompting the other layers to shut down.
	if t.bufferLayer != nil {
		t.bufferLayer.TriggerStopConsuming()
		if err = t.bufferLayer.WaitForClose(ctx); err != nil {
			return
		}
	}

	// After this point we can start closing the remaining components.
	if t.pipelineLayer != nil {
		if err = t.pipelineLayer.WaitForClose(ctx); err != nil {
			return
		}
	}

	if err = t.outputLayer.WaitForClose(ctx); err != nil {
		return
	}
	return nil
}

// StopUnordered attempts to close all components in parallel without allowing
// the stream to gracefully wind down in the order of component layers. This
// should only be attempted if both stopGracefully and stopOrdered failed.
func (t *Type) StopUnordered(ctx context.Context) (err error) {
	t.inputLayer.TriggerCloseNow()
	if t.bufferLayer != nil {
		t.bufferLayer.TriggerCloseNow()
	}
	if t.pipelineLayer != nil {
		t.pipelineLayer.TriggerCloseNow()
	}
	t.outputLayer.TriggerCloseNow()

	if err = t.inputLayer.WaitForClose(ctx); err != nil {
		return
	}

	if t.bufferLayer != nil {
		if err = t.bufferLayer.WaitForClose(ctx); err != nil {
			return
		}
	}

	if t.pipelineLayer != nil {
		if err = t.pipelineLayer.WaitForClose(ctx); err != nil {
			return
		}
	}

	if err = t.outputLayer.WaitForClose(ctx); err != nil {
		return
	}
	return nil
}

// Stop attempts to close the stream within the specified timeout period.
// Initially the attempt is graceful, but if the context contains a deadline and
// it draws near the attempt becomes progressively less graceful.
//
// If the context is cancelled an error is returned _after_ asynchronously
// instructing the remaining stream components to terminate ungracefully.
func (t *Type) Stop(ctx context.Context) error {
	ctxCloseGraceful := ctx

	// If the provided context has a known deadline then we calculate a period
	// of time whereby it would be appropriate to abandon graceful termination
	// and attempt ungraceful termination within that deadline.
	if deadline, ok := ctx.Deadline(); ok {
		// The calculated time we're willing to wait for graceful termination is
		// three quarters of the overall deadline.
		tUntil := time.Until(deadline)
		tUntil -= (tUntil / 4)

		if tUntil > time.Second {
			var gDone func()
			ctxCloseGraceful, gDone = context.WithTimeout(ctx, tUntil)
			defer gDone()
		}
	}

	// Attempt graceful termination by instructing the input to stop consuming
	// and for all downstream components to finish.
	err := t.StopGracefully(ctxCloseGraceful)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		t.manager.Logger().Infoln("Unable to fully drain buffered messages within target time.")
	} else {
		t.manager.Logger().Errorf("Encountered error whilst shutting down: %v\n", err)
	}

	// If graceful termination failed then call unordered termination, if the
	// overall ctx is already cancelled this will still trigger asynchronous
	// clean up of resources, which is a best attempt.
	err = t.StopUnordered(ctx)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		t.manager.Logger().Errorln("Failed to stop stream gracefully within target time.")

		dumpBuf := bytes.NewBuffer(nil)
		_ = pprof.Lookup("goroutine").WriteTo(dumpBuf, 1)

		t.manager.Logger().Debugln(dumpBuf.String())
	} else {
		t.manager.Logger().Errorf("Encountered error whilst shutting down: %v\n", err)
	}

	return err
}
