// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package stream

import (
	"bytes"
	"net/http"
	"runtime/pprof"
	"time"

	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Type creates and manages the lifetime of a Benthos stream.
type Type struct {
	conf Config

	inputLayer    input.Type
	bufferLayer   buffer.Type
	pipelineLayer pipeline.Type
	outputLayer   output.Type

	complementaryProcs []types.ProcessorConstructorFunc

	manager types.Manager
	stats   metrics.Type
	logger  log.Modular

	onClose func()
}

// New creates a new stream.Type.
func New(conf Config, opts ...func(*Type)) (*Type, error) {
	t := &Type{
		conf:    conf,
		stats:   metrics.Noop(),
		logger:  log.Noop(),
		manager: types.NoopMgr(),
		onClose: func() {},
	}
	for _, opt := range opts {
		opt(t)
	}
	if err := t.start(); err != nil {
		return nil, err
	}

	healthCheck := func(w http.ResponseWriter, r *http.Request) {
		connected := true
		if !t.inputLayer.Connected() {
			connected = false
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("input not connected\n"))
		}
		if !t.outputLayer.Connected() {
			connected = false
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("output not connected\n"))
		}
		if connected {
			w.Write([]byte("OK"))
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

// OptAddProcessors adds additional processors that will be constructed for each
// logical thread of the processing pipeline layer of the Benthos stream.
func OptAddProcessors(procs ...types.ProcessorConstructorFunc) func(*Type) {
	return func(t *Type) {
		t.complementaryProcs = append(t.complementaryProcs, procs...)
	}
}

// OptSetStats sets the metrics aggregator to be used by all components of the
// stream.
func OptSetStats(stats metrics.Type) func(*Type) {
	return func(t *Type) {
		t.stats = stats
	}
}

// OptSetLogger sets the logging output to be used by all components of the
// stream. To avoid implementing the log.Modular interface with a custom logger
// consider using OptSetLogSimple instead.
func OptSetLogger(l log.Modular) func(*Type) {
	return func(t *Type) {
		t.logger = l
	}
}

// OptSetLogSimple sets the logging output to a simpler log interface
// (implemented by the standard *log.Logger.)
func OptSetLogSimple(l log.PrintFormatter) func(*Type) {
	return func(t *Type) {
		t.logger = log.Wrap(l)
	}
}

// OptSetManager sets the service manager to be used by all components of the
// stream.
func OptSetManager(mgr types.Manager) func(*Type) {
	return func(t *Type) {
		t.manager = mgr
	}
}

// OptOnClose sets a closure to be called when the stream closes.
func OptOnClose(onClose func()) func(*Type) {
	return func(t *Type) {
		t.onClose = onClose
	}
}

//------------------------------------------------------------------------------

func (t *Type) start() (err error) {
	// Constructors
	if t.inputLayer, err = input.New(
		t.conf.Input, t.manager,
		t.logger.NewModule(".input"), metrics.Namespaced(t.stats, "input"),
	); err != nil {
		return
	}
	if t.conf.Buffer.Type != buffer.TypeNone {
		if t.bufferLayer, err = buffer.New(
			t.conf.Buffer, t.manager,
			t.logger.NewModule(".buffer"), metrics.Namespaced(t.stats, "buffer"),
		); err != nil {
			return
		}
	}
	if tLen := len(t.complementaryProcs) + len(t.conf.Pipeline.Processors); tLen > 0 {
		if t.pipelineLayer, err = pipeline.New(
			t.conf.Pipeline, t.manager,
			t.logger.NewModule(".pipeline"), metrics.Namespaced(t.stats, "pipeline"),
			t.complementaryProcs...,
		); err != nil {
			return
		}
	}
	if t.outputLayer, err = output.New(
		t.conf.Output, t.manager,
		t.logger.NewModule(".output"), metrics.Namespaced(t.stats, "output"),
	); err != nil {
		return
	}

	// Start chaining components
	var nextTranChan <-chan types.Transaction

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

	go func(out output.Type) {
		for {
			if err := out.WaitForClose(time.Second); err == nil {
				t.onClose()
				return
			}
		}
	}(t.outputLayer)

	return nil
}

// stopGracefully attempts to close the stream in the most graceful way by only
// closing the input layer and waiting for all other layers to terminate by
// proxy. This should guarantee that all in-flight and buffered data is resolved
// before shutting down.
func (t *Type) stopGracefully(timeout time.Duration) (err error) {
	t.inputLayer.CloseAsync()
	started := time.Now()
	if err = t.inputLayer.WaitForClose(timeout); err != nil {
		return
	}

	var remaining time.Duration

	// If we have a buffer then wait right here. We want to try and allow the
	// buffer to empty out before prompting the other layers to shut down.
	if t.bufferLayer != nil {
		t.bufferLayer.StopConsuming()
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.bufferLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	// After this point we can start closing the remaining components.
	if t.pipelineLayer != nil {
		t.pipelineLayer.CloseAsync()
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.pipelineLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	t.outputLayer.CloseAsync()
	remaining = timeout - time.Since(started)
	if remaining < 0 {
		return types.ErrTimeout
	}
	if err = t.outputLayer.WaitForClose(remaining); err != nil {
		return
	}

	return nil
}

// stopOrdered attempts to close all components of the stream in the order of
// positions within the stream, this allows data to flush all the way through
// the pipeline under certain circumstances but is less graceful than
// stopGracefully, which should be attempted first.
func (t *Type) stopOrdered(timeout time.Duration) (err error) {
	t.inputLayer.CloseAsync()
	started := time.Now()
	if err = t.inputLayer.WaitForClose(timeout); err != nil {
		return
	}

	var remaining time.Duration

	if t.bufferLayer != nil {
		t.bufferLayer.CloseAsync()
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.bufferLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	if t.pipelineLayer != nil {
		t.pipelineLayer.CloseAsync()
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.pipelineLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	t.outputLayer.CloseAsync()
	remaining = timeout - time.Since(started)
	if remaining < 0 {
		return types.ErrTimeout
	}
	if err = t.outputLayer.WaitForClose(remaining); err != nil {
		return
	}

	return nil
}

// stopUnorderd attempts to close all components in parallel without allowing
// the stream to gracefully wind down in the order of component layers. This
// should only be attempted if both stopGracefully and stopOrdered failed.
func (t *Type) stopUnordered(timeout time.Duration) (err error) {
	t.inputLayer.CloseAsync()
	if t.bufferLayer != nil {
		t.bufferLayer.CloseAsync()
	}
	if t.pipelineLayer != nil {
		t.pipelineLayer.CloseAsync()
	}
	t.outputLayer.CloseAsync()

	started := time.Now()
	if err = t.inputLayer.WaitForClose(timeout); err != nil {
		return
	}

	var remaining time.Duration

	if t.bufferLayer != nil {
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.bufferLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	if t.pipelineLayer != nil {
		remaining = timeout - time.Since(started)
		if remaining < 0 {
			return types.ErrTimeout
		}
		if err = t.pipelineLayer.WaitForClose(remaining); err != nil {
			return
		}
	}

	remaining = timeout - time.Since(started)
	if remaining < 0 {
		return types.ErrTimeout
	}
	if err = t.outputLayer.WaitForClose(remaining); err != nil {
		return
	}

	return nil
}

// Stop attempts to close the stream within the specified timeout period.
// Initially the attempt is graceful, but as the timeout draws close the attempt
// becomes progressively less graceful.
func (t *Type) Stop(timeout time.Duration) error {
	tOutUnordered := timeout / 4
	tOutGraceful := timeout - tOutUnordered

	err := t.stopGracefully(tOutGraceful)
	if err == nil {
		return nil
	}
	if err == types.ErrTimeout {
		t.logger.Infoln("Unable to fully drain buffered messages within target time.")
	} else {
		t.logger.Errorf("Encountered error whilst shutting down: %v\n", err)
	}

	err = t.stopUnordered(tOutUnordered)
	if err == nil {
		return nil
	}
	if err == types.ErrTimeout {
		t.logger.Errorln("Failed to stop stream gracefully within target time.")

		dumpBuf := bytes.NewBuffer(nil)
		pprof.Lookup("goroutine").WriteTo(dumpBuf, 1)

		t.logger.Debugln(dumpBuf.String())
	} else {
		t.logger.Errorf("Encountered error whilst shutting down: %v\n", err)
	}

	return err
}

//------------------------------------------------------------------------------
