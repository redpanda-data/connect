// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// LineWriter is an output type that writes messages to an io.WriterCloser type
// as lines.
type LineWriter struct {
	running int32

	typeStr string
	log     log.Modular
	stats   metrics.Type

	customDelim []byte

	transactions <-chan types.Transaction

	handle      io.WriteCloser
	closeOnExit bool

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewLineWriter creates a new LineWriter output type.
func NewLineWriter(
	handle io.WriteCloser,
	closeOnExit bool,
	customDelimiter []byte,
	typeStr string,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	return &LineWriter{
		running:     1,
		typeStr:     typeStr,
		log:         log.NewModule(".output." + typeStr),
		stats:       stats,
		customDelim: customDelimiter,
		handle:      handle,
		closeOnExit: closeOnExit,
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *LineWriter) loop() {
	// Metrics paths
	var (
		mRunning  = w.stats.GetCounter("output.running")
		mRunningF = w.stats.GetCounter("output." + w.typeStr + ".running")
		mCount    = w.stats.GetCounter("output.count")
		mCountF   = w.stats.GetCounter("output." + w.typeStr + ".count")
		mSuccess  = w.stats.GetCounter("output.success")
		mSuccessF = w.stats.GetCounter("output." + w.typeStr + ".success")
		mError    = w.stats.GetCounter("output.error")
		mErrorF   = w.stats.GetCounter("output." + w.typeStr + ".error")
	)

	defer func() {
		if w.closeOnExit {
			w.handle.Close()
		}
		mRunning.Decr(1)
		mRunningF.Decr(1)

		close(w.closedChan)
	}()
	mRunning.Incr(1)
	mRunningF.Incr(1)

	delim := []byte("\n")
	if len(w.customDelim) > 0 {
		delim = w.customDelim
	}

	for atomic.LoadInt32(&w.running) == 1 {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-w.transactions:
			if !open {
				return
			}
			mCount.Incr(1)
			mCountF.Incr(1)
		case <-w.closeChan:
			return
		}
		var err error
		if ts.Payload.Len() == 1 {
			_, err = fmt.Fprintf(w.handle, "%s%s", ts.Payload.Get(0), delim)
		} else {
			_, err = fmt.Fprintf(w.handle, "%s%s%s", bytes.Join(ts.Payload.GetAll(), delim), delim, delim)
		}
		if err != nil {
			mError.Incr(1)
			mErrorF.Incr(1)
		} else {
			mSuccess.Incr(1)
			mSuccessF.Incr(1)
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(err):
		case <-w.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (w *LineWriter) Consume(ts <-chan types.Transaction) error {
	if w.transactions != nil {
		return types.ErrAlreadyStarted
	}
	w.transactions = ts
	go w.loop()
	return nil
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *LineWriter) CloseAsync() {
	if atomic.CompareAndSwapInt32(&w.running, 1, 0) {
		close(w.closeChan)
	}
}

// WaitForClose blocks until the File output has closed down.
func (w *LineWriter) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
