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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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

	handle io.WriteCloser

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewLineWriter creates a new LineWriter output type.
func NewLineWriter(
	handle io.WriteCloser,
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
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *LineWriter) loop() {
	// Metrics paths
	var (
		runningPath = [2]string{"output." + w.typeStr + ".running", "output.running"}
		countPath   = [2]string{"output." + w.typeStr + ".count", "output.count"}
		successPath = [2]string{"output." + w.typeStr + ".success", "output.success"}
		errorPath   = [2]string{"output." + w.typeStr + ".error", "output.error"}
	)

	defer func() {
		w.handle.Close()
		w.stats.Decr(runningPath[0], 1)
		w.stats.Decr(runningPath[1], 1)

		close(w.closedChan)
	}()
	w.stats.Incr(runningPath[0], 1)
	w.stats.Incr(runningPath[1], 1)

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
			w.stats.Incr(countPath[0], 1)
			w.stats.Incr(countPath[1], 1)
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
			w.stats.Incr(errorPath[0], 1)
			w.stats.Incr(errorPath[1], 1)
		} else {
			w.stats.Incr(successPath[0], 1)
			w.stats.Incr(successPath[1], 1)
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(err):
		case <-w.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (w *LineWriter) StartReceiving(ts <-chan types.Transaction) error {
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
