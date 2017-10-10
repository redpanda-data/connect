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

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

// writer is an output type that pushes messages to a Writer type.
type writer struct {
	running int32

	conf  Config
	log   log.Modular
	stats metrics.Type

	messages     <-chan types.Message
	responseChan chan types.Response

	handle io.WriteCloser

	closeChan  chan struct{}
	closedChan chan struct{}
}

// newWriter creates a new writer output type.
func newWriter(handle io.WriteCloser, log log.Modular, stats metrics.Type) (Type, error) {
	return &writer{
		running:      1,
		log:          log.NewModule(".output.file"),
		stats:        stats,
		messages:     nil,
		responseChan: make(chan types.Response),
		handle:       handle,
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *writer) loop() {
	defer func() {
		w.handle.Close()

		close(w.responseChan)
		close(w.closedChan)
	}()

	for atomic.LoadInt32(&w.running) == 1 {
		var msg types.Message
		var open bool
		select {
		case msg, open = <-w.messages:
			if !open {
				return
			}
			w.stats.Incr("writer.message.received", 1)
		case <-w.closeChan:
			return
		}
		var err error
		if len(msg.Parts) == 1 {
			_, err = fmt.Fprintf(w.handle, "%s\n", msg.Parts[0])
		} else {
			_, err = fmt.Fprintf(w.handle, "%s\n\n", bytes.Join(msg.Parts, []byte("\n")))
		}
		select {
		case w.responseChan <- types.NewSimpleResponse(err):
			w.stats.Incr("writer.message.sent", 1)
		case <-w.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (w *writer) StartReceiving(msgs <-chan types.Message) error {
	if w.messages != nil {
		return types.ErrAlreadyStarted
	}
	w.messages = msgs
	go w.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (w *writer) ResponseChan() <-chan types.Response {
	return w.responseChan
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *writer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&w.running, 1, 0) {
		close(w.closeChan)
	}
}

// WaitForClose blocks until the File output has closed down.
func (w *writer) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
