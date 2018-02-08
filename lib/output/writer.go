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

package output

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// Writer is an output type that writes messages to a writer.Type.
type Writer struct {
	running int32

	typeStr string
	writer  writer.Type

	log   log.Modular
	stats metrics.Type

	messages     <-chan types.Message
	responseChan chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewWriter creates a new Writer output type.
func NewWriter(
	typeStr string,
	w writer.Type,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	return &Writer{
		running:      1,
		typeStr:      typeStr,
		writer:       w,
		log:          log.NewModule(".output." + typeStr),
		stats:        stats,
		messages:     nil,
		responseChan: make(chan types.Response),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}, nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (w *Writer) loop() {
	// Metrics paths
	var (
		runningPath    = "output." + w.typeStr + ".running"
		countPath      = "output." + w.typeStr + ".count"
		successPath    = "output." + w.typeStr + ".success"
		errorPath      = "output." + w.typeStr + ".error"
		connPath       = "output." + w.typeStr + ".connection.up"
		failedConnPath = "output." + w.typeStr + ".connection.failed"
		lostConnPath   = "output." + w.typeStr + ".connection.lost"
	)

	defer func() {
		err := w.writer.WaitForClose(time.Second)
		for ; err != nil; err = w.writer.WaitForClose(time.Second) {
		}
		w.stats.Decr(runningPath, 1)

		close(w.responseChan)
		close(w.closedChan)
	}()
	w.stats.Incr(runningPath, 1)

	for {
		if err := w.writer.Connect(); err != nil {
			// Close immediately if our writer is closed.
			if err == types.ErrTypeClosed {
				return
			}

			w.log.Errorf("Failed to connect to %v: %v\n", w.typeStr, err)
			w.stats.Incr(failedConnPath, 1)
			select {
			case <-time.After(time.Second):
			case <-w.closeChan:
				return
			}
		} else {
			break
		}
	}
	w.stats.Incr(connPath, 1)

	for atomic.LoadInt32(&w.running) == 1 {
		var msg types.Message
		var open bool
		select {
		case msg, open = <-w.messages:
			if !open {
				return
			}
			w.stats.Incr(countPath, 1)
		case <-w.closeChan:
			return
		}

		err := w.writer.Write(msg)

		// If our writer says it is not connected.
		if err == types.ErrNotConnected {
			w.stats.Incr(lostConnPath, 1)

			// Continue to try to reconnect while still active.
			for atomic.LoadInt32(&w.running) == 1 {
				if err = w.writer.Connect(); err != nil {
					// Close immediately if our writer is closed.
					if err == types.ErrTypeClosed {
						return
					}

					w.log.Errorf("Failed to reconnect to %v: %v\n", w.typeStr, err)
					w.stats.Incr(failedConnPath, 1)
					select {
					case <-time.After(time.Second):
					case <-w.closeChan:
						return
					}
				} else if err = w.writer.Write(msg); err != types.ErrNotConnected {
					w.stats.Incr(connPath, 1)
					break
				}
			}
		}

		// Close immediately if our writer is closed.
		if err == types.ErrTypeClosed {
			return
		}

		if err != nil {
			w.log.Errorf("Failed to send message to %v: %v\n", w.typeStr, err)
			w.stats.Incr(errorPath, 1)
		} else {
			w.stats.Incr(successPath, 1)
		}
		select {
		case w.responseChan <- types.NewSimpleResponse(err):
		case <-w.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (w *Writer) StartReceiving(msgs <-chan types.Message) error {
	if w.messages != nil {
		return types.ErrAlreadyStarted
	}
	w.messages = msgs
	go w.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (w *Writer) ResponseChan() <-chan types.Response {
	return w.responseChan
}

// CloseAsync shuts down the File output and stops processing messages.
func (w *Writer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&w.running, 1, 0) {
		w.writer.CloseAsync()
		close(w.closeChan)
	}
}

// WaitForClose blocks until the File output has closed down.
func (w *Writer) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
