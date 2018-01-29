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

package input

import (
	"bufio"
	"bytes"
	"io"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// LineReader is an input implementation that continuously reads line delimited
// messages from an io.Reader type.
type LineReader struct {
	running int32

	handleCtor func() (io.Reader, error)
	onClose    func()

	maxBuffer int
	multipart bool
	delimiter []byte

	typeStr string
	log     log.Modular
	stats   metrics.Type

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewLineReader creates a new reader input type.
//
// Callers must provide a constructor function for the target io.Reader, which
// is called on start up and again each time a reader is exhausted. If the
// constructor is called but there is no more content to create a Reader for
// then the error `io.EOF` should be returned and the LineReader will close.
//
// Callers must also provide an onClose function, which will be called if the
// LineReader has been instructed to shut down. This function should unblock any
// blocked Read calls.
func NewLineReader(
	typeStr string,
	handleCtor func() (io.Reader, error),
	onClose func(),
	log log.Modular,
	stats metrics.Type,
	options ...func(r *LineReader),
) (Type, error) {
	r := LineReader{
		running:          1,
		typeStr:          typeStr,
		handleCtor:       handleCtor,
		onClose:          onClose,
		maxBuffer:        bufio.MaxScanTokenSize,
		multipart:        false,
		delimiter:        []byte("\n"),
		log:              log.NewModule(".input." + typeStr),
		stats:            stats,
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	for _, opt := range options {
		opt(&r)
	}

	return &r, nil
}

//------------------------------------------------------------------------------

// OptLineReaderSetMaxBuffer is a option func that sets the maximum size of the
// line parsing buffers.
func OptLineReaderSetMaxBuffer(maxBuffer int) func(r *LineReader) {
	return func(r *LineReader) {
		r.maxBuffer = maxBuffer
	}
}

// OptLineReaderSetMultipart is a option func that sets the boolean flag
// indicating whether lines should be parsed as multipart or not.
func OptLineReaderSetMultipart(multipart bool) func(r *LineReader) {
	return func(r *LineReader) {
		r.multipart = multipart
	}
}

// OptLineReaderSetDelimiter is a option func that sets the delimiter (default
// '\n') used to divide lines (message parts) in the stream of data.
func OptLineReaderSetDelimiter(delimiter string) func(r *LineReader) {
	return func(r *LineReader) {
		r.delimiter = []byte(delimiter)
	}
}

//------------------------------------------------------------------------------

// readLoop reads from input pipe and sends to internal messages chan.
func (r *LineReader) readLoop() {
	var handle io.Reader
	var err error

	defer func() {
		close(r.internalMessages)
		if handle != nil {
			if closer, ok := handle.(io.ReadCloser); ok {
				closer.Close()
			}
		}
	}()

	var scanner *bufio.Scanner

	// We rotate two buffers as they save allocations by recycling the space but
	// it would not be safe to reset a single buffer since a message will
	// potentially still be in transit whilst we collect the next.
	buffer, bufferSpare := &bytes.Buffer{}, &bytes.Buffer{}
	bIndex := 0

	var partsToSend, multiParts [][]byte

	for atomic.LoadInt32(&r.running) == 1 {
		if handle == nil {
			if handle, err = r.handleCtor(); err != nil {
				// If we fail to construct a handle then we should close.
				// Failures that are periodic should be handled within the
				// constructor.
				if err != io.EOF {
					r.log.Errorf("Failed to construct read handle: %v\n", err)
				}
				return
			}

			scanner = bufio.NewScanner(handle)
			if r.maxBuffer != bufio.MaxScanTokenSize {
				scanner.Buffer([]byte{}, r.maxBuffer)
			}

			scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
				if atEOF && len(data) == 0 {
					return 0, nil, nil
				}

				if i := bytes.Index(data, r.delimiter); i >= 0 {
					// We have a full terminated line.
					return i + len(r.delimiter), data[0:i], nil
				}

				// If we're at EOF, we have a final, non-terminated line. Return it.
				if atEOF {
					return len(data), data, nil
				}

				// Request more data.
				return 0, nil, nil
			})
		}

		// If no bytes then read a line
		if len(partsToSend) == 0 && scanner != nil {
			if scanner.Scan() {
				var partSize int
				if partSize, err = buffer.Write(scanner.Bytes()); err != nil {
					r.log.Errorf("Failed to read input: %v\n", err)
				}
				if partSize > 0 {
					if r.multipart {
						multiParts = append(multiParts, buffer.Bytes()[bIndex:bIndex+partSize])
					} else {
						partsToSend = append(partsToSend, buffer.Bytes()[bIndex:bIndex+partSize])
					}
				} else if r.multipart {
					// Empty line means we're finished reading parts for this
					// message.
					partsToSend = multiParts
					multiParts = nil
				}
				bIndex += partSize
			} else {
				if err := scanner.Err(); err != nil {
					r.log.Errorf("Failed to read input: %v\n", err)
				}

				if closer, ok := handle.(io.ReadCloser); ok {
					closer.Close()
				}

				// Reset our io.Reader handle
				handle = nil
				scanner = nil

				// Remove any partial multipart reads we had when resetting an
				// io.Reader.
				multiParts = nil
				partsToSend = nil

				bIndex = 0
				buffer.Reset()
			}
		}

		// If we have a line to push out
		if len(partsToSend) != 0 {
			select {
			case r.internalMessages <- partsToSend:
				partsToSend = nil
			case <-time.After(time.Second):
			}

			// Rotate buffers and reset index
			bSwap := bufferSpare
			bufferSpare = buffer
			buffer = bSwap

			bIndex = 0
			buffer.Reset()
		}
	}
}

// loop is the internal loop that brokers incoming messages to output pipe.
func (r *LineReader) loop() {
	// Metrics paths
	var (
		runningPath = "input." + r.typeStr + ".running"
		countPath   = "input." + r.typeStr + ".count"
		successPath = "input." + r.typeStr + ".success"
		errorPath   = "input." + r.typeStr + ".error"
	)

	defer func() {
		atomic.StoreInt32(&r.running, 0)
		r.stats.Decr(runningPath, 1)
		close(r.messages)
		close(r.closedChan)
	}()
	r.stats.Incr(runningPath, 1)

	var data [][]byte
	var open bool

	readChan := r.internalMessages

	for atomic.LoadInt32(&r.running) == 1 {
		if data == nil {
			select {
			case data, open = <-readChan:
				if !open {
					return
				}
				r.stats.Incr(countPath, 1)
			case <-r.closeChan:
				return
			}
		}
		if data != nil {
			select {
			case r.messages <- types.Message{Parts: data}:
			case <-r.closeChan:
				return
			}

			var res types.Response
			if res, open = <-r.responses; !open {
				return
			}
			if res.Error() == nil {
				r.stats.Incr(successPath, 1)
				data = nil
			} else {
				r.stats.Incr(errorPath, 1)
			}
		}
	}
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (r *LineReader) StartListening(responses <-chan types.Response) error {
	if r.responses != nil {
		return types.ErrAlreadyStarted
	}
	r.responses = responses
	go r.readLoop()
	go r.loop()
	return nil
}

// MessageChan returns the messages channel.
func (r *LineReader) MessageChan() <-chan types.Message {
	return r.messages
}

// CloseAsync shuts down the reader input and stops processing requests.
func (r *LineReader) CloseAsync() {
	if atomic.CompareAndSwapInt32(&r.running, 1, 0) {
		close(r.closeChan)
		r.onClose()
	}
}

// WaitForClose blocks until the reader input has closed down.
func (r *LineReader) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
