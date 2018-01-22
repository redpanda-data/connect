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

// reader is an input type that reads messages from an io.Reader type.
type reader struct {
	running int32

	handle io.Reader

	maxBuffer   int
	multipart   bool
	customDelim []byte

	log   log.Modular
	stats metrics.Type

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// newReader creates a new reader input type.
func newReader(
	handle io.Reader,
	maxBuffer int,
	multipart bool,
	customDelim []byte,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	s := reader{
		running:          1,
		handle:           handle,
		maxBuffer:        maxBuffer,
		multipart:        multipart,
		customDelim:      customDelim,
		log:              log.NewModule(".input.reader"),
		stats:            stats,
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	return &s, nil
}

//------------------------------------------------------------------------------

// readLoop reads from input pipe and sends to internal messages chan.
func (s *reader) readLoop() {
	defer func() {
		close(s.internalMessages)
		if closer, ok := s.handle.(io.ReadCloser); ok {
			closer.Close()
		}
	}()
	scanner := bufio.NewScanner(s.handle)
	if s.maxBuffer != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, s.maxBuffer)
	}
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i := bytes.Index(data, s.customDelim); i >= 0 {
			// We have a full terminated line.
			return i + len(s.customDelim), data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}

		// Request more data.
		return 0, nil, nil
	})

	var partsToSend, parts [][]byte

	for atomic.LoadInt32(&s.running) == 1 {
		// If no bytes then read a line
		if len(partsToSend) == 0 {
			if scanner.Scan() {
				newPart := make([]byte, len(scanner.Bytes()))
				copy(newPart, scanner.Bytes())
				if len(newPart) > 0 {
					if s.multipart {
						parts = append(parts, newPart)
					} else {
						partsToSend = append(partsToSend, newPart)
					}
				} else if s.multipart {
					// Empty line means we're finished reading parts for this
					// message.
					partsToSend = parts
					parts = nil
				}
			} else {
				if err := scanner.Err(); err != nil {
					s.log.Errorf("Failed to read input: %v\n", err)
				}
				return
			}
		}

		// If we have a line to push out
		if len(partsToSend) != 0 {
			select {
			case s.internalMessages <- partsToSend:
				partsToSend = nil
			case <-time.After(time.Second):
			}
		}
	}
}

// loop is the internal loop that brokers incoming messages to output pipe.
func (s *reader) loop() {
	defer func() {
		atomic.StoreInt32(&s.running, 0)
		s.stats.Decr("input.reader.running", 1)
		close(s.messages)
		close(s.closedChan)
	}()
	s.stats.Incr("input.reader.running", 1)

	var data [][]byte
	var open bool

	readChan := s.internalMessages

	for atomic.LoadInt32(&s.running) == 1 {
		if data == nil {
			select {
			case data, open = <-readChan:
				if !open {
					return
				}
				s.stats.Incr("input.reader.count", 1)
			case <-s.closeChan:
				return
			}
		}
		if data != nil {
			select {
			case s.messages <- types.Message{Parts: data}:
			case <-s.closeChan:
				return
			}

			var res types.Response
			if res, open = <-s.responses; !open {
				return
			}
			if res.Error() == nil {
				s.stats.Incr("input.reader.send.success", 1)
				data = nil
			} else {
				s.stats.Incr("input.reader.send.error", 1)
			}
		}
	}
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (s *reader) StartListening(responses <-chan types.Response) error {
	if s.responses != nil {
		return types.ErrAlreadyStarted
	}
	s.responses = responses
	go s.readLoop()
	go s.loop()
	return nil
}

// MessageChan returns the messages channel.
func (s *reader) MessageChan() <-chan types.Message {
	return s.messages
}

// CloseAsync shuts down the reader input and stops processing requests.
func (s *reader) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closeChan)
	}
}

// WaitForClose blocks until the reader input has closed down.
func (s *reader) WaitForClose(timeout time.Duration) error {
	select {
	case <-s.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
