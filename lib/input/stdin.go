/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package input

import (
	"bufio"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["stdin"] = typeSpec{
		constructor: NewSTDIN,
		description: `
The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.`,
	}
}

//--------------------------------------------------------------------------------------------------

// STDINConfig - contains config fields for the STDIN input type.
type STDINConfig struct {
	Multipart bool `json:"multipart" yaml:"multipart"`
	MaxBuffer int  `json:"max_buffer" yaml:"max_buffer"`
}

// NewSTDINConfig - creates a STDINConfig populated with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{
		Multipart: false,
		MaxBuffer: bufio.MaxScanTokenSize,
	}
}

//--------------------------------------------------------------------------------------------------

// STDIN - An input type that reads lines from STDIN.
type STDIN struct {
	running int32

	handle io.Reader

	conf  Config
	log   log.Modular
	stats metrics.Type

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewSTDIN - Create a new STDIN input type.
func NewSTDIN(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	s := STDIN{
		running:          1,
		handle:           os.Stdin,
		conf:             conf,
		log:              log.NewModule(".input.stdin"),
		stats:            stats,
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	go s.readLoop()

	return &s, nil
}

//--------------------------------------------------------------------------------------------------

// readLoop - Reads from stdin pipe and sends to internal messages chan.
func (s *STDIN) readLoop() {
	defer func() {
		close(s.internalMessages)
	}()
	stdin := bufio.NewScanner(s.handle)
	if s.conf.STDIN.MaxBuffer != bufio.MaxScanTokenSize {
		stdin.Buffer([]byte{}, s.conf.STDIN.MaxBuffer)
	}

	var partsToSend, parts [][]byte

	for atomic.LoadInt32(&s.running) == 1 {
		// If no bytes then read a line
		if len(partsToSend) == 0 {
			if stdin.Scan() {
				newPart := make([]byte, len(stdin.Bytes()))
				copy(newPart, stdin.Bytes())
				if len(newPart) > 0 {
					if s.conf.STDIN.Multipart {
						parts = append(parts, newPart)
					} else {
						partsToSend = append(partsToSend, newPart)
					}
				} else if s.conf.STDIN.Multipart {
					// Empty line means we're finished reading parts for this message.
					partsToSend = parts
					parts = nil
				}
			} else {
				return
			}
		}

		// If we have a line to push out
		if len(partsToSend) != 0 {
			select {
			case s.internalMessages <- partsToSend:
				s.stats.Incr("stdin.message.sent", 1)
				partsToSend = nil
			case <-time.After(time.Second):
			}
		}
	}
}

// loop - Internal loop brokers incoming messages to output pipe.
func (s *STDIN) loop() {
	defer func() {
		atomic.StoreInt32(&s.running, 0)
		close(s.messages)
		close(s.closedChan)
	}()

	var data [][]byte
	var open bool

	readChan := s.internalMessages

	s.log.Infoln("Receiving messages through STDIN")

	for atomic.LoadInt32(&s.running) == 1 {
		if data == nil {
			select {
			case data, open = <-readChan:
				if !open {
					return
				}
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
				data = nil
			}
		}
	}
}

// StartListening - Sets the channel used by the input to validate message receipt.
func (s *STDIN) StartListening(responses <-chan types.Response) error {
	if s.responses != nil {
		return types.ErrAlreadyStarted
	}
	s.responses = responses
	go s.loop()
	return nil
}

// MessageChan - Returns the messages channel.
func (s *STDIN) MessageChan() <-chan types.Message {
	return s.messages
}

// CloseAsync - Shuts down the STDIN input and stops processing requests.
func (s *STDIN) CloseAsync() {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closeChan)
	}
}

// WaitForClose - Blocks until the STDIN input has closed down.
func (s *STDIN) WaitForClose(timeout time.Duration) error {
	select {
	case <-s.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
