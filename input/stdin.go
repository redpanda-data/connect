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
	"os"
	"time"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// STDINConfig - Configuration for the STDIN input type.
type STDINConfig struct {
}

// NewSTDINConfig - Creates a new STDINConfig with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{}
}

//--------------------------------------------------------------------------------------------------

// STDIN - An input type that serves STDIN POST requests.
type STDIN struct {
	conf Config

	messages  chan types.Message
	responses <-chan types.Response

	newResponsesChan chan (<-chan types.Response)

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewSTDIN - Create a new STDIN input type.
func NewSTDIN(conf Config) *STDIN {
	s := STDIN{
		conf:             conf,
		messages:         make(chan types.Message),
		responses:        nil,
		newResponsesChan: make(chan (<-chan types.Response)),
		closedChan:       make(chan struct{}),
		closeChan:        make(chan struct{}),
	}

	go s.loop()

	return &s
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (s *STDIN) loop() {
	stdin := bufio.NewScanner(os.Stdin)

	var bytes [][]byte
	var msgChan chan<- types.Message

	running, responsePending := true, false
	for running {
		// If no bytes then read a line
		if bytes == nil {
			bytes = make([][]byte, 0)
			for stdin.Scan() {
				if len(stdin.Bytes()) == 0 {
					break
				}
				bytes = append(bytes, stdin.Bytes())
			}
			if len(bytes) == 0 {
				bytes = nil
			}
		}

		// If we have a line to push out
		if bytes != nil && !responsePending {
			msgChan = s.messages
		} else {
			msgChan = nil
		}

		if running {
			select {
			case msgChan <- types.Message{Parts: bytes}:
				responsePending = true
			case err, open := <-s.responses:
				if !open {
					s.responses = nil
				} else if err == nil {
					responsePending = false
					bytes = nil
				}
			case newResChan, open := <-s.newResponsesChan:
				if running = open; open {
					s.responses = newResChan
				}
			case _, running = <-s.closeChan:
			}
		}
	}

	close(s.messages)
	close(s.newResponsesChan)
	close(s.closedChan)
}

// SetResponseChan - Sets the channel used by the input to validate message receipt.
func (s *STDIN) SetResponseChan(responses <-chan types.Response) {
	s.newResponsesChan <- responses
}

// ConsumerChan - Returns the messages channel.
func (s *STDIN) ConsumerChan() <-chan types.Message {
	return s.messages
}

// CloseAsync - Shuts down the STDIN input and stops processing requests.
func (s *STDIN) CloseAsync() {
	close(s.closeChan)
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
