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
	"time"

	"github.com/jeffail/benthos/message"
)

//--------------------------------------------------------------------------------------------------

// STDINConfig - Configuration for the STDIN input type.
type STDINConfig struct {
	Address string `json:"address" yaml:"address"`
	Path    string `json:"path" yaml:"path"`
}

// NewSTDINConfig - Creates a new STDINConfig with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{}
}

//--------------------------------------------------------------------------------------------------

// STDIN - An input type that serves STDIN POST requests.
type STDIN struct {
	conf Config

	messages chan message.Type

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewSTDIN - Create a new STDIN input type.
func NewSTDIN(conf Config) *STDIN {
	s := STDIN{
		conf:       conf,
		messages:   make(chan message.Type),
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}

	go s.loop()

	return &s
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (s *STDIN) loop() {
	running := true
	for running {
		select {
		case _, running = <-s.closeChan:
		default:
		}
	}
	close(s.closedChan)
}

// ConsumerChan - Returns the messages channel.
func (s *STDIN) ConsumerChan() <-chan message.Type {
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
		return message.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
