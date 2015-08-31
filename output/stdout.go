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

package output

import (
	"fmt"
	"os"
	"time"

	"github.com/jeffail/benthos/message"
)

//--------------------------------------------------------------------------------------------------

// STDOUTConfig - Configuration for the STDOUT input type.
type STDOUTConfig struct {
}

// NewSTDOUTConfig - Creates a new STDOUTConfig with default values.
func NewSTDOUTConfig() STDOUTConfig {
	return STDOUTConfig{}
}

//--------------------------------------------------------------------------------------------------

// STDOUT - An input type that serves STDOUT POST requests.
type STDOUT struct {
	conf Config

	messages <-chan message.Type
	errs     chan error

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewSTDOUT - Create a new STDOUT input type.
func NewSTDOUT(conf Config, messages <-chan message.Type) *STDOUT {
	s := STDOUT{
		conf:       conf,
		messages:   messages,
		errs:       make(chan error),
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}

	go s.loop()

	return &s
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe.
func (s *STDOUT) loop() {
	running := true
	for running {
		select {
		case msg := <-s.messages:
			_, err := fmt.Fprintln(os.Stdout, string(msg.Content))
			s.errs <- err
		case _, running = <-s.closeChan:
			running = false
		}
	}

	close(s.errs)
	close(s.closedChan)
}

// ErrorChan - Returns the errors channel.
func (s *STDOUT) ErrorChan() <-chan error {
	return s.errs
}

// CloseAsync - Shuts down the STDOUT input and stops processing requests.
func (s *STDOUT) CloseAsync() {
	close(s.closeChan)
}

// WaitForClose - Blocks until the STDOUT input has closed down.
func (s *STDOUT) WaitForClose(timeout time.Duration) error {
	select {
	case <-s.closedChan:
	case <-time.After(timeout):
		return message.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
