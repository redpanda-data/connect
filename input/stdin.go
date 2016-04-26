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
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["stdin"] = NewSTDIN
}

//--------------------------------------------------------------------------------------------------

// STDIN - An input type that serves STDIN POST requests.
type STDIN struct {
	running int32

	conf Config
	log  log.Modular

	internalMessages chan []byte

	messages  chan types.Message
	responses <-chan types.Response

	closedChan chan struct{}

	sync.Mutex
}

// NewSTDIN - Create a new STDIN input type.
func NewSTDIN(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	s := STDIN{
		running:          1,
		conf:             conf,
		log:              log.NewModule(".input.stdin"),
		internalMessages: make(chan []byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closedChan:       make(chan struct{}),
	}

	go s.readLoop()

	return &s, nil
}

//--------------------------------------------------------------------------------------------------

// readLoop - Reads from stdin pipe and sends to internal messages chan.
func (s *STDIN) readLoop() {
	stdin := bufio.NewScanner(os.Stdin)

	var bytes []byte

	for atomic.LoadInt32(&s.running) == 1 {
		// If no bytes then read a line
		if bytes == nil {
			bytes = []byte{}
			for stdin.Scan() {
				if len(stdin.Bytes()) == 0 {
					break
				}
				bytes = append(bytes, stdin.Bytes()...)
			}
			if len(bytes) == 0 {
				bytes = nil
			}
		}

		// If we have a line to push out
		if bytes != nil {
			s.Lock()
			select {
			case s.internalMessages <- bytes:
				bytes = nil
			case <-time.After(time.Second):
			}
			s.Unlock()
		}
	}
}

// loop - Internal loop brokers incoming messages to output pipe.
func (s *STDIN) loop() {
	defer func() {
		close(s.messages)
		close(s.closedChan)
	}()

	var data []byte
	var open bool

	s.log.Infoln("Receiving messages through STDIN")

	for atomic.LoadInt32(&s.running) == 1 {
		if data == nil {
			data, open = <-s.internalMessages
			if !open {
				return
			}
		}
		if data != nil {
			s.messages <- types.Message{Parts: [][]byte{data}}

			var res types.Response
			res, open = <-s.responses
			if !open {
				atomic.StoreInt32(&s.running, 0)
			} else if res.Error() == nil {
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
	iMsgs := s.internalMessages
	s.Lock()
	s.internalMessages = nil
	close(iMsgs)
	s.Unlock()

	atomic.StoreInt32(&s.running, 0)
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
