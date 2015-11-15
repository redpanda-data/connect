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

package buffer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// File - An agent that wraps an output with a message buffer.
type File struct {
	running int32

	messagesIn   <-chan types.Message
	messagesOut  chan types.Message
	responsesIn  <-chan types.Response
	responsesOut chan types.Response
	errorsChan   chan []error

	closedWG sync.WaitGroup
}

// NewFile - Create a new buffered agent type.
func NewFile(directory string) *File {
	f := File{
		running:      1,
		messagesOut:  make(chan types.Message),
		responsesOut: make(chan types.Response),
		errorsChan:   make(chan []error),
	}

	return &f
}

//--------------------------------------------------------------------------------------------------

// inputLoop - Internal loop brokers incoming messages to output pipe.
func (f *File) inputLoop() {
	defer close(f.responsesOut)
	defer f.closedWG.Done()

	for atomic.LoadInt32(&f.running) == 1 {
		msg, open := <-f.messagesIn
		if !open {
			return
		}
		_ = msg
		f.responsesOut <- types.NewSimpleResponse(nil)
	}
}

// outputLoop - Internal loop brokers incoming messages to output pipe.
func (f *File) outputLoop() {
	defer close(f.errorsChan)
	defer close(f.messagesOut)
	defer f.closedWG.Done()

	var errMap map[error]struct{}
	var errs []error

	for atomic.LoadInt32(&f.running) == 1 {
		f.messagesOut <- types.Message{}
		res, open := <-f.responsesIn
		if !open {
			return
		}
		if res.Error() != nil {
			if _, exists := errMap[res.Error()]; !exists {
				errMap[res.Error()] = struct{}{}
				errs = append(errs, res.Error())
			}
		}

		// If we have errors built up.
		if len(errs) > 0 {
			select {
			case f.errorsChan <- errs:
				errMap = map[error]struct{}{}
				errs = []error{}
			default:
				// Reader not ready, do not block here.
			}
		}
	}
}

// StartReceiving - Assigns a messages channel for the output to read.
func (f *File) StartReceiving(msgs <-chan types.Message) error {
	if f.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	f.messagesIn = msgs

	if f.responsesIn != nil {
		f.closedWG.Add(2)
		go f.inputLoop()
		go f.outputLoop()
	}
	return nil
}

// MessageChan - Returns the channel used for consuming messages from this input.
func (f *File) MessageChan() <-chan types.Message {
	return f.messagesOut
}

// StartListening - Sets the channel for reading responses.
func (f *File) StartListening(responses <-chan types.Response) error {
	if f.responsesIn != nil {
		return types.ErrAlreadyStarted
	}
	f.responsesIn = responses

	if f.messagesIn != nil {
		f.closedWG.Add(2)
		go f.inputLoop()
		go f.outputLoop()
	}
	return nil
}

// ResponseChan - Returns the response channel.
func (f *File) ResponseChan() <-chan types.Response {
	return f.responsesOut
}

// ErrorsChan - Returns the errors channel.
func (f *File) ErrorsChan() <-chan []error {
	return f.errorsChan
}

// CloseAsync - Shuts down the File output and stops processing messages.
func (f *File) CloseAsync() {
	atomic.StoreInt32(&f.running, 0)
}

// WaitForClose - Blocks until the File output has closed down.
func (f *File) WaitForClose(timeout time.Duration) error {
	closed := make(chan struct{})
	go func() {
		f.closedWG.Wait()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
