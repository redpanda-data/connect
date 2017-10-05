// Copyright (c) 2017 Ashley Jeffs
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

package pipeline

import (
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

// MessageProcessor is a type that reads a message, performs a processing
// operation, and returns a message and a flag indicating whether that message
// should be propagated or not.
type MessageProcessor interface {
	// ProcessMessage returns a message to be sent onwards, if the bool flag is
	// false then the message should be dropped.
	ProcessMessage(msg *types.Message) (*types.Message, bool)
}

//------------------------------------------------------------------------------

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	running int32

	log   log.Modular
	stats metrics.Type

	msgProcessor MessageProcessor

	messagesOut  chan types.Message
	responsesOut chan types.Response

	messagesIn  <-chan types.Message
	responsesIn <-chan types.Response

	closeChan chan struct{}
	closed    chan struct{}
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(
	msgProcessor MessageProcessor,
	log log.Modular,
	stats metrics.Type,
) *Processor {
	return &Processor{
		running:      1,
		msgProcessor: msgProcessor,
		log:          log.NewModule(".pipeline.processor"),
		stats:        stats,
		messagesOut:  make(chan types.Message),
		responsesOut: make(chan types.Response),
		closeChan:    make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Processor) loop() {
	defer func() {
		atomic.StoreInt32(&p.running, 0)

		close(p.responsesOut)
		close(p.messagesOut)
		close(p.closed)
	}()

	var open bool
	for atomic.LoadInt32(&p.running) == 1 {
		var msg types.Message
		if msg, open = <-p.messagesIn; !open {
			return
		}
		p.stats.Incr("pipeline.processor.message.received", 1)

		result, sending := p.msgProcessor.ProcessMessage(&msg)
		if !sending {
			p.stats.Incr("pipeline.processor.message.dropped", 1)
			select {
			case p.responsesOut <- types.NewSimpleResponse(nil):
			case <-p.closeChan:
				return
			}
			continue
		}

		select {
		case p.messagesOut <- *result:
		case <-p.closeChan:
			return
		}
		var res types.Response
		if res, open = <-p.responsesIn; !open {
			return
		}
		if res.Error() == nil {
			p.stats.Incr("pipeline.processor.message.sent", 1)
		}
		select {
		case p.responsesOut <- res:
		case <-p.closeChan:
			return
		}
	}
}

//------------------------------------------------------------------------------

// StartReceiving assigns a messages channel for the pipeline to read.
func (p *Processor) StartReceiving(msgs <-chan types.Message) error {
	if p.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	if p.responsesIn != nil {
		go p.loop()
	}
	return nil
}

// MessageChan returns the channel used for consuming messages from this
// pipeline.
func (p *Processor) MessageChan() <-chan types.Message {
	return p.messagesOut
}

// StartListening sets the channel that this pipeline will read responses from.
func (p *Processor) StartListening(responses <-chan types.Response) error {
	if p.responsesIn != nil {
		return types.ErrAlreadyStarted
	}
	p.responsesIn = responses
	if p.messagesIn != nil {
		go p.loop()
	}
	return nil
}

// ResponseChan returns the response channel from this pipeline.
func (p *Processor) ResponseChan() <-chan types.Response {
	return p.responsesOut
}

// CloseAsync shuts down the pipeline and stops processing messages.
func (p *Processor) CloseAsync() {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closeChan)
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (p *Processor) WaitForClose(timeout time.Duration) error {
	select {
	case <-p.closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
