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
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	nats "github.com/nats-io/go-nats"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["nats"] = typeSpec{
		constructor: NewNATS,
		description: `
Publish to an NATS subject.`,
	}
}

//--------------------------------------------------------------------------------------------------

// NATSConfig - Configuration for the NATS output type.
type NATSConfig struct {
	URL     string `json:"url" yaml:"url"`
	Subject string `json:"subject" yaml:"subject"`
}

// NewNATSConfig - Creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URL:     nats.DefaultURL,
		Subject: "benthos_messages",
	}
}

//--------------------------------------------------------------------------------------------------

// NATS - An output type that serves NATS messages.
type NATS struct {
	running int32

	log   log.Modular
	stats metrics.Type

	natsConn *nats.Conn

	conf Config

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNATS - Create a new NATS output type.
func NewNATS(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATS{
		running:      1,
		log:          log.NewModule(".output.nats"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	var err error
	if n.natsConn, err = nats.Connect(conf.NATS.URL); err != nil {
		return nil, err
	}
	return &n, nil
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe, does not use select.
func (n *NATS) loop() {
	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.natsConn.Close()

		close(n.responseChan)
		close(n.closedChan)
	}()

	var open bool
	for atomic.LoadInt32(&n.running) == 1 {
		var msg types.Message
		if msg, open = <-n.messages; !open {
			return
		}
		var err error
		switch len(msg.Parts) {
		case 0:
		case 1:
			err = n.natsConn.Publish(n.conf.NATS.Subject, msg.Parts[0])
		default:
			err = n.natsConn.Publish(n.conf.NATS.Subject, msg.Bytes())
		}
		select {
		case n.responseChan <- types.NewSimpleResponse(err):
		case <-n.closeChan:
			return
		}
	}
}

// StartReceiving - Assigns a messages channel for the output to read.
func (n *NATS) StartReceiving(msgs <-chan types.Message) error {
	if n.messages != nil {
		return types.ErrAlreadyStarted
	}
	n.messages = msgs
	go n.loop()
	return nil
}

// ResponseChan - Returns the errors channel.
func (n *NATS) ResponseChan() <-chan types.Response {
	return n.responseChan
}

// CloseAsync - Shuts down the NATS output and stops processing messages.
func (n *NATS) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose - Blocks until the NATS output has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
