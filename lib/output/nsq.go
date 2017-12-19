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

package output

import (
	"io/ioutil"
	llog "log"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/log"
	"github.com/jeffail/benthos/lib/util/service/metrics"
	nsq "github.com/nsqio/go-nsq"
)

//------------------------------------------------------------------------------

func init() {
	constructors["nsq"] = typeSpec{
		constructor: NewNSQ,
		description: `
Publish to an NSQ topic.`,
	}
}

//------------------------------------------------------------------------------

// NSQConfig is configuration for the NSQ output type.
type NSQConfig struct {
	Address     string `json:"nsqd_tcp_address" yaml:"nsqd_tcp_address"`
	Topic       string `json:"topic" yaml:"topic"`
	UserAgent   string `json:"user_agent" yaml:"user_agent"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNSQConfig creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	return NSQConfig{
		Address:     "127.0.0.1:4150",
		Topic:       "benthos_messages",
		UserAgent:   "benthos_producer",
		MaxInFlight: 100,
	}
}

//------------------------------------------------------------------------------

// NSQ is an output type that serves NSQ messages.
type NSQ struct {
	running int32

	log   log.Modular
	stats metrics.Type

	conf Config

	producer *nsq.Producer

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNSQ creates a new NSQ output type.
func NewNSQ(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NSQ{
		running:      1,
		log:          log.NewModule(".output.nsq"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	if err := n.connect(); err != nil {
		return nil, err
	}

	return &n, nil
}

//------------------------------------------------------------------------------

// connect establishes a connection to an NSQ server.
func (n *NSQ) connect() (err error) {
	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.NSQ.UserAgent
	cfg.MaxInFlight = n.conf.NSQ.MaxInFlight

	if n.producer, err = nsq.NewProducer(n.conf.NSQ.Address, cfg); err != nil {
		return
	}

	n.producer.SetLogger(llog.New(ioutil.Discard, "", llog.Flags()), nsq.LogLevelError)

	err = n.producer.Ping()
	return
}

// disconnect safely closes a connection to an NSQ server.
func (n *NSQ) disconnect() error {
	n.producer.Stop()
	n.producer = nil
	return nil
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe.
func (n *NSQ) loop() {
	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.disconnect()

		close(n.responseChan)
		close(n.closedChan)
	}()

	var open bool
	for atomic.LoadInt32(&n.running) == 1 {
		var msg types.Message
		select {
		case msg, open = <-n.messages:
			if !open {
				return
			}
		case <-n.closeChan:
			return
		}
		n.stats.Incr("output.nsq.count", 1)
		var err error
		for _, part := range msg.Parts {
			err = n.producer.Publish(n.conf.NSQ.Topic, part)
			if err != nil {
				n.stats.Incr("output.nsq.send.error", 1)
				break
			} else {
				n.stats.Incr("output.nsq.send.success", 1)
			}
		}
		select {
		case n.responseChan <- types.NewSimpleResponse(err):
		case <-n.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (n *NSQ) StartReceiving(msgs <-chan types.Message) error {
	if n.messages != nil {
		return types.ErrAlreadyStarted
	}
	n.messages = msgs
	go n.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (n *NSQ) ResponseChan() <-chan types.Response {
	return n.responseChan
}

// CloseAsync shuts down the NSQ output and stops processing messages.
func (n *NSQ) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose blocks until the NSQ output has closed down.
func (n *NSQ) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
