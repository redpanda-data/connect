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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	nsq "github.com/nsqio/go-nsq"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["nsq"] = TypeSpec{
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
		Address:     "localhost:4150",
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

	transactions <-chan types.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNSQ creates a new NSQ output type.
func NewNSQ(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	n := NSQ{
		running:    1,
		log:        log.NewModule(".output.nsq"),
		stats:      stats,
		conf:       conf,
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
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
	var (
		mRunning  = n.stats.GetCounter("output.nsq.running")
		mCount    = n.stats.GetCounter("output.nsq.count")
		mSendErr  = n.stats.GetCounter("output.nsq.send.error")
		mSendSucc = n.stats.GetCounter("output.nsq.send.success")
	)

	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.disconnect()
		mRunning.Decr(1)

		close(n.closedChan)
	}()
	mRunning.Incr(1)

	for {
		if err := n.connect(); err != nil {
			n.log.Errorf("Failed to connect to NSQ: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-n.closeChan:
				return
			}
		} else {
			break
		}
	}
	n.log.Infof("Sending NSQ messages to address: %s\n", n.conf.NSQ.Address)

	var open bool
	for atomic.LoadInt32(&n.running) == 1 {
		var ts types.Transaction
		select {
		case ts, open = <-n.transactions:
			if !open {
				return
			}
		case <-n.closeChan:
			return
		}
		mCount.Incr(1)
		var err error
		for _, part := range ts.Payload.GetAll() {
			err = n.producer.Publish(n.conf.NSQ.Topic, part)
			if err != nil {
				mSendErr.Incr(1)
				break
			} else {
				mSendSucc.Incr(1)
			}
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(err):
		case <-n.closeChan:
			return
		}
	}
}

// Consume assigns a messages channel for the output to read.
func (n *NSQ) Consume(ts <-chan types.Transaction) error {
	if n.transactions != nil {
		return types.ErrAlreadyStarted
	}
	n.transactions = ts
	go n.loop()
	return nil
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
