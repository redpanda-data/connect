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
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	nats "github.com/nats-io/go-nats"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["nats"] = TypeSpec{
		constructor: NewNATS,
		description: `
Publish to an NATS subject. NATS is at-most-once, so delivery is not guaranteed.
For at-least-once behaviour with NATS look at NATS Stream.`,
	}
}

//------------------------------------------------------------------------------

// NATSConfig is configuration for the NATS output type.
type NATSConfig struct {
	URLs    []string `json:"urls" yaml:"urls"`
	Subject string   `json:"subject" yaml:"subject"`
}

// NewNATSConfig creates a new NATSConfig with default values.
func NewNATSConfig() NATSConfig {
	return NATSConfig{
		URLs:    []string{nats.DefaultURL},
		Subject: "benthos_messages",
	}
}

//------------------------------------------------------------------------------

// NATS is an output type that serves NATS messages.
type NATS struct {
	running int32

	log   log.Modular
	stats metrics.Type

	natsConn *nats.Conn

	urls string
	conf Config

	transactions <-chan types.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNATS creates a new NATS output type.
func NewNATS(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATS{
		running:    1,
		log:        log.NewModule(".output.nats"),
		stats:      stats,
		conf:       conf,
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}
	n.urls = strings.Join(conf.NATS.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

func (n *NATS) connect() error {
	var err error
	n.natsConn, err = nats.Connect(n.urls)
	return err
}

// loop is an internal loop that brokers incoming messages to output pipe.
func (n *NATS) loop() {
	var (
		mRunning = n.stats.GetCounter("output.nats.running")
		mCount   = n.stats.GetCounter("output.nats.count")
		mErr     = n.stats.GetCounter("output.nats.send.error")
		mSucc    = n.stats.GetCounter("output.nats.send.success")
	)

	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.natsConn.Close()
		mRunning.Decr(1)

		close(n.closedChan)
	}()
	mRunning.Incr(1)

	for {
		if err := n.connect(); err != nil {
			n.log.Errorf("Failed to connect to NATS: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-n.closeChan:
				return
			}
		} else {
			break
		}
	}
	n.log.Infof("Sending NATS messages to URLs: %s\n", n.urls)

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
			err = n.natsConn.Publish(n.conf.NATS.Subject, part)
			if err != nil {
				mErr.Incr(1)
				break
			} else {
				mSucc.Incr(1)
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
func (n *NATS) Consume(ts <-chan types.Transaction) error {
	if n.transactions != nil {
		return types.ErrAlreadyStarted
	}
	n.transactions = ts
	go n.loop()
	return nil
}

// CloseAsync shuts down the NATS output and stops processing messages.
func (n *NATS) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
