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

package input

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	nats "github.com/nats-io/go-nats"
)

//------------------------------------------------------------------------------

func init() {
	constructors["nats"] = typeSpec{
		constructor: NewNATS,
		description: `
Subscribe to a NATS subject. NATS is at-most-once, if you need at-least-once
behaviour then look at NATS Stream.

The urls can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222`,
	}
}

//------------------------------------------------------------------------------

// NATSConfig is configuration for the NATS input type.
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

// NATS is an input type that receives NATS messages.
type NATS struct {
	running int32

	urls  string
	conf  Config
	stats metrics.Type
	log   log.Modular

	natsConn *nats.Conn
	natsSub  *nats.Subscription
	natsChan chan *nats.Msg

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewNATS creates a new NATS input type.
func NewNATS(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATS{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.nats"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}
	n.urls = strings.Join(conf.NATS.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

func (n *NATS) connect() error {
	var err error
	if n.natsConn == nil {
		if n.natsConn, err = nats.Connect(n.urls); err != nil {
			return err
		}
	}
	if n.natsSub == nil {
		n.natsChan = make(chan *nats.Msg)
		if n.natsSub, err = n.natsConn.ChanSubscribe(n.conf.NATS.Subject, n.natsChan); err != nil {
			return err
		}
	}
	return nil
}

func (n *NATS) disconnect() {
	if n.natsSub != nil {
		n.natsSub.Unsubscribe()
		n.natsSub = nil
	}
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
}

func (n *NATS) loop() {
	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.disconnect()
		n.stats.Decr("input.nats.running", 1)

		close(n.messages)
		close(n.closedChan)
	}()
	n.stats.Incr("input.nats.running", 1)

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
	n.log.Infof("Receiving NATS messages from URLs: %s\n", n.urls)

	var msg *nats.Msg

	for atomic.LoadInt32(&n.running) == 1 {
		// If bytes are read then try and propagate.
		if msg != nil {
			select {
			case n.messages <- types.Message{Parts: [][]byte{msg.Data}}:
			case <-n.closeChan:
				return
			}
			res, open := <-n.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				n.stats.Incr("input.nats.send.success", 1)
				msg = nil
			} else {
				n.stats.Incr("input.nats.send.error", 1)
			}
		} else {
			var open bool
			select {
			case msg, open = <-n.natsChan:
				if !open {
					n.disconnect()
					n.stats.Incr("input.nats.reconnect.count", 1)
					if err := n.connect(); err != nil {
						n.log.Errorf("Lost connection to NATS and failed to reconnect: %v\n", err)
						n.stats.Incr("input.nats.reconnect.error", 1)
						select {
						case <-time.After(time.Second):
						case <-n.closeChan:
							return
						}
					} else {

						n.stats.Incr("input.nats.reconnect.success", 1)
					}
				} else {
					n.stats.Incr("input.nats.count", 1)
				}
			case <-n.closeChan:
				return
			}
		}
	}
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (n *NATS) StartListening(responses <-chan types.Response) error {
	if n.responses != nil {
		return types.ErrAlreadyStarted
	}
	n.responses = responses
	go n.loop()
	return nil
}

// MessageChan returns the messages channel.
func (n *NATS) MessageChan() <-chan types.Message {
	return n.messages
}

// CloseAsync shuts down the NATS input and stops processing requests.
func (n *NATS) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose blocks until the NATS input has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
