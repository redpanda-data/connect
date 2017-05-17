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
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	nsq "github.com/nsqio/go-nsq"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["nsq"] = typeSpec{
		constructor: NewNSQ,
		description: `
Subscribe to an NSQ instance topic and channel.`,
	}
}

//--------------------------------------------------------------------------------------------------

// NSQConfig - Configuration for the NSQ input type.
type NSQConfig struct {
	Addresses       []string `json:"nsqd_tcp_addresses" yaml:"nsqd_tcp_addresses"`
	LookupAddresses []string `json:"lookupd_http_addresses" yaml:"lookupd_http_addresses"`
	Topic           string   `json:"topic" yaml:"topic"`
	Channel         string   `json:"channel" yaml:"channel"`
	UserAgent       string   `json:"user_agent" yaml:"user_agent"`
	MaxInFlight     int      `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewNSQConfig - Creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	return NSQConfig{
		Addresses:       []string{"127.0.0.1:4150"},
		LookupAddresses: []string{"127.0.0.1:4161"},
		Topic:           "benthos_messages",
		Channel:         "benthos_stream",
		UserAgent:       "benthos_consumer",
		MaxInFlight:     100,
	}
}

//--------------------------------------------------------------------------------------------------

// NSQ - An input type that receives NSQ messages.
type NSQ struct {
	running int32

	consumer *nsq.Consumer

	conf  Config
	stats metrics.Type
	log   log.Modular

	messages         chan types.Message
	responses        <-chan types.Response
	internalMessages chan *nsq.Message

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewNSQ - Create a new NSQ input type.
func NewNSQ(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NSQ{
		running:          1,
		conf:             conf,
		stats:            stats,
		log:              log.NewModule(".input.nsq"),
		messages:         make(chan types.Message),
		responses:        nil,
		internalMessages: make(chan *nsq.Message),
		closeChan:        make(chan struct{}),
		closedChan:       make(chan struct{}),
	}

	if err := n.connect(); err != nil {
		return nil, err
	}

	return &n, nil
}

//--------------------------------------------------------------------------------------------------

// HandleMessage - Handles an NSQ message.
func (n *NSQ) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	select {
	case n.internalMessages <- message:
	case <-n.closeChan:
		message.Requeue(-1)
		message.Finish()
	}
	return nil
}

//--------------------------------------------------------------------------------------------------

// connect - Establish a connection to an NSQ server.
func (n *NSQ) connect() (err error) {
	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.NSQ.UserAgent
	cfg.MaxInFlight = n.conf.NSQ.MaxInFlight

	if n.consumer, err = nsq.NewConsumer(n.conf.NSQ.Topic, n.conf.NSQ.Channel, cfg); err != nil {
		return
	}

	n.consumer.SetLogger(n.log, nsq.LogLevelWarning)
	n.consumer.AddHandler(n)

	if err = n.consumer.ConnectToNSQDs(n.conf.NSQ.Addresses); err != nil {
		return
	}
	if err = n.consumer.ConnectToNSQLookupds(n.conf.NSQ.LookupAddresses); err != nil {
		return
	}
	return
}

// disconnect - Safely close a connection to an NSQ server.
func (n *NSQ) disconnect() error {
	n.consumer.Stop()
	n.consumer = nil
	return nil
}

//--------------------------------------------------------------------------------------------------

func (n *NSQ) loop() {
	var msg *nsq.Message

	defer func() {
		if msg != nil {
			msg.Requeue(-1)
			msg.Finish()
			msg = nil
		}

		atomic.StoreInt32(&n.running, 0)
		n.disconnect()

		close(n.messages)
		close(n.closedChan)
	}()

	for atomic.LoadInt32(&n.running) == 1 {
		// If no bytes then read a message
		if msg == nil {
			select {
			case msg = <-n.internalMessages:
			case <-n.closeChan:
				return
			}
		}

		// If bytes are read then try and propagate.
		if msg != nil {
			select {
			case n.messages <- types.Message{Parts: [][]byte{msg.Body}}:
			case <-n.closeChan:
				return
			}
			res, open := <-n.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				n.stats.Incr("input.nsq.count", 1)
				msg.Finish()
				msg = nil
			} else if resErr == types.ErrMessageTooLarge {
				n.stats.Incr("input.nsq.send.rejected", 1)
				msg.Finish()
				msg = nil
			} else {
				n.stats.Incr("input.nsq.send.error", 1)
			}
		}
	}

}

// StartListening - Sets the channel used by the input to validate message receipt.
func (n *NSQ) StartListening(responses <-chan types.Response) error {
	if n.responses != nil {
		return types.ErrAlreadyStarted
	}
	n.responses = responses
	go n.loop()
	return nil
}

// MessageChan - Returns the messages channel.
func (n *NSQ) MessageChan() <-chan types.Message {
	return n.messages
}

// CloseAsync - Shuts down the NSQ input and stops processing requests.
func (n *NSQ) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose - Blocks until the NSQ input has closed down.
func (n *NSQ) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
