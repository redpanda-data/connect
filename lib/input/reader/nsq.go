// Copyright (c) 2018 Ashley Jeffs
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

package reader

import (
	"context"
	"io/ioutil"
	llog "log"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	nsq "github.com/nsqio/go-nsq"
)

//------------------------------------------------------------------------------

// NSQConfig contains configuration fields for the NSQ input type.
type NSQConfig struct {
	Addresses       []string           `json:"nsqd_tcp_addresses" yaml:"nsqd_tcp_addresses"`
	LookupAddresses []string           `json:"lookupd_http_addresses" yaml:"lookupd_http_addresses"`
	Topic           string             `json:"topic" yaml:"topic"`
	Channel         string             `json:"channel" yaml:"channel"`
	UserAgent       string             `json:"user_agent" yaml:"user_agent"`
	MaxInFlight     int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewNSQConfig creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	batching := batch.NewPolicyConfig()
	batching.Count = 1
	return NSQConfig{
		Addresses:       []string{"localhost:4150"},
		LookupAddresses: []string{"localhost:4161"},
		Topic:           "benthos_messages",
		Channel:         "benthos_stream",
		UserAgent:       "benthos_consumer",
		MaxInFlight:     100,
		Batching:        batching,
	}
}

//------------------------------------------------------------------------------

// NSQ is an input type that receives NSQ messages.
type NSQ struct {
	consumer *nsq.Consumer
	cMut     sync.Mutex

	unAckMsgs []*nsq.Message

	addresses       []string
	lookupAddresses []string
	conf            NSQConfig
	stats           metrics.Type
	log             log.Modular

	internalMessages chan *nsq.Message
	interruptChan    chan struct{}
}

// NewNSQ creates a new NSQ input type.
func NewNSQ(conf NSQConfig, log log.Modular, stats metrics.Type) (*NSQ, error) {
	n := NSQ{
		conf:             conf,
		stats:            stats,
		log:              log,
		internalMessages: make(chan *nsq.Message),
		interruptChan:    make(chan struct{}),
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				n.addresses = append(n.addresses, splitAddr)
			}
		}
	}
	for _, addr := range conf.LookupAddresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				n.lookupAddresses = append(n.lookupAddresses, splitAddr)
			}
		}
	}

	return &n, nil
}

//------------------------------------------------------------------------------

// HandleMessage handles an NSQ message.
func (n *NSQ) HandleMessage(message *nsq.Message) error {
	message.DisableAutoResponse()
	select {
	case n.internalMessages <- message:
	case <-n.interruptChan:
		message.Requeue(-1)
		message.Finish()
	}
	return nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an NSQ server.
func (n *NSQ) Connect() (err error) {
	return n.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to an NSQ server.
func (n *NSQ) ConnectWithContext(ctx context.Context) (err error) {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.consumer != nil {
		return nil
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.UserAgent
	cfg.MaxInFlight = n.conf.MaxInFlight

	var consumer *nsq.Consumer
	if consumer, err = nsq.NewConsumer(n.conf.Topic, n.conf.Channel, cfg); err != nil {
		return
	}

	consumer.SetLogger(llog.New(ioutil.Discard, "", llog.Flags()), nsq.LogLevelError)
	consumer.AddHandler(n)

	if err = consumer.ConnectToNSQDs(n.addresses); err != nil {
		consumer.Stop()
		return
	}
	if err = consumer.ConnectToNSQLookupds(n.lookupAddresses); err != nil {
		consumer.Stop()
		return
	}

	n.consumer = consumer
	n.log.Infof("Receiving NSQ messages from addresses: %s\n", n.addresses)
	return
}

// disconnect safely closes a connection to an NSQ server.
func (n *NSQ) disconnect() error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.consumer != nil {
		n.consumer.Stop()
		n.consumer = nil
	}
	return nil
}

//------------------------------------------------------------------------------

func (n *NSQ) read(ctx context.Context) (*nsq.Message, error) {
	var msg *nsq.Message
	select {
	case msg = <-n.internalMessages:
		return msg, nil
	case <-ctx.Done():
	case <-n.interruptChan:
		for _, m := range n.unAckMsgs {
			m.Requeue(-1)
			m.Finish()
		}
		n.unAckMsgs = nil
		n.disconnect()
		return nil, types.ErrTypeClosed
	}
	return nil, types.ErrTimeout
}

// ReadWithContext attempts to read a new message from NSQ.
func (n *NSQ) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	msg, err := n.read(ctx)
	if err != nil {
		return nil, nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)
	return message.New([][]byte{msg.Body}), func(rctx context.Context, res types.Response) error {
		if res.Error() != nil {
			msg.Requeue(-1)
		}
		msg.Finish()
		return nil
	}, nil
}

// Read attempts to read a new message from NSQ.
func (n *NSQ) Read() (types.Message, error) {
	msg, err := n.read(context.Background())
	if err != nil {
		return nil, err
	}
	n.unAckMsgs = append(n.unAckMsgs, msg)
	return message.New([][]byte{msg.Body}), nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (n *NSQ) Acknowledge(err error) error {
	if err != nil {
		for _, m := range n.unAckMsgs {
			m.Requeue(-1)
		}
	}
	for _, m := range n.unAckMsgs {
		m.Finish()
	}
	n.unAckMsgs = nil
	return nil
}

// CloseAsync shuts down the NSQ input and stops processing requests.
func (n *NSQ) CloseAsync() {
	close(n.interruptChan)
}

// WaitForClose blocks until the NSQ input has closed down.
func (n *NSQ) WaitForClose(timeout time.Duration) error {
	n.disconnect()
	return nil
}

//------------------------------------------------------------------------------
