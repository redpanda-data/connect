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

package writer

import (
	"io/ioutil"
	llog "log"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	nsq "github.com/nsqio/go-nsq"
)

//------------------------------------------------------------------------------

// NSQConfig contains configuration fields for the NSQ output type.
type NSQConfig struct {
	Address   string `json:"nsqd_tcp_address" yaml:"nsqd_tcp_address"`
	Topic     string `json:"topic" yaml:"topic"`
	UserAgent string `json:"user_agent" yaml:"user_agent"`
}

// NewNSQConfig creates a new NSQConfig with default values.
func NewNSQConfig() NSQConfig {
	return NSQConfig{
		Address:   "localhost:4150",
		Topic:     "benthos_messages",
		UserAgent: "benthos_producer",
	}
}

//------------------------------------------------------------------------------

// NSQ is an output type that serves NSQ messages.
type NSQ struct {
	log log.Modular

	topicStr *text.InterpolatedString

	connMut  sync.RWMutex
	producer *nsq.Producer

	conf NSQConfig
}

// NewNSQ creates a new NSQ output type.
func NewNSQ(conf NSQConfig, log log.Modular, stats metrics.Type) (*NSQ, error) {
	n := NSQ{
		log:      log,
		conf:     conf,
		topicStr: text.NewInterpolatedString(conf.Topic),
	}
	return &n, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to NSQ servers.
func (n *NSQ) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	cfg := nsq.NewConfig()
	cfg.UserAgent = n.conf.UserAgent

	producer, err := nsq.NewProducer(n.conf.Address, cfg)
	if err != nil {
		return err
	}

	producer.SetLogger(llog.New(ioutil.Discard, "", llog.Flags()), nsq.LogLevelError)

	if err = producer.Ping(); err != nil {
		return err
	}
	n.producer = producer
	n.log.Infof("Sending NSQ messages to address: %s\n", n.conf.Address)
	return nil
}

// Write attempts to write a message.
func (n *NSQ) Write(msg types.Message) error {
	n.connMut.RLock()
	prod := n.producer
	n.connMut.RUnlock()

	if prod == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		return prod.Publish(n.topicStr.Get(message.Lock(msg, i)), p.Get())
	})
}

// CloseAsync shuts down the NSQ output and stops processing messages.
func (n *NSQ) CloseAsync() {
	n.connMut.Lock()
	if n.producer != nil {
		n.producer.Stop()
		n.producer = nil
	}
	n.connMut.Unlock()
}

// WaitForClose blocks until the NSQ output has closed down.
func (n *NSQ) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
