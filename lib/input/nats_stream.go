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
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/nats-io/go-nats-streaming"
)

//------------------------------------------------------------------------------

func init() {
	constructors["nats_stream"] = typeSpec{
		constructor: NewNATSStream,
		description: `
Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics. Tracking and persisting offsets through a durable name is also
optional and works with or without a queue. If a durable name is not provided
then subjects are consumed from the most recently published message.

The url can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

Comma separated arrays are also supported, e.g. urlA, urlB.`,
	}
}

//------------------------------------------------------------------------------

// NATSStreamConfig is configuration for the NATSStream input type.
type NATSStreamConfig struct {
	URL         string `json:"url" yaml:"url"`
	ClusterID   string `json:"cluster_id" yaml:"cluster_id"`
	ClientID    string `json:"client_id" yaml:"client_id"`
	QueueID     string `json:"queue" yaml:"queue"`
	DurableName string `json:"durable_name" yaml:"durable_name"`
	Subject     string `json:"subject" yaml:"subject"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URL:         stan.DefaultNatsURL,
		ClusterID:   "benthos_cluster",
		ClientID:    "benthos_client",
		QueueID:     "benthos_queue",
		DurableName: "benthos_offset",
		Subject:     "benthos_messages",
	}
}

//------------------------------------------------------------------------------

// NATSStream is an input type that receives NATSStream messages.
type NATSStream struct {
	running int32

	conf  Config
	stats metrics.Type
	log   log.Modular

	natsConn stan.Conn
	natsSub  stan.Subscription

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewNATSStream creates a new NATSStream input type.
func NewNATSStream(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATSStream{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.nats_stream"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	var err error
	if n.natsConn, err = stan.Connect(
		conf.NATSStream.ClusterID,
		conf.NATSStream.ClientID,
		stan.NatsURL(conf.NATSStream.URL),
	); err != nil {
		return nil, err
	}
	return &n, nil
}

//------------------------------------------------------------------------------

func (n *NATSStream) loop() {
	defer func() {
		atomic.StoreInt32(&n.running, 0)

		if n.natsSub != nil {
			n.natsSub.Unsubscribe()
			n.natsConn.Close()
		}

		close(n.messages)
		close(n.closedChan)
	}()

	handler := func(m *stan.Msg) {
		n.stats.Incr("input.nats_stream.count", 1)

		select {
		case n.messages <- types.Message{Parts: [][]byte{m.Data}}:
		case <-n.closeChan:
			return
		}

		var res types.Response
		select {
		case <-n.closeChan:
			return
		case res = <-n.responses:
		}

		if resErr := res.Error(); resErr == nil {
			n.stats.Incr("input.nats_stream.send.success", 1)
			m.Ack()
			return
		}
		n.stats.Incr("input.nats_stream.send.error", 1)
	}

	options := []stan.SubscriptionOption{
		stan.SetManualAckMode(),
	}
	if len(n.conf.NATSStream.DurableName) > 0 {
		options = append(options, stan.DurableName(n.conf.NATSStream.DurableName))
	} else {
		options = append(options, stan.StartWithLastReceived())
	}

	var err error
	if len(n.conf.NATSStream.QueueID) > 0 {
		if n.natsSub, err = n.natsConn.QueueSubscribe(
			n.conf.NATSStream.Subject,
			n.conf.NATSStream.QueueID,
			handler,
			options...,
		); err != nil {
			n.log.Errorf("Failed to connect to NATS Streaming: %v\n", err)
			return
		}
	} else {
		if n.natsSub, err = n.natsConn.Subscribe(
			n.conf.NATSStream.Subject,
			handler,
			options...,
		); err != nil {
			n.log.Errorf("Failed to connect to NATS Streaming: %v\n", err)
			return
		}
	}

	<-n.closeChan
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (n *NATSStream) StartListening(responses <-chan types.Response) error {
	if n.responses != nil {
		return types.ErrAlreadyStarted
	}
	n.responses = responses
	go n.loop()
	return nil
}

// MessageChan returns the messages channel.
func (n *NATSStream) MessageChan() <-chan types.Message {
	return n.messages
}

// CloseAsync shuts down the NATSStream input and stops processing requests.
func (n *NATSStream) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose blocks until the NATSStream input has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
