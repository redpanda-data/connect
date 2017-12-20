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
Publish to a NATS Stream subject. NATS Streaming is at-least-once and therefore
this output is able to guarantee delivery on success.`,
	}
}

//------------------------------------------------------------------------------

// NATSStreamConfig is configuration for the NATSStream output type.
type NATSStreamConfig struct {
	URL       string `json:"url" yaml:"url"`
	ClusterID string `json:"cluster_id" yaml:"cluster_id"`
	ClientID  string `json:"client_id" yaml:"client_id"`
	Subject   string `json:"subject" yaml:"subject"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URL:       stan.DefaultNatsURL,
		ClusterID: "benthos_cluster",
		ClientID:  "benthos_client",
		Subject:   "benthos_messages",
	}
}

//------------------------------------------------------------------------------

// NATSStream is an output type that serves NATS messages.
type NATSStream struct {
	running int32

	log   log.Modular
	stats metrics.Type

	natsConn stan.Conn

	conf Config

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNATSStream creates a new NATSStream output type.
func NewNATSStream(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATSStream{
		running:      1,
		log:          log.NewModule(".output.nats_stream"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
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

// loop is an internal loop that brokers incoming messages to output pipe.
func (n *NATSStream) loop() {
	defer func() {
		atomic.StoreInt32(&n.running, 0)

		n.natsConn.Close()

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
		n.stats.Incr("output.nats_stream.count", 1)
		var err error
		for _, part := range msg.Parts {
			err = n.natsConn.Publish(n.conf.NATSStream.Subject, part)
			if err != nil {
				n.stats.Incr("output.nats_stream.send.error", 1)
				break
			} else {
				n.stats.Incr("output.nats_stream.send.success", 1)
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
func (n *NATSStream) StartReceiving(msgs <-chan types.Message) error {
	if n.messages != nil {
		return types.ErrAlreadyStarted
	}
	n.messages = msgs
	go n.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (n *NATSStream) ResponseChan() <-chan types.Response {
	return n.responseChan
}

// CloseAsync shuts down the NATSStream output and stops processing messages.
func (n *NATSStream) CloseAsync() {
	if atomic.CompareAndSwapInt32(&n.running, 1, 0) {
		close(n.closeChan)
	}
}

// WaitForClose blocks until the NATSStream output has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	select {
	case <-n.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
