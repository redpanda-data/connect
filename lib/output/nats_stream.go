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
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/nats-io/go-nats-streaming"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["nats_stream"] = TypeSpec{
		constructor: NewNATSStream,
		description: `
Publish to a NATS Stream subject. NATS Streaming is at-least-once and therefore
this output is able to guarantee delivery on success.`,
	}
}

//------------------------------------------------------------------------------

// NATSStreamConfig is configuration for the NATSStream output type.
type NATSStreamConfig struct {
	URLs      []string `json:"urls" yaml:"urls"`
	ClusterID string   `json:"cluster_id" yaml:"cluster_id"`
	ClientID  string   `json:"client_id" yaml:"client_id"`
	Subject   string   `json:"subject" yaml:"subject"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:      []string{stan.DefaultNatsURL},
		ClusterID: "test-cluster",
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

	urls string
	conf Config

	transactions <-chan types.Transaction

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewNATSStream creates a new NATSStream output type.
func NewNATSStream(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.NATSStream.ClientID) == 0 {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.NATSStream.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := NATSStream{
		running:    1,
		log:        log.NewModule(".output.nats_stream"),
		stats:      stats,
		conf:       conf,
		closedChan: make(chan struct{}),
		closeChan:  make(chan struct{}),
	}
	n.urls = strings.Join(conf.NATSStream.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

func (n *NATSStream) connect() error {
	var err error
	n.natsConn, err = stan.Connect(
		n.conf.NATSStream.ClusterID,
		n.conf.NATSStream.ClientID,
		stan.NatsURL(n.urls),
	)
	return err
}

// loop is an internal loop that brokers incoming messages to output pipe.
func (n *NATSStream) loop() {
	var (
		mRunning = n.stats.GetCounter("output.nats_stream.running")
		mCount   = n.stats.GetCounter("output.nats_stream.count")
		mErr     = n.stats.GetCounter("output.nats_stream.send.error")
		mSucc    = n.stats.GetCounter("output.nats_stream.send.success")
	)

	defer func() {
		atomic.StoreInt32(&n.running, 0)

		if n.natsConn != nil {
			n.natsConn.Close()
			n.natsConn = nil
		}
		mRunning.Decr(1)

		close(n.closedChan)
	}()
	mRunning.Incr(1)

	for {
		if err := n.connect(); err != nil {
			n.log.Errorf("Failed to connect to NATS Streaming: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-n.closeChan:
				return
			}
		} else {
			break
		}
	}
	n.log.Infof("Sending NATS Streaming messages to URLs: %s\n", n.urls)

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
			err = n.natsConn.Publish(n.conf.NATSStream.Subject, part)
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
func (n *NATSStream) Consume(ts <-chan types.Transaction) error {
	if n.transactions != nil {
		return types.ErrAlreadyStarted
	}
	n.transactions = ts
	go n.loop()
	return nil
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
