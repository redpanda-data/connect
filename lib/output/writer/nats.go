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
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	nats "github.com/nats-io/go-nats"
)

//------------------------------------------------------------------------------

// NATSConfig contains configuration fields for the NATS output type.
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
	log log.Modular

	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls string
	conf NATSConfig
}

// NewNATS creates a new NATS output type.
func NewNATS(conf NATSConfig, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATS{
		log:  log.NewModule(".output.nats"),
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to NATS servers.
func (n *NATS) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var err error
	n.natsConn, err = nats.Connect(n.urls)
	if err == nil {
		n.log.Infof("Sending NATS messages to URLs: %s\n", n.urls)
	}
	return err
}

// Write attempts to write a message.
func (n *NATS) Write(msg types.Message) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		return conn.Publish(n.conf.Subject, p.Get())
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (n *NATS) CloseAsync() {
	n.connMut.Lock()
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	n.connMut.Unlock()
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
