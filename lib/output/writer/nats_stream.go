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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/stan.go"
)

//------------------------------------------------------------------------------

// NATSStreamConfig contains configuration fields for the NATSStream output
// type.
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
	log log.Modular

	natsConn stan.Conn
	connMut  sync.RWMutex

	urls string
	conf NATSStreamConfig
}

// NewNATSStream creates a new NATS Stream output type.
func NewNATSStream(conf NATSStreamConfig, log log.Modular, stats metrics.Type) (*NATSStream, error) {
	if len(conf.ClientID) == 0 {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := NATSStream{
		log:  log,
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to NATS servers.
func (n *NATSStream) Connect() error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var err error
	n.natsConn, err = stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsURL(n.urls),
	)
	if err == nil {
		n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	}
	return err
}

// Write attempts to write a message.
func (n *NATSStream) Write(msg types.Message) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		err := conn.Publish(n.conf.Subject, p.Get())
		if err == stan.ErrConnectionClosed {
			conn.Close()
			n.connMut.Lock()
			n.natsConn = nil
			n.connMut.Unlock()
			return types.ErrNotConnected
		}
		return err
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (n *NATSStream) CloseAsync() {
	n.connMut.Lock()
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	n.connMut.Unlock()
}

// WaitForClose blocks until the NATS output has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
