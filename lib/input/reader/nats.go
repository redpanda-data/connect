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
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	nats "github.com/nats-io/go-nats"
)

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
	urls  string
	conf  NATSConfig
	stats metrics.Type
	log   log.Modular

	cMut sync.Mutex

	natsConn      *nats.Conn
	natsSub       *nats.Subscription
	natsChan      chan *nats.Msg
	interruptChan chan struct{}
}

// NewNATS creates a new NATS input type.
func NewNATS(conf NATSConfig, log log.Modular, stats metrics.Type) (Type, error) {
	n := NATS{
		conf:          conf,
		stats:         stats,
		log:           log.NewModule(".input.nats"),
		interruptChan: make(chan struct{}),
	}
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to a NATS server.
func (n *NATS) Connect() error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var natsSub *nats.Subscription
	var err error

	if natsConn, err = nats.Connect(n.urls); err != nil {
		return err
	}
	natsChan := make(chan *nats.Msg)
	if natsSub, err = natsConn.ChanSubscribe(n.conf.Subject, natsChan); err != nil {
		return err
	}

	n.log.Infof("Receiving NATS messages from URLs: %s\n", n.urls)

	n.natsConn = natsConn
	n.natsSub = natsSub
	n.natsChan = natsChan
	return nil
}

func (n *NATS) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		n.natsSub.Unsubscribe()
		n.natsSub = nil
	}
	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	n.natsChan = nil
}

// Read attempts to read a new message from the NATS subject.
func (n *NATS) Read() (types.Message, error) {
	n.cMut.Lock()
	natsChan := n.natsChan
	n.cMut.Unlock()

	var msg *nats.Msg
	var open bool
	select {
	case msg, open = <-natsChan:
	case _, open = <-n.interruptChan:
	}
	if !open {
		n.disconnect()
		return nil, types.ErrNotConnected
	}
	return types.NewMessage([][]byte{msg.Data}), nil
}

// Acknowledge instructs whether read messages have been successfully
// propagated.
func (n *NATS) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the NATS input and stops processing requests.
func (n *NATS) CloseAsync() {
	close(n.interruptChan)
}

// WaitForClose blocks until the NATS input has closed down.
func (n *NATS) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
