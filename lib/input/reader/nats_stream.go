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
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/gofrs/uuid"
	"github.com/nats-io/go-nats-streaming"
)

//------------------------------------------------------------------------------

// NATSStreamConfig contains configuration fields for the NATSStream input type.
type NATSStreamConfig struct {
	URLs            []string `json:"urls" yaml:"urls"`
	ClusterID       string   `json:"cluster_id" yaml:"cluster_id"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	QueueID         string   `json:"queue" yaml:"queue"`
	DurableName     string   `json:"durable_name" yaml:"durable_name"`
	UnsubOnClose    bool     `json:"unsubscribe_on_close" yaml:"unsubscribe_on_close"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
	Subject         string   `json:"subject" yaml:"subject"`
	MaxInflight     int      `json:"max_inflight" yaml:"max_inflight"`
}

// NewNATSStreamConfig creates a new NATSStreamConfig with default values.
func NewNATSStreamConfig() NATSStreamConfig {
	return NATSStreamConfig{
		URLs:        []string{stan.DefaultNatsURL},
		ClusterID:   "test-cluster",
		ClientID:    "benthos_client",
		QueueID:     "benthos_queue",
		DurableName: "benthos_offset",
		// TODO: V2 Make this false by default
		UnsubOnClose:    true,
		StartFromOldest: true,
		Subject:         "benthos_messages",
		MaxInflight:     1024,
	}
}

//------------------------------------------------------------------------------

// NATSStream is an input type that receives NATSStream messages.
type NATSStream struct {
	urls  string
	conf  NATSStreamConfig
	stats metrics.Type
	log   log.Modular

	unAckMsgs []*stan.Msg

	natsConn stan.Conn
	natsSub  stan.Subscription
	cMut     sync.Mutex

	msgChan       chan *stan.Msg
	interruptChan chan struct{}
}

// NewNATSStream creates a new NATSStream input type.
func NewNATSStream(conf NATSStreamConfig, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.ClientID) == 0 {
		u4, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		conf.ClientID = u4.String()
	}
	n := NATSStream{
		conf:          conf,
		stats:         stats,
		log:           log,
		msgChan:       make(chan *stan.Msg),
		interruptChan: make(chan struct{}),
	}
	close(n.msgChan)
	n.urls = strings.Join(conf.URLs, ",")

	return &n, nil
}

//------------------------------------------------------------------------------

func (n *NATSStream) disconnect() {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		if n.conf.UnsubOnClose {
			n.natsSub.Unsubscribe()
		}
		n.natsConn.Close()

		n.natsSub = nil
		n.natsConn = nil
	}
}

// Connect attempts to establish a connection to a NATS streaming server.
func (n *NATSStream) Connect() error {
	n.cMut.Lock()
	defer n.cMut.Unlock()

	if n.natsSub != nil {
		return nil
	}

	newMsgChan := make(chan *stan.Msg)
	handler := func(m *stan.Msg) {
		select {
		case newMsgChan <- m:
		case <-n.interruptChan:
			n.disconnect()
		}
	}
	dcHandler := func() {
		if newMsgChan == nil {
			return
		}
		close(newMsgChan)
		newMsgChan = nil
		n.disconnect()
	}

	natsConn, err := stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsURL(n.urls),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			n.log.Errorf("Connection lost: %v", reason)
			dcHandler()
		}),
	)
	if err != nil {
		return err
	}

	options := []stan.SubscriptionOption{
		stan.SetManualAckMode(),
	}
	if len(n.conf.DurableName) > 0 {
		options = append(options, stan.DurableName(n.conf.DurableName))
	}
	if n.conf.StartFromOldest {
		options = append(options, stan.DeliverAllAvailable())
	} else {
		options = append(options, stan.StartWithLastReceived())
	}
	if n.conf.MaxInflight != 0 {
		options = append(options, stan.MaxInflight(n.conf.MaxInflight))
	}

	var natsSub stan.Subscription
	if len(n.conf.QueueID) > 0 {
		natsSub, err = natsConn.QueueSubscribe(
			n.conf.Subject,
			n.conf.QueueID,
			handler,
			options...,
		)
	} else {
		natsSub, err = natsConn.Subscribe(
			n.conf.Subject,
			handler,
			options...,
		)
	}
	if err != nil {
		natsConn.Close()
		return err
	}

	n.natsConn = natsConn
	n.natsSub = natsSub
	n.msgChan = newMsgChan
	n.log.Infof("Receiving NATS Streaming messages from subject: %v\n", n.conf.Subject)
	return nil
}

// Read attempts to read a new message from the NATS streaming server.
func (n *NATSStream) Read() (types.Message, error) {
	var msg *stan.Msg
	var open bool
	select {
	case msg, open = <-n.msgChan:
		if !open {
			return nil, types.ErrNotConnected
		}
		n.unAckMsgs = append(n.unAckMsgs, msg)
	case <-n.interruptChan:
		n.unAckMsgs = nil
		n.disconnect()
		return nil, types.ErrTypeClosed
	}
	bmsg := message.New([][]byte{msg.Data})
	bmsg.Get(0).Metadata().Set("nats_stream_subject", msg.Subject)

	return bmsg, nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (n *NATSStream) Acknowledge(err error) error {
	if err == nil {
		for _, m := range n.unAckMsgs {
			m.Ack()
		}
	}
	n.unAckMsgs = nil
	return nil
}

// CloseAsync shuts down the NATSStream input and stops processing requests.
func (n *NATSStream) CloseAsync() {
	close(n.interruptChan)
}

// WaitForClose blocks until the NATSStream input has closed down.
func (n *NATSStream) WaitForClose(timeout time.Duration) error {
	n.disconnect()
	return nil
}

//------------------------------------------------------------------------------
