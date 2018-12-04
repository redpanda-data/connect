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
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/message/metadata"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

// GCPPubSubConfig contains configuration values for the input type.
type GCPPubSubConfig struct {
	ProjectID              string `json:"project" yaml:"project"`
	SubscriptionID         string `json:"subscription" yaml:"subscription"`
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:              "",
		SubscriptionID:         "",
		MaxOutstandingMessages: pubsub.DefaultReceiveSettings.MaxOutstandingMessages,
		MaxOutstandingBytes:    pubsub.DefaultReceiveSettings.MaxOutstandingBytes,
	}
}

//------------------------------------------------------------------------------

// GCPPubSub is a benthos reader.Type implementation that reads messages from
// a GCP Cloud Pub/Sub subscription.
type GCPPubSub struct {
	conf GCPPubSubConfig

	subscription *pubsub.Subscription
	msgsChan     chan *pubsub.Message
	closeFunc    context.CancelFunc
	subMut       sync.Mutex

	client      *pubsub.Client
	pendingMsgs []*pubsub.Message

	log   log.Modular
	stats metrics.Type
}

// NewGCPPubSub creates a new GCP pubsub reader.Type.
func NewGCPPubSub(
	conf GCPPubSubConfig,
	log log.Modular,
	stats metrics.Type,
) (*GCPPubSub, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	client, err := pubsub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		return nil, err
	}
	return &GCPPubSub{
		conf:   conf,
		log:    log,
		stats:  stats,
		client: client,
	}, nil
}

// Connect attempts to establish a connection to the target subscription.
func (c *GCPPubSub) Connect() error {
	c.subMut.Lock()
	defer c.subMut.Unlock()
	if c.subscription != nil {
		return nil
	}

	sub := c.client.Subscription(c.conf.SubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = c.conf.MaxOutstandingMessages
	sub.ReceiveSettings.MaxOutstandingBytes = c.conf.MaxOutstandingBytes

	existsCtx, existsCancel := context.WithTimeout(context.Background(), time.Second*5)
	exists, err := sub.Exists(existsCtx)
	existsCancel()
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("subscription '%v' does not exist", c.conf.SubscriptionID)
	}

	subCtx, cancel := context.WithCancel(context.Background())
	msgsChan := make(chan *pubsub.Message)

	c.subscription = sub
	c.msgsChan = msgsChan
	c.closeFunc = cancel

	go func() {
		rerr := sub.Receive(subCtx, func(ctx context.Context, m *pubsub.Message) {
			select {
			case msgsChan <- m:
			case <-ctx.Done():
			}
		})
		if rerr != context.Canceled {
			c.log.Errorf("Subscription error: %v\n", rerr)
		}
		c.subMut.Lock()
		c.subscription = nil
		close(c.msgsChan)
		c.msgsChan = nil
		c.closeFunc = nil
		c.subMut.Unlock()
	}()

	c.log.Infof("Receiving GCP Cloud Pub/Sub messages from project '%v' and subscription '%v'\n", c.conf.ProjectID, c.conf.SubscriptionID)
	return nil
}

// Read attempts to read a new message from the target subscription.
func (c *GCPPubSub) Read() (types.Message, error) {
	c.subMut.Lock()
	msgsChan := c.msgsChan
	c.subMut.Unlock()
	if msgsChan == nil {
		return nil, types.ErrNotConnected
	}

	gmsg, open := <-msgsChan
	if !open {
		return nil, types.ErrNotConnected
	}
	c.pendingMsgs = append(c.pendingMsgs, gmsg)

	msg := message.New([][]byte{gmsg.Data})
	msg.Get(0).SetMetadata(metadata.New(gmsg.Attributes))
	return msg, nil
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (c *GCPPubSub) Acknowledge(err error) error {
	for _, msg := range c.pendingMsgs {
		if err == nil {
			msg.Ack()
		} else {
			msg.Nack()
		}
	}
	c.pendingMsgs = nil
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (c *GCPPubSub) CloseAsync() {
	c.subMut.Lock()
	if c.closeFunc != nil {
		c.closeFunc()
		c.closeFunc = nil
	}
	c.subMut.Unlock()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (c *GCPPubSub) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
