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
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// GCPPubSubConfig contains configuration values for the input type.
type GCPPubSubConfig struct {
	ProjectID              string `json:"project" yaml:"project"`
	SubscriptionID         string `json:"subscription" yaml:"subscription"`
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	// TODO: V4 Remove this.
	MaxBatchCount int                `json:"max_batch_count" yaml:"max_batch_count"`
	Batching      batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.Count = 1
	return GCPPubSubConfig{
		ProjectID:              "",
		SubscriptionID:         "",
		MaxOutstandingMessages: pubsub.DefaultReceiveSettings.MaxOutstandingMessages,
		MaxOutstandingBytes:    pubsub.DefaultReceiveSettings.MaxOutstandingBytes,
		MaxBatchCount:          1,
		Batching:               batchConf,
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
	return c.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target
// subscription.
func (c *GCPPubSub) ConnectWithContext(ignored context.Context) error {
	c.subMut.Lock()
	defer c.subMut.Unlock()
	if c.subscription != nil {
		return nil
	}

	sub := c.client.Subscription(c.conf.SubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = c.conf.MaxOutstandingMessages
	sub.ReceiveSettings.MaxOutstandingBytes = c.conf.MaxOutstandingBytes

	subCtx, cancel := context.WithCancel(context.Background())
	msgsChan := make(chan *pubsub.Message, c.conf.MaxBatchCount)

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

// ReadWithContext attempts to read a new message from the target subscription.
func (c *GCPPubSub) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	c.subMut.Lock()
	msgsChan := c.msgsChan
	c.subMut.Unlock()
	if msgsChan == nil {
		return nil, nil, types.ErrNotConnected
	}

	msg := message.New(nil)

	var gmsg *pubsub.Message
	var open bool
	select {
	case gmsg, open = <-msgsChan:
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	}
	if !open {
		return nil, nil, types.ErrNotConnected
	}

	part := message.NewPart(gmsg.Data)
	part.SetMetadata(metadata.New(gmsg.Attributes))
	part.Metadata().Set("gcp_pubsub_publish_time_unix", strconv.FormatInt(gmsg.PublishTime.Unix(), 10))
	msg.Append(part)

	return msg, func(ctx context.Context, res types.Response) error {
		if res.Error() != nil {
			gmsg.Nack()
		} else {
			gmsg.Ack()
		}
		return nil
	}, nil
}

// Read attempts to read a new message from the target subscription.
func (c *GCPPubSub) Read() (types.Message, error) {
	c.subMut.Lock()
	msgsChan := c.msgsChan
	c.subMut.Unlock()
	if msgsChan == nil {
		return nil, types.ErrNotConnected
	}

	msg := message.New(nil)

	gmsg, open := <-msgsChan
	if !open {
		return nil, types.ErrNotConnected
	}
	c.pendingMsgs = append(c.pendingMsgs, gmsg)
	part := message.NewPart(gmsg.Data)
	part.SetMetadata(metadata.New(gmsg.Attributes))
	part.Metadata().Set("gcp_pubsub_publish_time_unix", strconv.FormatInt(gmsg.PublishTime.Unix(), 10))
	msg.Append(part)

batchLoop:
	for msg.Len() < c.conf.MaxBatchCount {
		select {
		case gmsg, open = <-msgsChan:
		default:
			// Drained the buffer
			break batchLoop
		}
		if !open {
			return nil, types.ErrNotConnected
		}
		c.pendingMsgs = append(c.pendingMsgs, gmsg)
		part := message.NewPart(gmsg.Data)
		part.SetMetadata(metadata.New(gmsg.Attributes))
		part.Metadata().Set("gcp_pubsub_publish_time_unix", strconv.FormatInt(gmsg.PublishTime.Unix(), 10))
		msg.Append(part)
	}

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
