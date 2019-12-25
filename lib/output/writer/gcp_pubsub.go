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
// OUT OF OR IN CONNETION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// GCPPubSubConfig contains configuration fields for the output GCPPubSub type.
type GCPPubSubConfig struct {
	ProjectID   string `json:"project" yaml:"project"`
	TopicID     string `json:"topic" yaml:"topic"`
	MaxInFlight int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:   "",
		TopicID:     "",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// GCPPubSub is a benthos writer.Type implementation that writes messages to a
// GCP Pub/Sub topic.
type GCPPubSub struct {
	conf GCPPubSubConfig

	client   *pubsub.Client
	topic    *pubsub.Topic
	topicMut sync.Mutex

	log   log.Modular
	stats metrics.Type
}

// NewGCPPubSub creates a new GCP Cloud Pub/Sub writer.Type.
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
		client: client,
		stats:  stats,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target GCP
// Pub/Sub topic.
func (c *GCPPubSub) ConnectWithContext(ctx context.Context) error {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topic != nil {
		return nil
	}

	topic := c.client.Topic(c.conf.TopicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("topic '%v' does not exist", c.conf.TopicID)
	}

	c.topic = topic
	c.log.Infof("Sending GCP Cloud Pub/Sub messages to project '%v' and topic '%v'\n", c.conf.ProjectID, c.conf.TopicID)
	return nil
}

// Connect attempts to establish a connection to the target GCP Pub/Sub topic.
func (c *GCPPubSub) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return c.ConnectWithContext(ctx)
}

// WriteWithContext attempts to write message contents to a target topic.
func (c *GCPPubSub) WriteWithContext(ctx context.Context, msg types.Message) error {
	c.topicMut.Lock()
	topic := c.topic
	c.topicMut.Unlock()

	if c.topic == nil {
		return types.ErrNotConnected
	}

	results := make([]*pubsub.PublishResult, msg.Len())

	msg.Iter(func(i int, part types.Part) error {
		attr := map[string]string{}
		part.Metadata().Iter(func(k, v string) error {
			attr[k] = v
			return nil
		})
		gmsg := &pubsub.Message{
			Data: part.Get(),
		}
		if len(attr) > 0 {
			gmsg.Attributes = attr
		}
		results[i] = topic.Publish(ctx, gmsg)
		return nil
	})

	var errs []error
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to send messages: %v", errs)
	}
	return nil
}

// Write attempts to write message contents to a target topic.
func (c *GCPPubSub) Write(msg types.Message) error {
	return c.WriteWithContext(context.Background(), msg)
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (c *GCPPubSub) CloseAsync() {
	go func() {
		c.topicMut.Lock()
		defer c.topicMut.Unlock()
		if c.topic != nil {
			c.topic.Stop()
			c.topic = nil
		}
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (c *GCPPubSub) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
