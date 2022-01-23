package reader

import (
	"context"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// GCPPubSubConfig contains configuration values for the input type.
type GCPPubSubConfig struct {
	ProjectID              string `json:"project" yaml:"project"`
	SubscriptionID         string `json:"subscription" yaml:"subscription"`
	MaxOutstandingMessages int    `json:"max_outstanding_messages" yaml:"max_outstanding_messages"`
	MaxOutstandingBytes    int    `json:"max_outstanding_bytes" yaml:"max_outstanding_bytes"`
	Sync                   bool   `json:"sync" yaml:"sync"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:              "",
		SubscriptionID:         "",
		MaxOutstandingMessages: pubsub.DefaultReceiveSettings.MaxOutstandingMessages,
		MaxOutstandingBytes:    pubsub.DefaultReceiveSettings.MaxOutstandingBytes,
		Sync:                   false,
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

	client *pubsub.Client

	log   log.Modular
	stats metrics.Type
}

// NewGCPPubSub creates a new GCP pubsub reader.Type.
func NewGCPPubSub(
	conf GCPPubSubConfig,
	log log.Modular,
	stats metrics.Type,
) (*GCPPubSub, error) {
	client, err := pubsub.NewClient(context.Background(), conf.ProjectID)
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
	sub.ReceiveSettings.Synchronous = c.conf.Sync

	subCtx, cancel := context.WithCancel(context.Background())
	msgsChan := make(chan *pubsub.Message, 1)

	c.subscription = sub
	c.msgsChan = msgsChan
	c.closeFunc = cancel

	go func() {
		rerr := sub.Receive(subCtx, func(ctx context.Context, m *pubsub.Message) {
			select {
			case msgsChan <- m:
			case <-ctx.Done():
				if m != nil {
					m.Nack()
				}
			}
		})
		if rerr != nil && rerr != context.Canceled {
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
