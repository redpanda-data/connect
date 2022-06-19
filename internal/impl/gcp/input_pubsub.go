package gcp

import (
	"context"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newGCPPubSubInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "gcp_pubsub",
		Summary: `Consumes messages from a GCP Cloud Pub/Sub subscription.`,
		Description: `
For information on how to set up credentials check out
[this guide](https://cloud.google.com/docs/authentication/production).

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- gcp_pubsub_publish_time_unix
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []string{
			"Services",
			"GCP",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("project", "The project ID of the target subscription."),
			docs.FieldString("subscription", "The target subscription ID."),
			docs.FieldBool("sync", "Enable synchronous pull mode."),
			docs.FieldInt("max_outstanding_messages", "The maximum number of outstanding pending messages to be consumed at a given time."),
			docs.FieldInt("max_outstanding_bytes", "The maximum number of outstanding pending messages to be consumed measured in bytes."),
		).ChildDefaultAndTypesFromStruct(input.NewGCPPubSubConfig()),
	})
	if err != nil {
		panic(err)
	}
}

func newGCPPubSubInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	var c input.Async
	var err error
	if c, err = newGCPPubSubReader(conf.GCPPubSub, log, stats); err != nil {
		return nil, err
	}
	return input.NewAsyncReader("gcp_pubsub", true, c, mgr)
}

type gcpPubSubReader struct {
	conf input.GCPPubSubConfig

	subscription *pubsub.Subscription
	msgsChan     chan *pubsub.Message
	closeFunc    context.CancelFunc
	subMut       sync.Mutex

	client *pubsub.Client

	log log.Modular
}

func newGCPPubSubReader(conf input.GCPPubSubConfig, log log.Modular, stats metrics.Type) (*gcpPubSubReader, error) {
	client, err := pubsub.NewClient(context.Background(), conf.ProjectID)
	if err != nil {
		return nil, err
	}
	return &gcpPubSubReader{
		conf:   conf,
		log:    log,
		client: client,
	}, nil
}

func (c *gcpPubSubReader) ConnectWithContext(ignored context.Context) error {
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

func (c *gcpPubSubReader) ReadWithContext(ctx context.Context) (*message.Batch, input.AsyncAckFn, error) {
	c.subMut.Lock()
	msgsChan := c.msgsChan
	c.subMut.Unlock()
	if msgsChan == nil {
		return nil, nil, component.ErrNotConnected
	}

	msg := message.QuickBatch(nil)

	var gmsg *pubsub.Message
	var open bool
	select {
	case gmsg, open = <-msgsChan:
	case <-ctx.Done():
		return nil, nil, component.ErrTimeout
	}
	if !open {
		return nil, nil, component.ErrNotConnected
	}

	part := message.NewPart(gmsg.Data)
	for k, v := range gmsg.Attributes {
		part.MetaSet(k, v)
	}
	part.MetaSet("gcp_pubsub_publish_time_unix", strconv.FormatInt(gmsg.PublishTime.Unix(), 10))
	msg.Append(part)

	return msg, func(ctx context.Context, res error) error {
		if res != nil {
			gmsg.Nack()
		} else {
			gmsg.Ack()
		}
		return nil
	}, nil
}

func (c *gcpPubSubReader) CloseAsync() {
	c.subMut.Lock()
	if c.closeFunc != nil {
		c.closeFunc()
		c.closeFunc = nil
	}
	c.subMut.Unlock()
}

func (c *gcpPubSubReader) WaitForClose(time.Duration) error {
	return nil
}
