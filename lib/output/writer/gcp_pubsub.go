package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// GCPPubSubConfig contains configuration fields for the output GCPPubSub type.
type GCPPubSubConfig struct {
	ProjectID      string          `json:"project" yaml:"project"`
	TopicID        string          `json:"topic" yaml:"topic"`
	MaxInFlight    int             `json:"max_in_flight" yaml:"max_in_flight"`
	PublishTimeout string          `json:"publish_timeout" yaml:"publish_timeout"`
	Metadata       output.Metadata `json:"metadata" yaml:"metadata"`
}

// NewGCPPubSubConfig creates a new Config with default values.
func NewGCPPubSubConfig() GCPPubSubConfig {
	return GCPPubSubConfig{
		ProjectID:      "",
		TopicID:        "",
		MaxInFlight:    1,
		PublishTimeout: "60s",
		Metadata:       output.NewMetadata(),
	}
}

//------------------------------------------------------------------------------

// GCPPubSub is a benthos writer.Type implementation that writes messages to a
// GCP Pub/Sub topic.
type GCPPubSub struct {
	conf GCPPubSubConfig

	client         *pubsub.Client
	publishTimeout time.Duration
	metaFilter     *output.MetadataFilter

	topicID  *field.Expression
	topics   map[string]*pubsub.Topic
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
	client, err := pubsub.NewClient(context.Background(), conf.ProjectID)
	if err != nil {
		return nil, err
	}
	topic, err := bloblang.NewField(conf.TopicID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}
	pubTimeout, err := time.ParseDuration(conf.PublishTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse publish timeout duration: %w", err)
	}
	metaFilter, err := conf.Metadata.Filter()
	if err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	return &GCPPubSub{
		conf:           conf,
		log:            log,
		metaFilter:     metaFilter,
		client:         client,
		publishTimeout: pubTimeout,
		stats:          stats,
		topicID:        topic,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target GCP
// Pub/Sub topic.
func (c *GCPPubSub) ConnectWithContext(ctx context.Context) error {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topics != nil {
		return nil
	}

	c.topics = map[string]*pubsub.Topic{}
	c.log.Infof("Sending GCP Cloud Pub/Sub messages to project '%v' and topic '%v'\n", c.conf.ProjectID, c.conf.TopicID)
	return nil
}

func (c *GCPPubSub) getTopic(ctx context.Context, t string) (*pubsub.Topic, error) {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topics == nil {
		return nil, types.ErrNotConnected
	}
	if t, exists := c.topics[t]; exists {
		return t, nil
	}

	topic := c.client.Topic(t)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate topic '%v': %v", t, err)
	}
	if !exists {
		return nil, fmt.Errorf("topic '%v' does not exist", t)
	}
	topic.PublishSettings.Timeout = c.publishTimeout
	c.topics[t] = topic
	return topic, nil
}

// Connect attempts to establish a connection to the target GCP Pub/Sub topic.
func (c *GCPPubSub) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	return c.ConnectWithContext(ctx)
}

// WriteWithContext attempts to write message contents to a target topic.
func (c *GCPPubSub) WriteWithContext(ctx context.Context, msg types.Message) error {
	topics := make([]*pubsub.Topic, msg.Len())
	if err := msg.Iter(func(i int, _ types.Part) error {
		var tErr error
		topics[i], tErr = c.getTopic(ctx, c.topicID.String(i, msg))
		return tErr
	}); err != nil {
		return err
	}

	results := make([]*pubsub.PublishResult, msg.Len())
	msg.Iter(func(i int, part types.Part) error {
		topic := topics[i]
		attr := map[string]string{}
		c.metaFilter.Iter(part.Metadata(), func(k, v string) error {
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

	var batchErr *batch.Error
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			if batchErr == nil {
				batchErr = batch.NewError(msg, err)
			}
			batchErr.Failed(i, err)
		}
	}
	if batchErr != nil {
		return batchErr
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
		if c.topics != nil {
			for _, t := range c.topics {
				t.Stop()
			}
			c.topics = nil
		}
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (c *GCPPubSub) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
