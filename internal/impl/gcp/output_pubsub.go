package gcp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/option"

	"cloud.google.com/go/pubsub"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newGCPPubSubOutput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "gcp_pubsub",
		Summary: `Sends messages to a GCP Cloud Pub/Sub topic. [Metadata](/docs/configuration/metadata) from messages are sent as attributes.`,
		Description: output.Description(true, false, `
For information on how to set up credentials check out [this guide](https://cloud.google.com/docs/authentication/production).

### Troubleshooting

If you're consistently seeing `+"`Failed to send message to gcp_pubsub: context deadline exceeded`"+` error logs without any further information it is possible that you are encountering https://github.com/benthosdev/benthos/issues/1042, which occurs when metadata values contain characters that are not valid utf-8. This can frequently occur when consuming from Kafka as the key metadata field may be populated with an arbitrary binary value, but this issue is not exclusive to Kafka.

If you are blocked by this issue then a work around is to delete either the specific problematic keys:

`+"```yaml"+`
pipeline:
  processors:
    - mapping: |
        meta kafka_key = deleted()
`+"```"+`

Or delete all keys with:

`+"```yaml"+`
pipeline:
  processors:
    - mapping: meta = deleted()
`+"```"+``),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("project", "The project ID of the topic to publish to."),
			docs.FieldString("topic", "The topic to publish to.").IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldString("publish_timeout", "The maximum length of time to wait before abandoning a publish attempt for a message.", "10s", "5m", "60m").Advanced(),
			docs.FieldString("ordering_key", "The ordering key to use for publishing messages.").IsInterpolated().Advanced(),
			docs.FieldString("endpoint", "An optional endpoint to override the default of `pubsub.googleapis.com:443`. This can be used to connect to a region specific pubsub endpoint. For a list of valid values check out [this document.](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#list_of_regional_endpoints)", "us-central1-pubsub.googleapis.com:443", "us-west3-pubsub.googleapis.com:443").HasDefault(""),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are sent as attributes.").WithChildren(metadata.ExcludeFilterFields()...),
			docs.FieldObject("flow_control", "For a given topic, configures the PubSub client's internal buffer for messages to be published.").
				WithChildren(
					docs.FieldInt("max_outstanding_messages", "Maximum number of buffered messages to be published. If less than or equal to zero, this is disabled.").HasDefault(1000),
					docs.FieldInt("max_outstanding_bytes", "Maximum size of buffered messages to be published. If less than or equal to zero, this is disabled.").HasDefault(-1),
					docs.FieldString("limit_exceeded_behavior", "Configures the behavior when trying to publish additional messages while the flow controller is full. The available options are ignore (disable, default), block, and signal_error (publish results will return an error).").
						HasDefault("ignore").
						HasOptions("ignore", "block", "signal_error"),
				).Advanced(),
		).ChildDefaultAndTypesFromStruct(output.NewGCPPubSubConfig()),
		Categories: []string{
			"Services",
			"GCP",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newGCPPubSubOutput(conf output.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	a, err := newGCPPubSubWriter(conf.GCPPubSub, mgr, log)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("gcp_pubsub", conf.GCPPubSub.MaxInFlight, a, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(w), nil
}

type gcpPubSubWriter struct {
	conf output.GCPPubSubConfig

	client         *pubsub.Client
	publishTimeout time.Duration
	metaFilter     *metadata.ExcludeFilter

	orderingEnabled bool
	orderingKey     *field.Expression

	topicID  *field.Expression
	topics   map[string]*pubsub.Topic
	topicMut sync.Mutex

	flowControl pubsub.FlowControlSettings

	log log.Modular
}

func newGCPPubSubWriter(conf output.GCPPubSubConfig, mgr bundle.NewManagement, log log.Modular) (*gcpPubSubWriter, error) {
	var opt []option.ClientOption
	if len(strings.TrimSpace(conf.Endpoint)) > 0 {
		opt = []option.ClientOption{option.WithEndpoint(conf.Endpoint)}
	}

	client, err := pubsub.NewClient(context.Background(), conf.ProjectID, opt...)
	if err != nil {
		return nil, err
	}
	topic, err := mgr.BloblEnvironment().NewField(conf.TopicID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}
	orderingKey, err := mgr.BloblEnvironment().NewField(conf.OrderingKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ordering key: %v", err)
	}
	pubTimeout, err := time.ParseDuration(conf.PublishTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to parse publish timeout duration: %w", err)
	}
	metaFilter, err := conf.Metadata.Filter()
	if err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	flowControl := pubsub.FlowControlSettings{
		MaxOutstandingMessages: conf.FlowControl.MaxOutstandingMessages,
		MaxOutstandingBytes:    conf.FlowControl.MaxOutstandingBytes,
	}
	switch conf.FlowControl.LimitExceededBehavior {
	case "ignore":
		flowControl.LimitExceededBehavior = pubsub.FlowControlIgnore
	case "block":
		flowControl.LimitExceededBehavior = pubsub.FlowControlBlock
	case "signal_error":
		flowControl.LimitExceededBehavior = pubsub.FlowControlSignalError
	default:
		return nil, fmt.Errorf("unrecognized flow control setting: %s", conf.FlowControl.LimitExceededBehavior)
	}

	return &gcpPubSubWriter{
		conf:            conf,
		log:             log,
		metaFilter:      metaFilter,
		client:          client,
		publishTimeout:  pubTimeout,
		topicID:         topic,
		orderingKey:     orderingKey,
		orderingEnabled: len(conf.OrderingKey) > 0,
		flowControl:     flowControl,
	}, nil
}

func (c *gcpPubSubWriter) Connect(ctx context.Context) error {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topics != nil {
		return nil
	}

	c.topics = map[string]*pubsub.Topic{}
	c.log.Infof("Sending GCP Cloud Pub/Sub messages to project '%v' and topic '%v'\n", c.conf.ProjectID, c.conf.TopicID)
	return nil
}

func (c *gcpPubSubWriter) getTopic(ctx context.Context, t string) (*pubsub.Topic, error) {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topics == nil {
		return nil, component.ErrNotConnected
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
	topic.PublishSettings.FlowControlSettings = c.flowControl
	topic.EnableMessageOrdering = c.orderingEnabled
	c.topics[t] = topic
	return topic, nil
}

func (c *gcpPubSubWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	topics := make([]*pubsub.Topic, msg.Len())
	if err := msg.Iter(func(i int, _ *message.Part) error {
		topicStr, tErr := c.topicID.String(i, msg)
		if tErr != nil {
			return fmt.Errorf("topic id interpolation error: %w", tErr)
		}
		topics[i], tErr = c.getTopic(ctx, topicStr)
		return tErr
	}); err != nil {
		return err
	}

	results := make([]*pubsub.PublishResult, msg.Len())
	if err := msg.Iter(func(i int, part *message.Part) error {
		topic := topics[i]
		attr := map[string]string{}
		_ = c.metaFilter.IterStr(part, func(k, v string) error {
			attr[k] = v
			return nil
		})
		gmsg := &pubsub.Message{
			Data: part.AsBytes(),
		}
		if c.orderingEnabled {
			var err error
			if gmsg.OrderingKey, err = c.orderingKey.String(i, msg); err != nil {
				return fmt.Errorf("ordering key interpolation error: %w", err)
			}
		}
		if len(attr) > 0 {
			gmsg.Attributes = attr
		}
		results[i] = topic.Publish(ctx, gmsg)
		return nil
	}); err != nil {
		return err
	}

	var batchErr *batch.Error
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			publishErr := fmt.Errorf("failed to publish message: %w", err)

			if batchErr == nil {
				batchErr = batch.NewError(msg, publishErr)
			}
			batchErr.Failed(i, publishErr)
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (c *gcpPubSubWriter) Close(context.Context) error {
	c.topicMut.Lock()
	defer c.topicMut.Unlock()
	if c.topics != nil {
		for _, t := range c.topics {
			t.Stop()
		}
		c.topics = nil
	}
	return nil
}
