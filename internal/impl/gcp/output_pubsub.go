package gcp

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/sourcegraph/conc/pool"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
)

func newPubSubOutputConfig() *service.ConfigSpec {
	defaults := pubsub.DefaultPublishSettings

	return service.NewConfigSpec().
		Stable().
		Categories("Services", "GCP").
		Summary("Sends messages to a GCP Cloud Pub/Sub topic. [Metadata](/docs/configuration/metadata) from messages are sent as attributes.").
		Description(`
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
`+"```"+``).
		Fields(
			service.NewStringField("project").Description("The project ID of the topic to publish to."),
			service.NewInterpolatedStringField("topic").Description("The topic to publish to."),
			service.NewStringField("endpoint").
				Default("").
				Example("us-central1-pubsub.googleapis.com:443").
				Example("us-west3-pubsub.googleapis.com:443").
				Description("An optional endpoint to override the default of `pubsub.googleapis.com:443`. This can be used to connect to a region specific pubsub endpoint. For a list of valid values check out [this document.](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#list_of_regional_endpoints)"),
			service.NewInterpolatedStringField("ordering_key").
				Optional().
				Description("The ordering key to use for publishing messages.").
				Advanced(),
			service.NewIntField("max_in_flight").Default(64).Description("The maximum number of messages to have in flight at a given time. Increasing this may improve throughput."),
			service.NewIntField("count_threshold").
				Default(defaults.CountThreshold).
				Description("Publish a pubsub buffer when it has this many messages"),
			service.NewDurationField("delay_threshold").
				Default(defaults.DelayThreshold.String()).
				Description("Publish a non-empty pubsub buffer after this delay has passed."),
			service.NewIntField("byte_threshold").
				Default(defaults.ByteThreshold).
				Description("Publish a batch when its size in bytes reaches this value."),
			service.NewDurationField("publish_timeout").
				Default(defaults.Timeout.String()).
				Example("10s").
				Example("5m").
				Example("60m").
				Description("The maximum length of time to wait before abandoning a publish attempt for a message.").
				Advanced(),
			service.NewMetadataExcludeFilterField("metadata").
				Optional().
				Description("Specify criteria for which metadata values are sent as attributes, all are sent by default."),
			service.NewObjectField(
				"flow_control",
				service.NewIntField("max_outstanding_bytes").
					Default(defaults.FlowControlSettings.MaxOutstandingBytes).
					Description("Maximum size of buffered messages to be published. If less than or equal to zero, this is disabled."),
				service.NewIntField("max_outstanding_messages").
					Default(defaults.FlowControlSettings.MaxOutstandingMessages).
					Description("Maximum number of buffered messages to be published. If less than or equal to zero, this is disabled."),
				service.NewStringEnumField("limit_exceeded_behavior", "ignore", "block", "signal_error").
					Default("block").
					Description("Configures the behavior when trying to publish additional messages while the flow controller is full. The available options are block (default), ignore (disable), and signal_error (publish results will return an error)."),
			).
				Description("For a given topic, configures the PubSub client's internal buffer for messages to be published.").
				Advanced(),
			service.NewBatchPolicyField("batching").
				Description("Configures a batching policy on this output. While the PubSub client maintains its own internal buffering mechanism, preparing larger batches of messages can further trade-off some latency for throughput."),
		)
}

type pubsubOutput struct {
	topicMut sync.Mutex
	topics   map[string]pubsubTopic

	project         string
	clientOpts      []option.ClientOption
	client          pubsubClient
	clientCancel    context.CancelFunc
	publishSettings *pubsub.PublishSettings
	topicQ          *service.InterpolatedString
	metaFilter      *service.MetadataExcludeFilter
	orderingKeyQ    *service.InterpolatedString
}

func newPubSubOutput(conf *service.ParsedConfig) (*pubsubOutput, error) {
	var settings pubsub.PublishSettings

	project, err := conf.FieldString("project")
	if err != nil {
		return nil, err
	}

	topicQ, err := conf.FieldInterpolatedString("topic")
	if err != nil {
		return nil, err
	}

	metaFilter, err := conf.FieldMetadataExcludeFilter("metadata")
	if err != nil {
		return nil, err
	}

	var orderingKeyQ *service.InterpolatedString
	if conf.Contains("ordering_key") {
		if orderingKeyQ, err = conf.FieldInterpolatedString("ordering_key"); err != nil {
			return nil, err
		}
	}

	if settings.DelayThreshold, err = conf.FieldDuration("delay_threshold"); err != nil {
		return nil, err
	}
	if settings.CountThreshold, err = conf.FieldInt("count_threshold"); err != nil {
		return nil, err
	}
	if settings.ByteThreshold, err = conf.FieldInt("byte_threshold"); err != nil {
		return nil, err
	}
	if settings.Timeout, err = conf.FieldDuration("publish_timeout"); err != nil {
		return nil, err
	}

	flowConf := conf.Namespace("flow_control")
	var flowControl pubsub.FlowControlSettings
	if flowControl.MaxOutstandingBytes, err = flowConf.FieldInt("max_outstanding_bytes"); err != nil {
		return nil, err
	}
	if flowControl.MaxOutstandingMessages, err = flowConf.FieldInt("max_outstanding_messages"); err != nil {
		return nil, err
	}

	var limitBehavior string
	if limitBehavior, err = flowConf.FieldString("limit_exceeded_behavior"); err != nil {
		return nil, err
	}

	switch limitBehavior {
	case "ignore":
		flowControl.LimitExceededBehavior = pubsub.FlowControlIgnore
	case "block":
		flowControl.LimitExceededBehavior = pubsub.FlowControlBlock
	case "signal_error":
		flowControl.LimitExceededBehavior = pubsub.FlowControlSignalError
	default:
		return nil, fmt.Errorf("unrecognized flow control setting: %s", limitBehavior)
	}

	settings.FlowControlSettings = flowControl

	var endpoint string
	if endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, err
	}

	var opt []option.ClientOption
	if endpoint != "" {
		opt = []option.ClientOption{option.WithEndpoint(endpoint)}
	}

	return &pubsubOutput{
		topics:          make(map[string]pubsubTopic),
		project:         project,
		clientOpts:      opt,
		publishSettings: &settings,
		topicQ:          topicQ,
		metaFilter:      metaFilter,
		orderingKeyQ:    orderingKeyQ,
	}, nil
}

func (out *pubsubOutput) Connect(_ context.Context) error {
	if out.client != nil {
		return nil
	}

	clientCtx, clientCancel := context.WithCancel(context.Background())
	client, err := pubsub.NewClient(clientCtx, out.project, out.clientOpts...)
	if err != nil {
		clientCancel()
		return fmt.Errorf("failed to create pubsub client: %w", err)
	}

	out.client = &airGappedPubsubClient{client}
	out.clientCancel = clientCancel

	return nil
}

func (out *pubsubOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	topics := make(map[string]pubsubTopic)
	p := pool.NewWithResults[*serverResult]().WithContext(ctx)

	var batchErr *service.BatchError
	batchErrFailed := func(i int, err error) {
		if batchErr == nil {
			batchErr = service.NewBatchError(batch, err)
		}
		batchErr.Failed(i, err)
	}

	for i, msg := range batch {
		i := i
		res, err := out.writeMessage(ctx, topics, msg)
		if err != nil {
			batchErrFailed(i, err)
			continue
		}

		p.Go(func(ctx context.Context) (*serverResult, error) {
			_, err := res.Get(ctx)
			if err != nil {
				return &serverResult{batchIndex: i, err: err}, nil
			}
			return nil, nil
		})
	}

	getResults, err := p.Wait()
	if err != nil {
		return fmt.Errorf("failed to get publish results: %w", err)
	}

	for _, res := range getResults {
		if res == nil {
			continue
		}
		batchErrFailed(res.batchIndex, res.err)
	}

	if batchErr != nil && batchErr.IndexedErrors() > 0 {
		return batchErr
	}
	return nil
}

func (out *pubsubOutput) Close(_ context.Context) error {
	out.topicMut.Lock()
	defer out.topicMut.Unlock()

	for _, t := range out.topics {
		t.Stop()
	}
	out.topics = nil

	if out.clientCancel != nil {
		out.clientCancel()
	}

	return nil
}

func (out *pubsubOutput) writeMessage(ctx context.Context, cachedTopics map[string]pubsubTopic, msg *service.Message) (publishResult, error) {
	topicName, err := out.topicQ.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve topic name: %w", err)
	}

	topic, found := cachedTopics[topicName]

	if !found {
		t, err := out.getTopic(ctx, topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to get topic: %s: %w", topicName, err)
		}

		cachedTopics[topicName] = t
		topic = t
	}

	attr := make(map[string]string)
	if err := out.metaFilter.Walk(msg, func(key, value string) error {
		attr[key] = value
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to build message attributes: %w", err)
	}

	var orderingKey string
	if out.orderingKeyQ != nil {
		if orderingKey, err = out.orderingKeyQ.TryString(msg); err != nil {
			return nil, fmt.Errorf("failed to build ordering key: %w", err)
		}
	}

	data, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get bytes from message: %w", err)
	}

	return topic.Publish(ctx, &pubsub.Message{
		Data:        data,
		Attributes:  attr,
		OrderingKey: orderingKey,
	}), nil
}

func (out *pubsubOutput) getTopic(ctx context.Context, name string) (pubsubTopic, error) {
	out.topicMut.Lock()
	defer out.topicMut.Unlock()

	if t, exists := out.topics[name]; exists {
		return t, nil
	}

	t := out.client.Topic(name, out.publishSettings)
	exists, err := t.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to validate topic '%v': %v", name, err)
	}
	if !exists {
		return nil, fmt.Errorf("topic '%v' does not exist", name)
	}

	if out.orderingKeyQ != nil {
		t.EnableOrdering()
	}

	out.topics[name] = t
	return t, nil
}

type serverResult struct {
	batchIndex int
	err        error
}

func init() {
	if err := service.RegisterBatchOutput("gcp_pubsub", newPubSubOutputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
		maxInFlight, err = conf.FieldInt("max_in_flight")
		if err != nil {
			return
		}

		batchPolicy, err = conf.FieldBatchPolicy("batching")
		if err != nil {
			return
		}

		out, err = newPubSubOutput(conf)

		return
	}); err != nil {
		panic(err)
	}
}
