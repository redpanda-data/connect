package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	iskFieldAddresses                     = "addresses"
	iskFieldTopics                        = "topics"
	iskFieldTargetVersion                 = "target_version"
	iskFieldTLS                           = "tls"
	iskFieldConsumerGroup                 = "consumer_group"
	iskFieldClientID                      = "client_id"
	iskFieldRackID                        = "rack_id"
	iskFieldStartFromOldest               = "start_from_oldest"
	iskFieldCheckpointLimit               = "checkpoint_limit"
	iskFieldCommitPeriod                  = "commit_period"
	iskFieldMaxProcessingPeriod           = "max_processing_period"
	iskFieldGroup                         = "group"
	iskFieldGroupSessionTimeout           = "session_timeout"
	iskFieldGroupSessionHeartbeatInterval = "heartbeat_interval"
	iskFieldGroupSessionRebalanceTimeout  = "rebalance_timeout"
	iskFieldFetchBufferCap                = "fetch_buffer_cap"
	iskFieldMultiHeader                   = "multi_header"
	iskFieldBatching                      = "batching"
)

func iskConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Connects to Kafka brokers and consumes one or more topics.`).
		Description(`
Offsets are managed within Kafka under the specified consumer group, and partitions for each topic are automatically balanced across members of the consumer group.

The Kafka input allows parallel processing of messages from different topic partitions, and messages of the same topic partition are processed with a maximum parallelism determined by the field `+"[`checkpoint_limit`](#checkpoint_limit)"+`.

In order to enforce ordered processing of partition messages set the `+"[`checkpoint_limit`](#checkpoint_limit) to `1`"+` and this will force partitions to be processed in lock-step, where a message will only be processed once the prior message is delivered.

Batching messages before processing can be enabled using the `+"[`batching`](#batching)"+` field, and this batching is performed per-partition such that messages of a batch will always originate from the same partition. This batching mechanism is capable of creating batches of greater size than the `+"[`checkpoint_limit`](#checkpoint_limit)"+`, in which case the next batch will only be created upon delivery of the current one.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- kafka_tombstone_message
- All existing message headers (version 0.11+)
`+"```"+`

The field `+"`kafka_lag`"+` is the calculated difference between the high water mark offset of the partition at the time of ingestion and the current message offset.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

### Ordering

By default messages of a topic partition can be processed in parallel, up to a limit determined by the field `+"`checkpoint_limit`"+`. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step. When doing so it is recommended that you perform batching at this component for performance as it will not be possible to batch lock-stepped messages at the output level.

### Troubleshooting

If you're seeing issues writing to or reading from Kafka with this component then it's worth trying out the newer `+"[`kafka_franz` input](/docs/components/inputs/kafka_franz)"+`.

- I'm seeing logs that report `+"`Failed to connect to kafka: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`"+`, but the brokers are definitely reachable.

Unfortunately this error message will appear for a wide range of connection problems even when the broker endpoint can be reached. Double check your authentication configuration and also ensure that you have [enabled TLS](#tlsenabled) if applicable.`).
		Fields(
			service.NewStringListField(iskFieldAddresses).
				Description("A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.").
				Examples(
					[]string{"localhost:9092"},
					[]string{"localhost:9041,localhost:9042"},
					[]string{"localhost:9041", "localhost:9042"},
				),
			service.NewStringListField(iskFieldTopics).
				Description("A list of topics to consume from. Multiple comma separated topics can be listed in a single element. Partitions are automatically distributed across consumers of a topic. Alternatively, it's possible to specify explicit partitions to consume from with a colon after the topic name, e.g. `foo:0` would consume the partition 0 of the topic foo. This syntax supports ranges, e.g. `foo:0-10` would consume partitions 0 through to 10 inclusive.").
				Examples(
					[]string{"foo", "bar"},
					[]string{"foo,bar"},
					[]string{"foo:0", "bar:1", "bar:3"},
					[]string{"foo:0,bar:1,bar:3"},
					[]string{"foo:0-5"},
				).
				Version("3.33.0"),
			service.NewStringField(iskFieldTargetVersion).
				Description("The version of the Kafka protocol to use. This limits the capabilities used by the client and should ideally match the version of your brokers. Defaults to the oldest supported stable version.").
				Examples(sarama.DefaultVersion.String(), "3.1.0").
				Optional(),
			service.NewTLSToggledField(iskFieldTLS),
			SaramaSASLField(),
			service.NewStringField(iskFieldConsumerGroup).
				Description("An identifier for the consumer group of the connection. This field can be explicitly made empty in order to disable stored offsets for the consumed topic partitions.").
				Default(""),
			service.NewStringField(iskFieldClientID).
				Description("An identifier for the client connection.").
				Advanced().Default("benthos"),
			service.NewStringField(iskFieldRackID).
				Description("A rack identifier for this client.").
				Advanced().Default(""),
			service.NewBoolField(iskFieldStartFromOldest).
				Description("Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists.").
				Advanced().Default(true),
			service.NewIntField(iskFieldCheckpointLimit).
				Description("The maximum number of messages of the same topic and partition that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual partitions. Any given offset will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
				Version("3.33.0").Default(1024),
			service.NewAutoRetryNacksToggleField(),
			service.NewDurationField(iskFieldCommitPeriod).
				Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
				Advanced().Default("1s"),
			service.NewDurationField(iskFieldMaxProcessingPeriod).
				Description("A maximum estimate for the time taken to process a message, this is used for tuning consumer group synchronization.").
				Advanced().Default("100ms"),
			span.ExtractTracingSpanMappingDocs(),
			service.NewObjectField(iskFieldGroup,
				service.NewDurationField(iskFieldGroupSessionTimeout).
					Description("A period after which a consumer of the group is kicked after no heartbeats.").
					Default("10s"),
				service.NewDurationField(iskFieldGroupSessionHeartbeatInterval).
					Description("A period in which heartbeats should be sent out.").
					Default("3s"),
				service.NewDurationField(iskFieldGroupSessionRebalanceTimeout).
					Description("A period after which rebalancing is abandoned if unresolved.").
					Default("60s"),
			).
				Description("Tuning parameters for consumer group synchronization.").
				Advanced(),
			service.NewIntField(iskFieldFetchBufferCap).
				Description("The maximum number of unprocessed messages to fetch at a given time.").
				Advanced().Default(256),
			service.NewBoolField(iskFieldMultiHeader).
				Description("Decode headers into lists to allow handling of multiple values with the same key").
				Advanced().Default(false),
			service.NewBatchPolicyField(iskFieldBatching).Advanced(),
		)
}

func init() {
	err := service.RegisterBatchInput("kafka", iskConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		i, err := newKafkaReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}

		r, err := service.AutoRetryNacksBatchedToggled(conf, i)
		if err != nil {
			return nil, err
		}

		return span.NewBatchInput("kafka", conf, r, mgr)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type offsetMarker interface {
	MarkOffset(topic string, partition int32, offset int64, metadata string)
}

type kafkaReader struct {
	saramConf *sarama.Config

	addresses       []string
	batching        service.BatchPolicy
	checkpointLimit int
	commitPeriod    time.Duration
	consumerGroup   string
	multiHeader     bool
	startFromOldest bool

	topicPartitions map[string][]int32
	balancedTopics  []string

	// Connection resources
	cMut            sync.Mutex
	consumerCloseFn context.CancelFunc
	consumerDoneCtx context.Context
	msgChan         chan asyncMessage
	session         offsetMarker

	mgr *service.Resources

	closeOnce  sync.Once
	closedChan chan struct{}
}

var errCannotMixBalanced = errors.New("it is not currently possible to include balanced and explicit partition topics in the same kafka input")

func newKafkaReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*kafkaReader, error) {
	k := kafkaReader{
		consumerCloseFn: nil,
		mgr:             mgr,
		closedChan:      make(chan struct{}),
		topicPartitions: map[string][]int32{},
	}

	cAddresses, err := conf.FieldStringList(iskFieldAddresses)
	if err != nil {
		return nil, err
	}
	for _, addr := range cAddresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); trimmed != "" {
				k.addresses = append(k.addresses, trimmed)
			}
		}
	}

	if k.batching, err = conf.FieldBatchPolicy(iskFieldBatching); err != nil {
		return nil, err
	} else if k.batching.IsNoop() {
		k.batching.Count = 1
	}

	topics, err := conf.FieldStringList(iskFieldTopics)
	if err != nil {
		return nil, err
	}
	if len(topics) == 0 {
		return nil, errors.New("must specify at least one topic in the topics field")
	}

	balancedTopics, topicPartitions, err := parseTopics(topics, -1, false)
	if err != nil {
		return nil, err
	}

	if len(balancedTopics) > 0 && len(topicPartitions) > 0 {
		return nil, errCannotMixBalanced
	}
	if len(balancedTopics) > 0 {
		k.balancedTopics = balancedTopics
	} else {
		k.topicPartitions = map[string][]int32{}
		for topic, v := range topicPartitions {
			partSlice := make([]int32, 0, len(v))
			for p := range v {
				partSlice = append(partSlice, p)
			}
			k.topicPartitions[topic] = partSlice
		}
	}

	if k.checkpointLimit, err = conf.FieldInt(iskFieldCheckpointLimit); err != nil {
		return nil, err
	}
	if k.commitPeriod, err = conf.FieldDuration(iskFieldCommitPeriod); err != nil {
		return nil, err
	}
	if k.consumerGroup, err = conf.FieldString(iskFieldConsumerGroup); err != nil {
		return nil, err
	}
	if k.multiHeader, err = conf.FieldBool(iskFieldMultiHeader); err != nil {
		return nil, err
	}
	if k.startFromOldest, err = conf.FieldBool(iskFieldStartFromOldest); err != nil {
		return nil, err
	}

	if k.consumerGroup == "" && len(k.balancedTopics) > 0 {
		return nil, errors.New("a consumer group must be specified when consuming balanced topics")
	}

	if k.saramConf, err = k.saramaConfigFromParsed(conf); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

func (k *kafkaReader) asyncCheckpointer(topic string, partition int32) func(context.Context, chan<- asyncMessage, service.MessageBatch, int64) bool {
	cp := checkpoint.NewCapped[int64](int64(k.checkpointLimit))
	return func(ctx context.Context, c chan<- asyncMessage, msg service.MessageBatch, offset int64) bool {
		if msg == nil {
			return true
		}
		resolveFn, err := cp.Track(ctx, offset, int64(len(msg)))
		if err != nil {
			if ctx.Err() == nil {
				k.mgr.Logger().Errorf("Failed to checkpoint offset: %v\n", err)
			}
			return false
		}
		select {
		case c <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res error) error {
				maxOffset := resolveFn()
				if maxOffset == nil {
					return nil
				}
				k.cMut.Lock()
				if k.session != nil {
					k.mgr.Logger().Tracef("Marking offset for topic '%v' partition '%v'.\n", topic, partition)
					k.session.MarkOffset(topic, partition, *maxOffset, "")
				} else {
					k.mgr.Logger().Debugf("Unable to mark offset for topic '%v' partition '%v'.\n", topic, partition)
				}
				k.cMut.Unlock()
				return nil
			},
		}:
		case <-ctx.Done():
			return false
		}
		return true
	}
}

func (k *kafkaReader) syncCheckpointer(topic string, partition int32) func(context.Context, chan<- asyncMessage, service.MessageBatch, int64) bool {
	ackedChan := make(chan error)
	return func(ctx context.Context, c chan<- asyncMessage, msg service.MessageBatch, offset int64) bool {
		if msg == nil {
			return true
		}
		select {
		case c <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res error) error {
				resErr := res
				if resErr == nil {
					k.cMut.Lock()
					if k.session != nil {
						k.mgr.Logger().Debugf("Marking offset for topic '%v' partition '%v'.\n", topic, partition)
						k.session.MarkOffset(topic, partition, offset, "")
					} else {
						k.mgr.Logger().Debugf("Unable to mark offset for topic '%v' partition '%v'.\n", topic, partition)
					}
					k.cMut.Unlock()
				}
				select {
				case ackedChan <- resErr:
				case <-ctx.Done():
				}
				return nil
			},
		}:
			select {
			case resErr := <-ackedChan:
				if resErr != nil {
					k.mgr.Logger().Errorf("Received error from message batch: %v, shutting down consumer.\n", resErr)
					return false
				}
			case <-ctx.Done():
				return false
			}
		case <-ctx.Done():
			return false
		}
		return true
	}
}

func dataToPart(highestOffset int64, data *sarama.ConsumerMessage, multiHeader bool) *service.Message {
	part := service.NewMessage(data.Value)

	if multiHeader {
		// in multi header mode we gather headers so we can encode them as lists
		headers := map[string][]any{}

		for _, hdr := range data.Headers {
			key := string(hdr.Key)
			headers[key] = append(headers[key], string(hdr.Value))
		}

		for key, values := range headers {
			part.MetaSetMut(key, values)
		}
	} else {
		for _, hdr := range data.Headers {
			part.MetaSetMut(string(hdr.Key), string(hdr.Value))
		}
	}

	lag := highestOffset - data.Offset - 1
	if lag < 0 {
		lag = 0
	}

	part.MetaSetMut("kafka_key", string(data.Key))
	part.MetaSetMut("kafka_partition", int(data.Partition))
	part.MetaSetMut("kafka_topic", data.Topic)
	part.MetaSetMut("kafka_offset", int(data.Offset))
	part.MetaSetMut("kafka_lag", lag)
	part.MetaSetMut("kafka_timestamp_unix", data.Timestamp.Unix())
	part.MetaSetMut("kafka_tombstone_message", data.Value == nil)

	return part
}

//------------------------------------------------------------------------------

func (k *kafkaReader) closeGroupAndConsumers() {
	k.cMut.Lock()
	consumerCloseFn := k.consumerCloseFn
	consumerDoneCtx := k.consumerDoneCtx
	k.cMut.Unlock()

	if consumerCloseFn != nil {
		k.mgr.Logger().Debug("Waiting for topic consumers to close.")
		consumerCloseFn()
		<-consumerDoneCtx.Done()
		k.mgr.Logger().Debug("Topic consumers are closed.")
	}

	k.closeOnce.Do(func() {
		close(k.closedChan)
	})
}

//------------------------------------------------------------------------------

func (k *kafkaReader) saramaConfigFromParsed(conf *service.ParsedConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	var err error
	if targetVersionStr, _ := conf.FieldString(iskFieldTargetVersion); targetVersionStr != "" {
		if config.Version, err = sarama.ParseKafkaVersion(targetVersionStr); err != nil {
			return nil, err
		}
	}

	if config.ClientID, err = conf.FieldString(iskFieldClientID); err != nil {
		return nil, err
	}

	if config.RackID, err = conf.FieldString(iskFieldRackID); err != nil {
		return nil, err
	}

	config.Net.DialTimeout = time.Second
	config.Consumer.Return.Errors = true
	if config.Consumer.MaxProcessingTime, err = conf.FieldDuration(iskFieldMaxProcessingPeriod); err != nil {
		return nil, err
	}

	// NOTE: The following activates an async goroutine that periodically
	// commits marked offsets, but that does NOT mean we automatically commit
	// consumed message offsets.
	//
	// Offsets are manually marked ready for commit only once the associated
	// message is successfully sent via outputs (look for k.session.MarkOffset).
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = k.commitPeriod

	{
		cConf := conf.Namespace(iskFieldGroup)
		if config.Consumer.Group.Session.Timeout, err = cConf.FieldDuration(iskFieldGroupSessionTimeout); err != nil {
			return nil, err
		}
		if config.Consumer.Group.Heartbeat.Interval, err = cConf.FieldDuration(iskFieldGroupSessionHeartbeatInterval); err != nil {
			return nil, err
		}
		if config.Consumer.Group.Rebalance.Timeout, err = cConf.FieldDuration(iskFieldGroupSessionRebalanceTimeout); err != nil {
			return nil, err
		}
	}
	if config.ChannelBufferSize, err = conf.FieldInt(iskFieldFetchBufferCap); err != nil {
		return nil, err
	}

	if config.Net.ReadTimeout <= config.Consumer.Group.Session.Timeout {
		config.Net.ReadTimeout = config.Consumer.Group.Session.Timeout * 2
	}
	if config.Net.ReadTimeout <= config.Consumer.Group.Rebalance.Timeout {
		config.Net.ReadTimeout = config.Consumer.Group.Rebalance.Timeout * 2
	}

	if config.Net.TLS.Config, config.Net.TLS.Enable, err = conf.FieldTLSToggled(iskFieldTLS); err != nil {
		return nil, err
	}

	if k.startFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if err := ApplySaramaSASLFromParsed(conf, k.mgr, config); err != nil {
		return nil, err
	}
	return config, nil
}

// Connect establishes a kafkaReader connection.
func (k *kafkaReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	if len(k.topicPartitions) > 0 {
		return k.connectExplicitTopics(ctx, k.saramConf)
	}
	return k.connectBalancedTopics(ctx, k.saramConf)
}

// ReadBatch attempts to read a message from a kafkaReader topic.
func (k *kafkaReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

// CloseAsync shuts down the kafkaReader input and stops processing requests.
func (k *kafkaReader) Close(ctx context.Context) (err error) {
	k.closeGroupAndConsumers()
	select {
	case <-k.closedChan:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}
