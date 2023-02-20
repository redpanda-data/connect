package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/kafka/sasl"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newKafkaInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "kafka",
		Summary: `Connects to Kafka brokers and consumes one or more topics.`,
		Description: `
Offsets are managed within Kafka under the specified consumer group, and partitions for each topic are automatically balanced across members of the consumer group.

The Kafka input allows parallel processing of messages from different topic partitions, but by default messages of the same topic partition are processed in lockstep in order to enforce ordered processing. This protection often means that batching messages at the output level can stall, in which case it can be tuned by increasing the field ` + "[`checkpoint_limit`](#checkpoint_limit)" + `, ideally to a value greater than the number of messages you expect to batch.

Alternatively, if you perform batching at the input level using the ` + "[`batching`](#batching)" + ` field it is done per-partition and therefore avoids stalling.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
` + "```" + `

The field ` + "`kafka_lag`" + ` is the calculated difference between the high water mark offset of the partition at the time of ingestion and the current message offset.

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).

### Ordering

By default messages of a topic partition can be processed in parallel, up to a limit determined by the field ` + "`checkpoint_limit`" + `. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step. When doing so it is recommended that you perform batching at this component for performance as it will not be possible to batch lock-stepped messages at the output level.

### Troubleshooting

If you're seeing issues writing to or reading from Kafka with this component then it's worth trying out the newer ` + "[`kafka_franz` input](/docs/components/inputs/kafka_franz)" + `.

- I'm seeing logs that report ` + "`Failed to connect to kafka: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`" + `, but the brokers are definitely reachable.

Unfortunately this error message will appear for a wide range of connection problems even when the broker endpoint can be reached. Double check your authentication configuration and also ensure that you have [enabled TLS](#tlsenabled) if applicable.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.",
				[]string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"},
			).Array(),
			docs.FieldString(
				"topics",
				"A list of topics to consume from. Multiple comma separated topics can be listed in a single element. Partitions are automatically distributed across consumers of a topic. Alternatively, it's possible to specify explicit partitions to consume from with a colon after the topic name, e.g. `foo:0` would consume the partition 0 of the topic foo. This syntax supports ranges, e.g. `foo:0-10` would consume partitions 0 through to 10 inclusive.",
				[]string{"foo", "bar"},
				[]string{"foo,bar"},
				[]string{"foo:0", "bar:1", "bar:3"},
				[]string{"foo:0,bar:1,bar:3"},
				[]string{"foo:0-5"},
			).AtVersion("3.33.0").Array(),
			docs.FieldString("target_version", "The version of the Kafka protocol to use. This limits the capabilities used by the client and should ideally match the version of your brokers."),
			btls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldString("consumer_group", "An identifier for the consumer group of the connection. This field can be explicitly made empty in order to disable stored offsets for the consumed topic partitions."),
			docs.FieldString("client_id", "An identifier for the client connection.").Advanced(),
			docs.FieldString("rack_id", "A rack identifier for this client.").Advanced(),
			docs.FieldBool("start_from_oldest", "If an offset is not found for a topic partition, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset.").Advanced(),
			docs.FieldInt(
				"checkpoint_limit", "The maximum number of messages of the same topic and partition that can be processed at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual partitions. Any given offset will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.",
			).AtVersion("3.33.0"),
			docs.FieldString("commit_period", "The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").Advanced(),
			docs.FieldString("max_processing_period", "A maximum estimate for the time taken to process a message, this is used for tuning consumer group synchronization.").Advanced(),
			span.ExtractTracingSpanMappingDocs,
			docs.FieldObject("group", "Tuning parameters for consumer group synchronization.").WithChildren(
				docs.FieldString("session_timeout", "A period after which a consumer of the group is kicked after no heartbeats.").Advanced(),
				docs.FieldString("heartbeat_interval", "A period in which heartbeats should be sent out.").Advanced(),
				docs.FieldString("rebalance_timeout", "A period after which rebalancing is abandoned if unresolved.").Advanced(),
			).Advanced(),
			docs.FieldInt("fetch_buffer_cap", "The maximum number of unprocessed messages to fetch at a given time.").Advanced(),
			docs.FieldBool("multi_header", "Decode headers into lists to allow handling of multiple values with the same key").Advanced(),
			func() docs.FieldSpec {
				b := policy.FieldSpec()
				b.IsAdvanced = true
				return b
			}(),
		).ChildDefaultAndTypesFromStruct(input.NewKafkaConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newKafkaInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	var rdr input.Async
	var err error
	if rdr, err = newKafkaReader(conf.Kafka, mgr, log); err != nil {
		return nil, err
	}
	if conf.Kafka.ExtractTracingMap != "" {
		if rdr, err = span.NewReader("kafka", conf.Kafka.ExtractTracingMap, rdr, mgr); err != nil {
			return nil, err
		}
	}
	return input.NewAsyncReader("kafka", input.NewAsyncPreserver(rdr), mgr)
}

//------------------------------------------------------------------------------

type asyncMessage struct {
	msg   message.Batch
	ackFn input.AsyncAckFn
}

type offsetMarker interface {
	MarkOffset(topic string, partition int32, offset int64, metadata string)
}

type kafkaReader struct {
	version   sarama.KafkaVersion
	tlsConf   *tls.Config
	addresses []string

	topicPartitions map[string][]int32
	balancedTopics  []string

	commitPeriod      time.Duration
	sessionTimeout    time.Duration
	heartbeatInterval time.Duration
	rebalanceTimeout  time.Duration
	maxProcPeriod     time.Duration

	// Connection resources
	cMut            sync.Mutex
	consumerCloseFn context.CancelFunc
	consumerDoneCtx context.Context
	msgChan         chan asyncMessage
	session         offsetMarker

	conf input.KafkaConfig
	log  log.Modular
	mgr  bundle.NewManagement

	closeOnce  sync.Once
	closedChan chan struct{}
}

var errCannotMixBalanced = errors.New("it is not currently possible to include balanced and explicit partition topics in the same kafka input")

func parsePartitions(expr string) ([]int32, error) {
	rangeExpr := strings.Split(expr, "-")
	if len(rangeExpr) > 2 {
		return nil, fmt.Errorf("partition '%v' is invalid, only one range can be specified", expr)
	}

	if len(rangeExpr) == 1 {
		partition, err := strconv.ParseInt(expr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse partition number: %w", err)
		}
		return []int32{int32(partition)}, nil
	}

	start, err := strconv.ParseInt(rangeExpr[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse start of range: %w", err)
	}
	end, err := strconv.ParseInt(rangeExpr[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("failed to parse end of range: %w", err)
	}

	var parts []int32
	for i := start; i <= end; i++ {
		parts = append(parts, int32(i))
	}
	return parts, nil
}

func newKafkaReader(conf input.KafkaConfig, mgr bundle.NewManagement, log log.Modular) (*kafkaReader, error) {
	if conf.Batching.IsNoop() {
		conf.Batching.Count = 1
	}
	k := kafkaReader{
		conf:            conf,
		consumerCloseFn: nil,
		log:             log,
		mgr:             mgr,
		closedChan:      make(chan struct{}),
		topicPartitions: map[string][]int32{},
	}
	if conf.TLS.Enabled {
		var err error
		if k.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); len(trimmed) > 0 {
				k.addresses = append(k.addresses, trimmed)
			}
		}
	}
	if len(conf.Topics) == 0 {
		return nil, errors.New("must specify at least one topic in the topics field")
	}
	for _, t := range conf.Topics {
		for _, splitTopics := range strings.Split(t, ",") {
			if trimmed := strings.TrimSpace(splitTopics); len(trimmed) > 0 {
				if withParts := strings.Split(trimmed, ":"); len(withParts) > 1 {
					if len(k.balancedTopics) > 0 {
						return nil, errCannotMixBalanced
					}
					if len(withParts) > 2 {
						return nil, fmt.Errorf("topic '%v' is invalid, only one partition should be specified and the same topic can be listed multiple times, e.g. use `foo:0,foo:1` not `foo:0:1`", trimmed)
					}

					topic := strings.TrimSpace(withParts[0])
					parts, err := parsePartitions(withParts[1])
					if err != nil {
						return nil, err
					}
					k.topicPartitions[topic] = append(k.topicPartitions[topic], parts...)
				} else {
					if len(k.topicPartitions) > 0 {
						return nil, errCannotMixBalanced
					}
					k.balancedTopics = append(k.balancedTopics, trimmed)
				}
			}
		}
	}
	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if k.commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}
	if tout := conf.Group.SessionTimeout; len(tout) > 0 {
		var err error
		if k.sessionTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse session timeout string: %v", err)
		}
	}
	if tout := conf.Group.HeartbeatInterval; len(tout) > 0 {
		var err error
		if k.heartbeatInterval, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse heartbeat interval string: %v", err)
		}
	}
	if tout := conf.Group.RebalanceTimeout; len(tout) > 0 {
		var err error
		if k.rebalanceTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse rebalance timeout string: %v", err)
		}
	}
	if tout := conf.MaxProcessingPeriod; len(tout) > 0 {
		var err error
		if k.maxProcPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse max processing period string: %v", err)
		}
	}
	if conf.ConsumerGroup == "" && len(k.balancedTopics) > 0 {
		return nil, errors.New("a consumer group must be specified when consuming balanced topics")
	}

	var err error
	if k.version, err = sarama.ParseKafkaVersion(conf.TargetVersion); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

func (k *kafkaReader) asyncCheckpointer(topic string, partition int32) func(context.Context, chan<- asyncMessage, message.Batch, int64) bool {
	cp := checkpoint.NewCapped[int64](int64(k.conf.CheckpointLimit))
	return func(ctx context.Context, c chan<- asyncMessage, msg message.Batch, offset int64) bool {
		if msg == nil {
			return true
		}
		resolveFn, err := cp.Track(ctx, offset, int64(msg.Len()))
		if err != nil {
			if err != component.ErrTimeout {
				k.log.Errorf("Failed to checkpoint offset: %v\n", err)
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
					k.log.Debugf("Marking offset for topic '%v' partition '%v'.\n", topic, partition)
					k.session.MarkOffset(topic, partition, *maxOffset, "")
				} else {
					k.log.Debugf("Unable to mark offset for topic '%v' partition '%v'.\n", topic, partition)
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

func (k *kafkaReader) syncCheckpointer(topic string, partition int32) func(context.Context, chan<- asyncMessage, message.Batch, int64) bool {
	ackedChan := make(chan error)
	return func(ctx context.Context, c chan<- asyncMessage, msg message.Batch, offset int64) bool {
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
						k.log.Debugf("Marking offset for topic '%v' partition '%v'.\n", topic, partition)
						k.session.MarkOffset(topic, partition, offset, "")
					} else {
						k.log.Debugf("Unable to mark offset for topic '%v' partition '%v'.\n", topic, partition)
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
					k.log.Errorf("Received error from message batch: %v, shutting down consumer.\n", resErr)
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

func dataToPart(highestOffset int64, data *sarama.ConsumerMessage, multiHeader bool) *message.Part {
	part := message.NewPart(data.Value)

	if multiHeader {
		// in multi header mode we gather headers so we can encode them as lists
		var headers = map[string][]any{}

		for _, hdr := range data.Headers {
			var key = string(hdr.Key)
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

	return part
}

//------------------------------------------------------------------------------

func (k *kafkaReader) closeGroupAndConsumers() {
	k.cMut.Lock()
	consumerCloseFn := k.consumerCloseFn
	consumerDoneCtx := k.consumerDoneCtx
	k.cMut.Unlock()

	if consumerCloseFn != nil {
		k.log.Debugln("Waiting for topic consumers to close.")
		consumerCloseFn()
		<-consumerDoneCtx.Done()
		k.log.Debugln("Topic consumers are closed.")
	}

	k.closeOnce.Do(func() {
		close(k.closedChan)
	})
}

//------------------------------------------------------------------------------

// Connect establishes a kafkaReader connection.
func (k *kafkaReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID
	config.RackID = k.conf.RackID
	config.Net.DialTimeout = time.Second
	config.Version = k.version
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = k.maxProcPeriod

	// NOTE: The following activates an async goroutine that periodically
	// commits marked offsets, but that does NOT mean we automatically commit
	// consumed message offsets.
	//
	// Offsets are manually marked ready for commit only once the associated
	// message is successfully sent via outputs (look for k.session.MarkOffset).
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = k.commitPeriod

	config.Consumer.Group.Session.Timeout = k.sessionTimeout
	config.Consumer.Group.Heartbeat.Interval = k.heartbeatInterval
	config.Consumer.Group.Rebalance.Timeout = k.rebalanceTimeout
	config.ChannelBufferSize = k.conf.FetchBufferCap

	if config.Net.ReadTimeout <= k.sessionTimeout {
		config.Net.ReadTimeout = k.sessionTimeout * 2
	}
	if config.Net.ReadTimeout <= k.rebalanceTimeout {
		config.Net.ReadTimeout = k.rebalanceTimeout * 2
	}

	config.Net.TLS.Enable = k.conf.TLS.Enabled
	if k.conf.TLS.Enabled {
		config.Net.TLS.Config = k.tlsConf
	}
	if k.conf.StartFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if err := ApplySASLConfig(k.conf.SASL, k.mgr, config); err != nil {
		return err
	}

	if len(k.topicPartitions) > 0 {
		return k.connectExplicitTopics(ctx, config)
	}
	return k.connectBalancedTopics(ctx, config)
}

// ReadBatch attempts to read a message from a kafkaReader topic.
func (k *kafkaReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, component.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, component.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, component.ErrTimeout
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
