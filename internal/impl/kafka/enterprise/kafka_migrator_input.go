// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rpriDefaultLabel = "kafka_migrator_input"
)

func kafkaMigratorInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.35.0").
		Summary(`A Kafka Migrator input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
Reads a batch of messages from a Kafka broker and waits for the output to acknowledge the writes before updating the Kafka consumer group offset.

This input should be used in combination with a ` + "`kafka_migrator`" + ` output which it can query for existing topics.

When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

It attempts to create all selected topics it along with their associated ACLs in the broker that the ` + "`kafka_migrator`" + ` output points to identified by the label specified in ` + "`output_resource`" + `.

== Metrics

Emits a ` + "`input_kafka_migrator_lag`" + ` metric with ` + "`topic`" + ` and ` + "`partition`" + ` labels for each consumed topic.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- kafka_tombstone_message
- All record headers
` + "```" + `
`).
		Fields(KafkaMigratorInputConfigFields()...).
		LintRule(`
let has_topic_partitions = this.topics.any(t -> t.contains(":"))
root = if $has_topic_partitions {
  if this.consumer_group.or("") != "" {
    "this input does not support both a consumer group and explicit topic partitions"
  } else if this.regexp_topics {
    "this input does not support both regular expression topics and explicit topic partitions"
  }
}
`)
}

// KafkaMigratorInputConfigFields returns the full suite of config fields for a `kafka_migrator` input using the
// franz-go client library.
func KafkaMigratorInputConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"}),
		service.NewStringListField("topics").
			Description(`
A list of topics to consume from. Multiple comma separated topics can be listed in a single element. When a ` + "`consumer_group`" + ` is specified partitions are automatically distributed across consumers of a topic, otherwise all partitions are consumed.

Alternatively, it's possible to specify explicit partitions to consume from with a colon after the topic name, e.g. ` + "`foo:0`" + ` would consume the partition 0 of the topic foo. This syntax supports ranges, e.g. ` + "`foo:0-10`" + ` would consume partitions 0 through to 10 inclusive.

Finally, it's also possible to specify an explicit offset to consume from by adding another colon after the partition, e.g. ` + "`foo:0:10`" + ` would consume the partition 0 of the topic foo starting from the offset 10. If the offset is not present (or remains unspecified) then the field ` + "`start_from_oldest`" + ` determines which offset to start from.`).
			Example([]string{"foo", "bar"}).
			Example([]string{"things.*"}).
			Example([]string{"foo,bar"}).
			Example([]string{"foo:0", "bar:1", "bar:3"}).
			Example([]string{"foo:0,bar:1,bar:3"}).
			Example([]string{"foo:0-5"}),
		service.NewBoolField("regexp_topics").
			Description("Whether listed topics should be interpreted as regular expression patterns for matching multiple topics. When topics are specified with explicit partitions this field must remain set to `false`.").
			Default(false),
		service.NewStringField("consumer_group").
			Description("An optional consumer group to consume as. When specified the partitions of specified topics are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name. Consumer groups are not supported when specifying explicit partitions to consume from in the `topics` field.").
			Optional(),
		service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("benthos").
			Advanced(),
		service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced(),
		service.NewIntField("batch_size").
			Description("The maximum number of messages that should be accumulated into each batch.").
			Default(1024).
			Advanced(),
		service.NewAutoRetryNacksToggleField(),
		service.NewDurationField("commit_period").
			Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
			Default("5s").
			Advanced(),
		service.NewBoolField("start_from_oldest").
			Description("Determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset. The setting is applied when creating a new consumer group or the saved offset no longer exists.").
			Default(true).
			Advanced(),
		service.NewTLSToggledField("tls"),
		kafka.SASLFields(),
		service.NewBoolField("multi_header").Description("Decode headers into lists to allow handling of multiple values with the same key").Default(false).Advanced(),
		service.NewBatchPolicyField("batching").
			Description("Allows you to configure a xref:configuration:batching.adoc[batching policy] that applies to individual topic partitions in order to batch messages together before flushing them for processing. Batching can be beneficial for performance as well as useful for windowed processing, and doing so this way preserves the ordering of topic partitions.").
			Advanced(),
		service.NewDurationField("topic_lag_refresh_period").
			Description("The period of time between each topic lag refresh cycle.").
			Default("5s").
			Advanced(),
		service.NewStringField("output_resource").
			Description("The label of the kafka_migrator output in which the currently selected topics need to be created before attempting to read messages.").
			Default(rproDefaultLabel).
			Advanced(),
		service.NewBoolField("replication_factor_override").
			Description("Use the specified replication factor when creating topics.").
			Default(true).
			Advanced(),
		service.NewIntField("replication_factor").
			Description("Replication factor for created topics. This is only used when `replication_factor_override` is set to `true`.").
			Default(3).
			Advanced(),
	}
}

func init() {
	err := service.RegisterBatchInput("kafka_migrator", kafkaMigratorInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := NewKafkaMigratorReaderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// KafkaMigratorReader implements a kafka reader using the franz-go library.
type KafkaMigratorReader struct {
	SeedBrokers               []string
	topics                    []string
	topicPatterns             []*regexp.Regexp
	topicPartitions           map[string]map[int32]kgo.Offset
	clientID                  string
	rackID                    string
	consumerGroup             string
	TLSConf                   *tls.Config
	saslConfs                 []sasl.Mechanism
	batchSize                 int
	startFromOldest           bool
	commitPeriod              time.Duration
	regexPattern              bool
	multiHeader               bool
	batchPolicy               service.BatchPolicy
	topicLagRefreshPeriod     time.Duration
	replicationFactorOverride bool
	replicationFactor         int
	outputResource            string

	connMut             sync.Mutex
	readMut             sync.Mutex
	client              *kgo.Client
	topicLagGauge       *service.MetricGauge
	topicLagCache       sync.Map
	outputTopicsCreated bool

	mgr     *service.Resources
	shutSig *shutdown.Signaller
}

// NewKafkaMigratorReaderFromConfig attempts to instantiate a new KafkaMigratorReader
// from a parsed config.
func NewKafkaMigratorReaderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*KafkaMigratorReader, error) {
	r := KafkaMigratorReader{
		mgr:           mgr,
		shutSig:       shutdown.NewSignaller(),
		topicLagGauge: mgr.Metrics().NewGauge("input_kafka_migrator_lag", "topic", "partition"),
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		r.SeedBrokers = append(r.SeedBrokers, strings.Split(b, ",")...)
	}

	if r.startFromOldest, err = conf.FieldBool("start_from_oldest"); err != nil {
		return nil, err
	}

	topicList, err := conf.FieldStringList("topics")
	if err != nil {
		return nil, err
	}

	var defaultOffset int64 = -1
	if r.startFromOldest {
		defaultOffset = -2
	}

	var topicPartitions map[string]map[int32]int64
	if r.topics, topicPartitions, err = kafka.ParseTopics(topicList, defaultOffset, true); err != nil {
		return nil, err
	}
	if len(topicPartitions) > 0 {
		r.topicPartitions = map[string]map[int32]kgo.Offset{}
		for topic, partitions := range topicPartitions {
			partMap := map[int32]kgo.Offset{}
			for part, offset := range partitions {
				partMap[part] = kgo.NewOffset().At(offset)
			}
			r.topicPartitions[topic] = partMap
		}
	}

	if r.regexPattern, err = conf.FieldBool("regexp_topics"); err != nil {
		return nil, err
	}

	if r.regexPattern {
		r.topicPatterns = make([]*regexp.Regexp, 0, len(r.topics))
		for _, topic := range r.topics {
			tp, err := regexp.Compile(topic)
			if err != nil {
				return nil, fmt.Errorf("failed to compile topic regex %q: %s", topic, err)
			}
			r.topicPatterns = append(r.topicPatterns, tp)
		}
	}

	if r.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	if r.rackID, err = conf.FieldString("rack_id"); err != nil {
		return nil, err
	}

	if r.consumerGroup, err = conf.FieldString("consumer_group"); err != nil {
		return nil, err
	}

	if r.batchSize, err = conf.FieldInt("batch_size"); err != nil {
		return nil, err
	}

	if r.commitPeriod, err = conf.FieldDuration("commit_period"); err != nil {
		return nil, err
	}

	if r.batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		r.TLSConf = tlsConf
	}
	if r.multiHeader, err = conf.FieldBool("multi_header"); err != nil {
		return nil, err
	}
	if r.saslConfs, err = kafka.SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	if r.topicLagRefreshPeriod, err = conf.FieldDuration("topic_lag_refresh_period"); err != nil {
		return nil, err
	}

	if r.replicationFactorOverride, err = conf.FieldBool("replication_factor_override"); err != nil {
		return nil, err
	}

	if r.replicationFactor, err = conf.FieldInt("replication_factor"); err != nil {
		return nil, err
	}

	if r.outputResource, err = conf.FieldString("output_resource"); err != nil {
		return nil, err
	}

	if label := mgr.Label(); label != "" {
		mgr.SetGeneric(mgr.Label(), &r)
	} else {
		mgr.SetGeneric(rpriDefaultLabel, &r)
	}

	return &r, nil
}

func (r *KafkaMigratorReader) recordToMessage(record *kgo.Record) *service.Message {
	msg := service.NewMessage(record.Value)
	msg.MetaSetMut("kafka_key", record.Key)
	msg.MetaSetMut("kafka_topic", record.Topic)
	msg.MetaSetMut("kafka_partition", int(record.Partition))
	msg.MetaSetMut("kafka_offset", int(record.Offset))
	msg.MetaSetMut("kafka_timestamp_unix", record.Timestamp.Unix())
	msg.MetaSetMut("kafka_tombstone_message", record.Value == nil)
	if r.multiHeader {
		// in multi header mode we gather headers so we can encode them as lists
		headers := map[string][]any{}

		for _, hdr := range record.Headers {
			headers[hdr.Key] = append(headers[hdr.Key], string(hdr.Value))
		}

		for key, values := range headers {
			msg.MetaSetMut(key, values)
		}
	} else {
		for _, hdr := range record.Headers {
			msg.MetaSetMut(hdr.Key, string(hdr.Value))
		}
	}

	lag := int64(0)
	if val, ok := r.topicLagCache.Load(fmt.Sprintf("%s_%d", record.Topic, record.Partition)); ok {
		lag = val.(int64)
	}
	msg.MetaSetMut("kafka_lag", lag)

	// The record lives on for checkpointing, but we don't need the contents
	// going forward so discard these. This looked fine to me but could
	// potentially be a source of problems so treat this as sus.
	record.Key = nil
	record.Value = nil

	return msg
}

//------------------------------------------------------------------------------

// Connect to the kafka seed brokers.
func (r *KafkaMigratorReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.client != nil {
		return nil
	}

	if r.shutSig.IsSoftStopSignalled() {
		r.shutSig.TriggerHasStopped()
		return service.ErrEndOfInput
	}

	var initialOffset kgo.Offset
	if r.startFromOldest {
		initialOffset = kgo.NewOffset().AtStart()
	} else {
		initialOffset = kgo.NewOffset().AtEnd()
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(r.SeedBrokers...),
		kgo.ConsumeTopics(r.topics...),
		kgo.ConsumePartitions(r.topicPartitions),
		kgo.ConsumeResetOffset(initialOffset),
		kgo.SASL(r.saslConfs...),
		kgo.ConsumerGroup(r.consumerGroup),
		kgo.ClientID(r.clientID),
		kgo.Rack(r.rackID),
	}

	if r.consumerGroup != "" {
		clientOpts = append(clientOpts,
			// TODO: Do we need to do anything in `kgo.OnPartitionsRevoked()` / `kgo.OnPartitionsLost()`
			kgo.AutoCommitMarks(),
			kgo.BlockRebalanceOnPoll(),
			kgo.AutoCommitInterval(r.commitPeriod),
			kgo.WithLogger(&kafka.KGoLogger{L: r.mgr.Logger()}),
		)
	}

	if r.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(r.TLSConf))
	}

	if r.regexPattern {
		clientOpts = append(clientOpts, kgo.ConsumeRegex())
	}

	var err error
	if r.client, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err = r.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	go func() {
		closeCtx, done := r.shutSig.SoftStopCtx(context.Background())
		defer done()

		adminClient := kadm.NewClient(r.client)

		for {
			ctx, done = context.WithTimeout(closeCtx, r.topicLagRefreshPeriod)
			var lags kadm.DescribedGroupLags
			var err error
			if lags, err = adminClient.Lag(ctx, r.consumerGroup); err != nil {
				r.mgr.Logger().Errorf("Failed to fetch group lags: %s", err)
			}
			done()

			lags.Each(func(gl kadm.DescribedGroupLag) {
				for _, gl := range gl.Lag {
					for _, pl := range gl {
						lag := pl.Lag
						if lag < 0 {
							lag = 0
						}

						r.topicLagGauge.Set(lag, pl.Topic, strconv.Itoa(int(pl.Partition)))
						r.topicLagCache.Store(fmt.Sprintf("%s_%d", pl.Topic, pl.Partition), lag)
					}
				}
			})

			select {
			case <-r.shutSig.SoftStopChan():
				return
			case <-time.After(r.topicLagRefreshPeriod):
			}
		}
	}()

	return nil
}

// ReadBatch attempts to extract a batch of messages from the target topics.
func (r *KafkaMigratorReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	r.readMut.Lock()
	defer r.readMut.Unlock()

	if r.client == nil {
		return nil, nil, service.ErrNotConnected
	}

	// TODO: Is there a way to wait a while until we actually get f.batchSize messages instead of returning as many as
	// we have right now? Otherwise, maybe switch back to `PollFetches()` and have `batch_byte_size` and `batch_period`
	// via `FetchMinBytes`, `FetchMaxBytes` and `FetchMaxWait()`?

	// TODO: Looks like when using `regexp_topics: true`, franz-go takes over a minute to discover topics which were
	// created after `PollRecords()` was called for the first time. Might need to adjust the timeout for the internal
	// topic cache.
	fetches := r.client.PollRecords(ctx, r.batchSize)
	if errs := fetches.Errors(); len(errs) > 0 {
		// Any non-temporal error sets this true and we close the client
		// forcing a reconnect.
		nonTemporalErr := false

		for _, kerr := range errs {
			// TODO: The documentation from franz-go is top-tier, it
			// should be straight forward to expand this to include more
			// errors that are safe to disregard.
			if errors.Is(kerr.Err, context.DeadlineExceeded) ||
				errors.Is(kerr.Err, context.Canceled) {
				continue
			}

			nonTemporalErr = true

			if !errors.Is(kerr.Err, kgo.ErrClientClosed) {
				r.mgr.Logger().Errorf("Kafka poll error on topic %v, partition %v: %v", kerr.Topic, kerr.Partition, kerr.Err)
			}
		}

		if nonTemporalErr {
			r.client.Close()
			r.client = nil
			return nil, nil, service.ErrNotConnected
		}
	}

	// TODO: Is there a way to get the actual selected topics instead of all of them?
	topics := r.client.GetConsumeTopics()
	if r.regexPattern {
		topics = slices.DeleteFunc(topics, func(topic string) bool {
			for _, tp := range r.topicPatterns {
				if tp.MatchString(topic) {
					return false
				}
			}
			return true
		})
	}

	if len(topics) > 0 {
		r.mgr.Logger().Debugf("Consuming from topics: %s", topics)
	} else if r.regexPattern {
		r.mgr.Logger().Warn("No matching topics found")
	}

	if !r.outputTopicsCreated {
		var output *KafkaMigratorWriter
		if res, ok := r.mgr.GetGeneric(r.outputResource); ok {
			output = res.(*KafkaMigratorWriter)
		} else {
			r.mgr.Logger().Debugf("Writer for topic destination %q not found", r.outputResource)
		}

		if output != nil {
			for _, topic := range topics {
				r.mgr.Logger().Infof("Creating topic %q", topic)

				if err := createTopic(ctx, topic, r.replicationFactorOverride, r.replicationFactor, r.client, output.client); err != nil && err != errTopicAlreadyExists {
					r.mgr.Logger().Errorf("Failed to create topic %q and ACLs: %s", topic, err)
				} else {
					if err == errTopicAlreadyExists {
						r.mgr.Logger().Debugf("Topic %q already exists", topic)
					}
					if err := createACLs(ctx, topic, r.client, output.client); err != nil {
						r.mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
					}
				}
			}
		}
		r.outputTopicsCreated = true
	}

	resBatch := make(service.MessageBatch, 0, fetches.NumRecords())
	fetches.EachRecord(func(rec *kgo.Record) {
		resBatch = append(resBatch, r.recordToMessage(rec))
	})

	// TODO: Does this need to happen after the call to `MarkCommitRecords()`?
	r.client.AllowRebalance()

	return resBatch, func(ctx context.Context, res error) error {
		r.readMut.Lock()
		defer r.readMut.Unlock()

		// TODO: What should happen when `auto_replay_nacks: false` and a batch gets rejected followed by another one
		// which gets acked?
		// Also see "Res will always be nil because we initialize with service.AutoRetryNacks" comment in
		// `input_kafka_franz.go`
		if res != nil {
			return res
		}

		r.client.MarkCommitRecords(fetches.Records()...)

		return nil
	}, nil
}

// Close underlying connections.
func (r *KafkaMigratorReader) Close(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	go func() {
		r.shutSig.TriggerSoftStop()
		if r.client != nil {
			r.client.Close()
			r.client = nil

			r.shutSig.TriggerHasStopped()
		}
	}()

	select {
	case <-r.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
