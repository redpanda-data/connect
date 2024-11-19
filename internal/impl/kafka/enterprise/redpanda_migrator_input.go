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
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rmiFieldConsumerGroup             = "consumer_group"
	rmiFieldCommitPeriod              = "commit_period"
	rmiFieldMultiHeader               = "multi_header"
	rmiFieldBatchSize                 = "batch_size"
	rmiFieldTopicLagRefreshPeriod     = "topic_lag_refresh_period"
	rmiFieldOutputResource            = "output_resource"
	rmiFieldReplicationFactorOverride = "replication_factor_override"
	rmiFieldReplicationFactor         = "replication_factor"

	rmiResourceDefaultLabel = "redpanda_migrator_input"
)

func redpandaMigratorInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.37.0").
		Summary(`A Redpanda Migrator input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
Reads a batch of messages from a Kafka broker and waits for the output to acknowledge the writes before updating the Kafka consumer group offset.

This input should be used in combination with a ` + "`redpanda_migrator`" + ` output which it can query for existing topics.

When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

It attempts to create all selected topics it along with their associated ACLs in the broker that the ` + "`redpanda_migrator`" + ` output points to identified by the label specified in ` + "`output_resource`" + `.

== Metrics

Emits a ` + "`input_redpanda_migrator_lag`" + ` metric with ` + "`topic`" + ` and ` + "`partition`" + ` labels for each consumed topic.

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
		Fields(RedpandaMigratorInputConfigFields()...).
		LintRule(`
let has_topic_partitions = this.topics.any(t -> t.contains(":"))
root = if $has_topic_partitions {
  if this.consumer_group.or("") != "" {
    "this input does not support both a consumer group and explicit topic partitions"
  } else if this.regexp_topics {
    "this input does not support both regular expression topics and explicit topic partitions"
  }
} else {
  if this.consumer_group.or("") == "" {
    "a consumer group is mandatory when not using explicit topic partitions"
  }
}
`)
}

// RedpandaMigratorInputConfigFields returns the full suite of config fields for a `redpanda_migrator` input using the
// franz-go client library.
func RedpandaMigratorInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		kafka.FranzConsumerFields(),
		[]*service.ConfigField{
			service.NewStringField(rmiFieldConsumerGroup).
				Description("An optional consumer group to consume as. When specified the partitions of specified topics are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name. Consumer groups are not supported when specifying explicit partitions to consume from in the `topics` field.").
				Optional(),
			service.NewDurationField(rmiFieldCommitPeriod).
				Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
				Default("5s").
				Advanced(),
			service.NewBoolField(rmiFieldMultiHeader).
				Description("Decode headers into lists to allow handling of multiple values with the same key").
				Default(false).
				Advanced(),
			service.NewIntField(rmiFieldBatchSize).
				Description("The maximum number of messages that should be accumulated into each batch.").
				Default(1024).
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
			service.NewDurationField(rmiFieldTopicLagRefreshPeriod).
				Description("The period of time between each topic lag refresh cycle.").
				Default("5s").
				Advanced(),
			service.NewStringField(rmiFieldOutputResource).
				Description("The label of the redpanda_migrator output in which the currently selected topics need to be created before attempting to read messages.").
				Default(rmoResourceDefaultLabel).
				Advanced(),
			service.NewBoolField(rmiFieldReplicationFactorOverride).
				Description("Use the specified replication factor when creating topics.").
				Default(true).
				Advanced(),
			service.NewIntField(rmiFieldReplicationFactor).
				Description("Replication factor for created topics. This is only used when `replication_factor_override` is set to `true`.").
				Default(3).
				Advanced(),
		},
	)
}

func init() {
	err := service.RegisterBatchInput("redpanda_migrator", redpandaMigratorInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := NewRedpandaMigratorReaderFromConfig(conf, mgr)
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

// RedpandaMigratorReader implements a kafka reader using the franz-go library.
type RedpandaMigratorReader struct {
	clientDetails   *kafka.FranzConnectionDetails
	consumerDetails *kafka.FranzConsumerDetails

	clientLabel string

	topicPatterns []*regexp.Regexp

	consumerGroup             string
	commitPeriod              time.Duration
	multiHeader               bool
	batchSize                 int
	topicLagRefreshPeriod     time.Duration
	outputResource            string
	replicationFactorOverride bool
	replicationFactor         int

	connMut             sync.Mutex
	readMut             sync.Mutex
	client              *kgo.Client
	topicLagGauge       *service.MetricGauge
	topicLagCache       sync.Map
	outputTopicsCreated bool

	mgr     *service.Resources
	shutSig *shutdown.Signaller
}

// NewRedpandaMigratorReaderFromConfig attempts to instantiate a new RedpandaMigratorReader
// from a parsed config.
func NewRedpandaMigratorReaderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*RedpandaMigratorReader, error) {
	r := RedpandaMigratorReader{
		mgr:           mgr,
		shutSig:       shutdown.NewSignaller(),
		topicLagGauge: mgr.Metrics().NewGauge("input_redpanda_migrator_lag", "topic", "partition"),
	}

	var err error

	if r.clientDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
		return nil, err
	}
	if r.consumerDetails, err = kafka.FranzConsumerDetailsFromConfig(conf); err != nil {
		return nil, err
	}

	if r.consumerDetails.RegexPattern {
		r.topicPatterns = make([]*regexp.Regexp, 0, len(r.consumerDetails.Topics))
		for _, topic := range r.consumerDetails.Topics {
			tp, err := regexp.Compile(topic)
			if err != nil {
				return nil, fmt.Errorf("failed to compile topic regex %q: %s", topic, err)
			}
			r.topicPatterns = append(r.topicPatterns, tp)
		}
	}

	if conf.Contains(rmiFieldConsumerGroup) {
		if r.consumerGroup, err = conf.FieldString(rmiFieldConsumerGroup); err != nil {
			return nil, err
		}
	}

	if r.batchSize, err = conf.FieldInt(rmiFieldBatchSize); err != nil {
		return nil, err
	}

	if r.commitPeriod, err = conf.FieldDuration(rmiFieldCommitPeriod); err != nil {
		return nil, err
	}

	if r.multiHeader, err = conf.FieldBool(rmiFieldMultiHeader); err != nil {
		return nil, err
	}

	if r.topicLagRefreshPeriod, err = conf.FieldDuration(rmiFieldTopicLagRefreshPeriod); err != nil {
		return nil, err
	}

	if r.replicationFactorOverride, err = conf.FieldBool(rmiFieldReplicationFactorOverride); err != nil {
		return nil, err
	}

	if r.replicationFactor, err = conf.FieldInt(rmiFieldReplicationFactor); err != nil {
		return nil, err
	}

	if r.outputResource, err = conf.FieldString(rmiFieldOutputResource); err != nil {
		return nil, err
	}

	if r.clientLabel = mgr.Label(); r.clientLabel == "" {
		r.clientLabel = rmiResourceDefaultLabel
	}

	return &r, nil
}

func (r *RedpandaMigratorReader) recordToMessage(record *kgo.Record) *service.Message {
	msg := kafka.FranzRecordToMessageV0(record, r.multiHeader)

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
func (r *RedpandaMigratorReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.client != nil {
		return nil
	}

	if r.shutSig.IsSoftStopSignalled() {
		r.shutSig.TriggerHasStopped()
		return service.ErrEndOfInput
	}

	clientOpts := append([]kgo.Opt{}, r.clientDetails.FranzOpts()...)
	clientOpts = append(clientOpts, r.consumerDetails.FranzOpts()...)
	if r.consumerGroup != "" {
		clientOpts = append(clientOpts,
			// TODO: Do we need to do anything in `kgo.OnPartitionsRevoked()` / `kgo.OnPartitionsLost()`
			kgo.ConsumerGroup(r.consumerGroup),
			kgo.AutoCommitMarks(),
			kgo.BlockRebalanceOnPoll(),
			kgo.AutoCommitInterval(r.commitPeriod),
			kgo.WithLogger(&kafka.KGoLogger{L: r.mgr.Logger()}),
		)
	}

	var err error
	if r.client, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err = r.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	if err = kafka.FranzSharedClientSet(r.clientLabel, &kafka.FranzSharedClientInfo{
		Client:      r.client,
		ConnDetails: r.clientDetails,
	}, r.mgr); err != nil {
		r.mgr.Logger().With("error", err).Warn("Failed to store client connection for sharing")
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
func (r *RedpandaMigratorReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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
	if r.consumerDetails.RegexPattern {
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
	} else if r.consumerDetails.RegexPattern {
		r.mgr.Logger().Warn("No matching topics found")
	}

	if !r.outputTopicsCreated {
		if err := kafka.FranzSharedClientUse(r.outputResource, r.mgr, func(details *kafka.FranzSharedClientInfo) error {
			for _, topic := range topics {
				if err := createTopic(ctx, topic, r.replicationFactorOverride, r.replicationFactor, r.client, details.Client); err != nil && err != errTopicAlreadyExists {
					// We could end up attempting to create a topic which doesn't have any messages in it, so if that
					// fails, we can just log an error and carry on. If it does contain messages, the output will
					// attempt to create it again anyway and will trigger and error if it can't.
					// The output `topicCache` could be populated here to avoid the redundant call to create topics, but
					// it's not worth the complexity.
					r.mgr.Logger().Errorf("Failed to create topic %q and ACLs: %s", topic, err)
				} else {
					if err == errTopicAlreadyExists {
						r.mgr.Logger().Debugf("Topic %q already exists", topic)
					} else {
						r.mgr.Logger().Infof("Created topic %q in output cluster", topic)
					}
					if err := createACLs(ctx, topic, r.client, details.Client); err != nil {
						r.mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
					}
				}
			}
			r.outputTopicsCreated = true
			return nil
		}); err != nil {
			r.mgr.Logger().With("error", err, "resource", r.outputResource).Warn("Failed to access shared client for given resource identifier")
		}

	}

	resBatch := make(service.MessageBatch, 0, fetches.NumRecords())
	fetches.EachRecord(func(rec *kgo.Record) {
		resBatch = append(resBatch, r.recordToMessage(rec))
	})

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
		r.client.AllowRebalance()

		return nil
	}, nil
}

// Close underlying connections.
func (r *RedpandaMigratorReader) Close(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	go func() {
		r.shutSig.TriggerSoftStop()
		if r.client != nil {
			_, _ = kafka.FranzSharedClientPop(r.clientLabel, r.mgr)

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
