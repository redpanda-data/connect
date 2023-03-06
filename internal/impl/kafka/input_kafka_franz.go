package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/benthosdev/benthos/v4/internal/checkpoint"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func franzKafkaInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		Version("3.61.0").
		Summary("An alternative Kafka input using the [Franz Kafka client library](https://github.com/twmb/franz-go).").
		Description(`
Consumes one or more topics by balancing the partitions across any other connected clients with the same consumer group.

This input is new and experimental, and the existing ` + "`kafka`" + ` input is not going anywhere, but here's some reasons why it might be worth trying this one out:

- You like shiny new stuff
- You are experiencing issues with the existing ` + "`kafka`" + ` input
- Someone told you to

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp_unix
- All record headers
` + "```" + `
`).
		Field(service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewStringListField("topics").
			Description("A list of topics to consume from, partitions are automatically shared across consumers sharing the consumer group.")).
		Field(service.NewBoolField("regexp_topics").
			Description("Whether listed topics should be interpretted as regular expression patterns for matching multiple topics.").
			Default(false)).
		Field(service.NewStringField("consumer_group").
			Description("A consumer group to consume as. Partitions are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name.")).
		Field(service.NewIntField("checkpoint_limit").
			Description("Determines how many messages of the same partition can be processed in parallel before applying back pressure. When a message of a given offset is delivered to the output the offset is only allowed to be committed when all messages of prior offsets have also been delivered, this ensures at-least-once delivery guarantees. However, this mechanism also increases the likelihood of duplicates in the event of crashes or server faults, reducing the checkpoint limit will mitigate this.").
			Default(1024).
			Advanced()).
		Field(service.NewDurationField("commit_period").
			Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
			Default("5s").
			Advanced()).
		Field(service.NewBoolField("start_from_oldest").
			Description("If an offset is not found for a topic partition, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset.").
			Default(true).
			Advanced()).
		Field(service.NewTLSToggledField("tls")).
		Field(saslField()).
		Field(service.NewBoolField("multi_header").Description("Decode headers into lists to allow handling of multiple values with the same key").Default(false).Advanced())
}

func init() {
	err := service.RegisterInput("kafka_franz", franzKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			rdr, err := newFranzKafkaReaderFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(rdr), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type msgWithAckFn struct {
	onAck func()
	msg   *service.Message
}

type franzKafkaReader struct {
	seedBrokers     []string
	topics          []string
	consumerGroup   string
	tlsConf         *tls.Config
	saslConfs       []sasl.Mechanism
	checkpointLimit int
	startFromOldest bool
	commitPeriod    time.Duration
	regexPattern    bool
	multiHeader     bool

	msgChan atomic.Value
	log     *service.Logger
	shutSig *shutdown.Signaller
}

func (f *franzKafkaReader) getMsgChan() chan msgWithAckFn {
	c, _ := f.msgChan.Load().(chan msgWithAckFn)
	return c
}

func (f *franzKafkaReader) storeMsgChan(c chan msgWithAckFn) {
	f.msgChan.Store(c)
}

func newFranzKafkaReaderFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzKafkaReader, error) {
	f := franzKafkaReader{
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		f.seedBrokers = append(f.seedBrokers, strings.Split(b, ",")...)
	}

	topicList, err := conf.FieldStringList("topics")
	if err != nil {
		return nil, err
	}
	for _, t := range topicList {
		f.topics = append(f.topics, strings.Split(t, ",")...)
	}

	if f.regexPattern, err = conf.FieldBool("regexp_topics"); err != nil {
		return nil, err
	}

	if f.consumerGroup, err = conf.FieldString("consumer_group"); err != nil {
		return nil, err
	}

	if f.checkpointLimit, err = conf.FieldInt("checkpoint_limit"); err != nil {
		return nil, err
	}

	if f.startFromOldest, err = conf.FieldBool("start_from_oldest"); err != nil {
		return nil, err
	}

	if f.commitPeriod, err = conf.FieldDuration("commit_period"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		f.tlsConf = tlsConf
	}
	if f.multiHeader, err = conf.FieldBool("multi_header"); err != nil {
		return nil, err
	}
	if f.saslConfs, err = saslMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	return &f, nil
}

//------------------------------------------------------------------------------

type checkpointTracker struct {
	mut    sync.Mutex
	topics map[string]map[int32]*checkpoint.Uncapped[*kgo.Record]
}

func newCheckpointTracker() *checkpointTracker {
	return &checkpointTracker{
		topics: map[string]map[int32]*checkpoint.Uncapped[*kgo.Record]{},
	}
}

func (c *checkpointTracker) addRecord(r *kgo.Record) (removeFn func() *kgo.Record, currentPending int) {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicCheckpoints := c.topics[r.Topic]
	if topicCheckpoints == nil {
		topicCheckpoints = map[int32]*checkpoint.Uncapped[*kgo.Record]{}
		c.topics[r.Topic] = topicCheckpoints
	}

	partCheckpoint := topicCheckpoints[r.Partition]
	if partCheckpoint == nil {
		partCheckpoint = checkpoint.NewUncapped[*kgo.Record]()
		topicCheckpoints[r.Partition] = partCheckpoint
	}

	releaseFn := partCheckpoint.Track(r, 1)
	return func() *kgo.Record {
		c.mut.Lock()
		defer c.mut.Unlock()

		r := releaseFn()
		if r == nil {
			return nil
		}
		return *r
	}, int(partCheckpoint.Pending())
}

func (c *checkpointTracker) getHighest(topic string, partition int32) *kgo.Record {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicCheckpoints := c.topics[topic]
	if topicCheckpoints == nil {
		return nil
	}
	partCheckpoint := topicCheckpoints[partition]
	if partCheckpoint == nil {
		return nil
	}

	highestRec := partCheckpoint.Highest()
	if highestRec == nil {
		return nil
	}
	return *highestRec
}

func (c *checkpointTracker) getPending(topic string, partition int32) int {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicCheckpoints := c.topics[topic]
	if topicCheckpoints == nil {
		return 0
	}
	partCheckpoint := topicCheckpoints[partition]
	if partCheckpoint == nil {
		return 0
	}
	return int(partCheckpoint.Pending())
}

func (c *checkpointTracker) removeTopicPartitions(m map[string][]int32) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for topicName, lostTopic := range m {
		trackedTopic, exists := c.topics[topicName]
		if !exists {
			continue
		}
		for _, lostPartition := range lostTopic {
			delete(trackedTopic, lostPartition)
		}
		if len(trackedTopic) == 0 {
			delete(c.topics, topicName)
		}
	}
}

//------------------------------------------------------------------------------

func (f *franzKafkaReader) Connect(ctx context.Context) error {
	if f.getMsgChan() != nil {
		return nil
	}

	if f.shutSig.ShouldCloseAtLeisure() {
		f.shutSig.ShutdownComplete()
		return service.ErrEndOfInput
	}

	checkpoints := newCheckpointTracker()

	var initialOffset kgo.Offset
	if f.startFromOldest {
		initialOffset = kgo.NewOffset().AtStart()
	} else {
		initialOffset = kgo.NewOffset().AtEnd()
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(f.seedBrokers...),
		kgo.ConsumerGroup(f.consumerGroup),
		kgo.ConsumeTopics(f.topics...),
		kgo.ConsumeResetOffset(initialOffset),
		kgo.SASL(f.saslConfs...),
		kgo.OnPartitionsRevoked(func(rctx context.Context, c *kgo.Client, m map[string][]int32) {
			// Note: this is a best attempt, there's a chance of duplicates if
			// the checkpoint limit is borked with slow moving pending messages,
			// but we can't block here, so work with that we have.
			finalOffsets := map[string]map[int32]kgo.EpochOffset{}
			for topic, parts := range m {
				offsets := map[int32]kgo.EpochOffset{}
				for _, part := range parts {
					if rec := checkpoints.getHighest(topic, part); rec != nil {
						offsets[part] = kgo.EpochOffset{
							Epoch:  rec.LeaderEpoch,
							Offset: rec.Offset,
						}
					}
				}
				finalOffsets[topic] = offsets
			}

			c.CommitOffsetsSync(rctx, finalOffsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, commitErr error) {
				if commitErr == nil {
					return
				}
				f.log.Errorf("Commit error on partition revoke: %v", commitErr)
			})
			checkpoints.removeTopicPartitions(m)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			// No point trying to commit our offsets, just clean up our topic map
			checkpoints.removeTopicPartitions(m)
		}),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(f.commitPeriod),
		kgo.WithLogger(&kgoLogger{f.log}),
	}

	if f.tlsConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.tlsConf))
	}

	if f.regexPattern {
		clientOpts = append(clientOpts, kgo.ConsumeRegex())
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
	}

	msgChan := make(chan msgWithAckFn)
	go func() {
		defer func() {
			cl.Close()
			f.storeMsgChan(nil)
			close(msgChan)
			if f.shutSig.ShouldCloseAtLeisure() {
				f.shutSig.ShutdownComplete()
			}
		}()

		closeCtx, done := f.shutSig.CloseAtLeisureCtx(context.Background())
		defer done()

		for {
			// Using a stall prevention context here because I've realised we
			// might end up disabling literally all the partitions and topics
			// we're allocated.
			//
			// In this case we don't want to actually resume any of them yet so
			// I add a forced timeout to deal with it.
			stallCtx, pollDone := context.WithTimeout(closeCtx, time.Second)
			fetches := cl.PollFetches(stallCtx)
			pollDone()

			if errs := fetches.Errors(); len(errs) > 0 {
				// Any non-temporal error sets this true and we close the client
				// forcing a reconnect.
				nonTemporalErr := false

				for _, kerr := range errs {
					// TODO: The documentation from franz-go is top-tier, it
					// should be straight forward to expand this to include more
					// errors that are safe to disregard.
					if errors.Is(kerr.Err, context.DeadlineExceeded) {
						continue
					}
					nonTemporalErr = true
					f.log.Errorf("Kafka poll error on topic %v, partition %v: %v", kerr.Topic, kerr.Partition, kerr.Err)
				}

				if nonTemporalErr {
					cl.Close()
					return
				}
			}
			if closeCtx.Err() != nil {
				return
			}

			pauseTopicPartitions := map[string][]int32{}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				msg := recordToMessage(record, f.multiHeader)

				// The record lives on for checkpointing, but we don't need the
				// contents going forward so discard these. This looked fine to
				// me but could potentially be a source of problems so treat
				// this as sus.
				record.Key = nil
				record.Value = nil

				releaseFn, pending := checkpoints.addRecord(record)
				if pending >= f.checkpointLimit {
					// If the number of in flight messages from this partition
					// reaches our limit then add it to the list of parsed
					// partitions.
					//
					// We then flush the record downstream, which may block long
					// enough for the pending count to go down. However, we have
					// a chance before the next flush to resume if this is the
					// case.
					pauseTopicPartitions[record.Topic] = append(pauseTopicPartitions[record.Topic], record.Partition)
				}

				select {
				case msgChan <- msgWithAckFn{
					msg: msg,
					onAck: func() {
						if maxRec := releaseFn(); maxRec != nil {
							cl.MarkCommitRecords(maxRec)
						}
					},
				}:
				case <-closeCtx.Done():
					return
				}
			}

			// Walk all the disabled topic partitions and check whether any of
			// them can be resumed.
			resumeTopicPartitions := map[string][]int32{}
			for pausedTopic, pausedPartitions := range cl.PauseFetchPartitions(pauseTopicPartitions) {
				for _, pausedPartition := range pausedPartitions {
					pending := checkpoints.getPending(pausedTopic, pausedPartition)
					if pending >= f.checkpointLimit {
						continue
					}
					resumeTopicPartitions[pausedTopic] = append(resumeTopicPartitions[pausedTopic], pausedPartition)
				}
			}
			if len(resumeTopicPartitions) > 0 {
				cl.ResumeFetchPartitions(resumeTopicPartitions)
			}
		}
	}()

	f.storeMsgChan(msgChan)
	f.log.Infof("Receiving messages from Kafka topics: %v", f.topics)
	return nil
}

func recordToMessage(record *kgo.Record, multiHeader bool) *service.Message {
	msg := service.NewMessage(record.Value)
	msg.MetaSet("kafka_key", string(record.Key))
	msg.MetaSet("kafka_topic", record.Topic)
	msg.MetaSet("kafka_partition", strconv.Itoa(int(record.Partition)))
	msg.MetaSet("kafka_offset", strconv.Itoa(int(record.Offset)))
	msg.MetaSet("kafka_timestamp_unix", strconv.FormatInt(record.Timestamp.Unix(), 10))
	if multiHeader {
		// in multi header mode we gather headers so we can encode them as lists
		var headers = map[string][]any{}

		for _, hdr := range record.Headers {
			headers[hdr.Key] = append(headers[hdr.Key], string(hdr.Value))
		}

		for key, values := range headers {
			msg.MetaSetMut(key, values)
		}
	} else {
		for _, hdr := range record.Headers {
			msg.MetaSet(hdr.Key, string(hdr.Value))
		}
	}
	return msg
}

func (f *franzKafkaReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msgChan := f.getMsgChan()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var mAck msgWithAckFn
	var open bool
	select {
	case mAck, open = <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	return mAck.msg, func(ctx context.Context, res error) error {
		// Res will always be nil because we initialize with service.AutoRetryNacks
		mAck.onAck()
		return nil
	}, nil
}

func (f *franzKafkaReader) Close(ctx context.Context) error {
	go func() {
		f.shutSig.CloseAtLeisure()
		if f.getMsgChan() == nil {
			// If the record chan is already nil then we might've not been
			// connected, so force the shutdown complete signal.
			f.shutSig.ShutdownComplete()
		}
	}()
	select {
	case <-f.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
