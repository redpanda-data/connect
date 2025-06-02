// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/Jeffail/shutdown"
	"github.com/cenkalti/backoff/v4"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/dispatch"
)

const (
	kroFieldConsumerGroup         = "consumer_group"
	kroFieldCommitPeriod          = "commit_period"
	kroFieldPartitionBuffer       = "partition_buffer_bytes"
	kroFieldTopicLagRefreshPeriod = "topic_lag_refresh_period"
	kroFieldMaxYieldBatchBytes    = "max_yield_batch_bytes"
)

// FranzReaderOrderedConfigFields returns config fields for customising the
// behaviour of kafka reader with strict ordering using the franz-go library.
func FranzReaderOrderedConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(kroFieldConsumerGroup).
			Description("An optional consumer group to consume as. When specified the partitions of specified topics are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name. Consumer groups are not supported when specifying explicit partitions to consume from in the `topics` field.").
			Optional(),
		service.NewDurationField(kroFieldCommitPeriod).
			Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
			Default("5s").
			Advanced(),
		service.NewStringField(kroFieldPartitionBuffer).
			Description("A buffer size (in bytes) for each consumed partition, allowing records to be queued internally before flushing. Increasing this may improve throughput at the cost of higher memory utilisation. Note that each buffer can grow slightly beyond this value.").
			Default("1MB").
			Advanced(),
		service.NewDurationField(kroFieldTopicLagRefreshPeriod).
			Description("The period of time between each topic lag refresh cycle.").
			Default("5s").
			Advanced(),
		service.NewStringField(kroFieldMaxYieldBatchBytes).
			Description("The maximum size (in bytes) for each batch yielded by this input. When routed to a redpanda output without modification this would roughly translate to the batch.bytes config field of a traditional producer.").
			Default("32KB").
			Advanced(),
	}
}

//------------------------------------------------------------------------------

// FranzReaderOrdered implements a kafka reader using the franz-go library.
type FranzReaderOrdered struct {
	clientOpts func() ([]kgo.Opt, error)

	partState *partitionState
	Client    *kgo.Client

	consumerGroup         string
	commitPeriod          time.Duration
	cacheLimit            uint64
	readBackOff           backoff.BackOff
	topicLagRefreshPeriod time.Duration
	batchMaxSize          uint64

	res     *service.Resources
	log     *service.Logger
	shutSig *shutdown.Signaller
}

// NewFranzReaderOrderedFromConfig attempts to instantiate a new FranzReaderOrdered reader from a parsed config.
func NewFranzReaderOrderedFromConfig(conf *service.ParsedConfig, res *service.Resources, optsFn func() ([]kgo.Opt, error)) (*FranzReaderOrdered, error) {
	readBackOff := backoff.NewExponentialBackOff()
	readBackOff.InitialInterval = time.Millisecond
	readBackOff.MaxInterval = time.Millisecond * 100
	readBackOff.MaxElapsedTime = 0

	f := FranzReaderOrdered{
		readBackOff: readBackOff,
		res:         res,
		log:         res.Logger(),
		shutSig:     shutdown.NewSignaller(),
		clientOpts:  optsFn,
	}

	f.consumerGroup, _ = conf.FieldString(kroFieldConsumerGroup)

	var err error
	if f.cacheLimit, err = bytesFromStrField(kroFieldPartitionBuffer, conf); err != nil {
		return nil, err
	}

	if f.commitPeriod, err = conf.FieldDuration(kroFieldCommitPeriod); err != nil {
		return nil, err
	}

	if f.topicLagRefreshPeriod, err = conf.FieldDuration(kroFieldTopicLagRefreshPeriod); err != nil {
		return nil, err
	}

	if f.batchMaxSize, err = bytesFromStrField(kroFieldMaxYieldBatchBytes, conf); err != nil {
		return nil, err
	}

	return &f, nil
}

type messageWithRecord struct {
	m    *service.Message
	r    *kgo.Record
	size uint64
}

type batchWithRecords struct {
	b    []*messageWithRecord
	size uint64
}

func recordsToBatch(records []*kgo.Record, consumerLag *ConsumerLag) (batch batchWithRecords) {
	batch.b = make([]*messageWithRecord, len(records))

	for i, r := range records {
		msg := FranzRecordToMessageV1(r)
		if consumerLag != nil {
			lag := consumerLag.Load(r.Topic, r.Partition)
			msg.MetaSetMut("kafka_lag", lag)
		}

		rmsg := &messageWithRecord{
			m:    msg,
			r:    r,
			size: uint64(len(r.Value) + len(r.Key)),
		}

		batch.b[i] = rmsg
		batch.size += rmsg.size

		// The record lives on for checkpointing, but we don't need the contents
		// going forward so discard these. This looked fine to me but could
		// potentially be a source of problems so treat this as sus.
		r.Key = nil
		r.Value = nil
	}
	return
}

//------------------------------------------------------------------------------

type partitionCache struct {
	mut             sync.Mutex
	pendingDispatch map[int64]struct{}
	cache           []*batchWithRecords
	cacheSize       uint64
	checkpointer    *checkpoint.Uncapped[*kgo.Record]
	commitFn        func(r *kgo.Record)
}

func newPartitionCache(commitFn func(r *kgo.Record)) *partitionCache {
	pt := &partitionCache{
		pendingDispatch: map[int64]struct{}{},
		checkpointer:    checkpoint.NewUncapped[*kgo.Record](),
		commitFn:        commitFn,
	}
	return pt
}

func (p *partitionCache) push(bufferSize, maxBatchSize uint64, batch *batchWithRecords) (pauseFetch bool) {
	p.mut.Lock()
	defer p.mut.Unlock()

	// Calculate new size of the cache
	p.cacheSize += batch.size
	pauseFetch = p.cacheSize >= bufferSize

	if len(p.cache) > 0 {
		// If we have existing batch in the cache and it has spare capacity then
		// collapse as many of our new batch into it as possible.
		indexEnd := len(p.cache) - 1

		for len(batch.b) > 0 && p.cache[indexEnd].size < maxBatchSize {
			nextMsgSize := batch.b[0].size

			if p.cache[indexEnd].size+nextMsgSize > maxBatchSize {
				break
			}

			p.cache[indexEnd].b = append(p.cache[indexEnd].b, batch.b[0])
			p.cache[indexEnd].size += nextMsgSize

			batch.b = batch.b[1:]
			batch.size -= nextMsgSize
		}
	}

	for len(batch.b) > 0 {
		if batch.size <= maxBatchSize {
			p.cache = append(p.cache, batch)
			return
		}

		tmpBatch := &batchWithRecords{}
		for len(batch.b) > 0 {
			nextMsgSize := batch.b[0].size

			if len(tmpBatch.b) > 0 && tmpBatch.size+nextMsgSize > maxBatchSize {
				break
			}

			tmpBatch.b = append(tmpBatch.b, batch.b[0])
			tmpBatch.size += nextMsgSize

			batch.b = batch.b[1:]
			batch.size -= nextMsgSize
		}

		p.cache = append(p.cache, tmpBatch)
	}

	return
}

func (p *partitionCache) pop() *batchWithAckFn {
	p.mut.Lock()
	defer p.mut.Unlock()

	if len(p.cache) == 0 {
		return nil
	}

	// If any batches are in flight and pending dispatch then we do not allow
	// further batches to be popped. This is necessary for ordering guarantees.
	if len(p.pendingDispatch) > 0 {
		return nil
	}

	nextBatch := p.cache[0]
	p.cache = p.cache[1:]

	batchID := nextBatch.b[0].r.Offset
	p.pendingDispatch[batchID] = struct{}{}

	dispatchCounter := int64(len(nextBatch.b))

	outBatch := make(service.MessageBatch, len(nextBatch.b))

	for i := 0; i < len(nextBatch.b); i++ {
		var incOnce sync.Once
		outBatch[i] = nextBatch.b[i].m.WithContext(dispatch.CtxOnTriggerSignal(nextBatch.b[i].m.Context(), func() {
			incOnce.Do(func() {
				if atomic.AddInt64(&dispatchCounter, -1) <= 0 {
					p.mut.Lock()
					delete(p.pendingDispatch, batchID)
					p.mut.Unlock()
				}
			})
		}))
	}

	releaseFn := p.checkpointer.Track(nextBatch.b[len(nextBatch.b)-1].r, int64(len(nextBatch.b)))
	onAck := func() {
		p.mut.Lock()
		releaseRecord := releaseFn()
		delete(p.pendingDispatch, batchID)
		p.cacheSize -= nextBatch.size
		p.mut.Unlock()

		if releaseRecord != nil && *releaseRecord != nil {
			p.commitFn(*releaseRecord)
		}
	}

	return &batchWithAckFn{
		onAck: onAck,
		batch: outBatch,
	}
}

func (p *partitionCache) pauseFetch(limit uint64) (pauseFetch bool) {
	p.mut.Lock()
	pauseFetch = p.cacheSize >= limit
	p.mut.Unlock()
	return
}

//------------------------------------------------------------------------------

type partitionState struct {
	mut    sync.Mutex
	topics map[string]map[int32]*partitionCache

	commitFn func(r *kgo.Record)
}

func newPartitionState(releaseFn func(r *kgo.Record)) *partitionState {
	return &partitionState{
		topics:   map[string]map[int32]*partitionCache{},
		commitFn: releaseFn,
	}
}

func (c *partitionState) pop() *batchWithAckFn {
	c.mut.Lock()
	defer c.mut.Unlock()

	for _, v := range c.topics {
		for _, p := range v {
			if b := p.pop(); b != nil {
				return b
			}
		}
	}
	return nil
}

func (c *partitionState) addRecords(topic string, partition int32, batch *batchWithRecords, bufferSize, maxBatchSize uint64) (pauseFetch bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicTracker := c.topics[topic]
	if topicTracker == nil {
		topicTracker = map[int32]*partitionCache{}
		c.topics[topic] = topicTracker
	}

	partCache := topicTracker[partition]
	if partCache == nil {
		partCache = newPartitionCache(c.commitFn)
		topicTracker[partition] = partCache
	}

	if batch != nil {
		return partCache.push(bufferSize, maxBatchSize, batch)
	}
	return partCache.pauseFetch(bufferSize)
}

func (c *partitionState) pauseFetch(topic string, partition int32, limit uint64) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicTracker := c.topics[topic]
	if topicTracker == nil {
		return false
	}
	partTracker := topicTracker[partition]
	if partTracker == nil {
		return false
	}

	return partTracker.pauseFetch(limit)
}

func (c *partitionState) removeTopicPartitions(m map[string][]int32) {
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

func (c *partitionState) tallyActivePartitions(pausedPartitions map[string][]int32) (tally int) {
	c.mut.Lock()
	defer c.mut.Unlock()

	// This may not be 100% accurate, and perhaps even flakey, but as long as
	// we're able to detect 0 active partitions then we're happy.
	for topic, parts := range c.topics {
		tally += (len(parts) - len(pausedPartitions[topic]))
	}
	return
}

//------------------------------------------------------------------------------

// Connect to the kafka seed brokers.
func (f *FranzReaderOrdered) Connect(ctx context.Context) error {
	if f.partState != nil {
		return nil
	}

	if f.shutSig.IsSoftStopSignalled() {
		f.shutSig.TriggerHasStopped()
		return service.ErrEndOfInput
	}

	clientOpts, err := f.clientOpts()
	if err != nil {
		return err
	}

	commitFn := func(*kgo.Record) {}
	if f.consumerGroup != "" {
		commitFn = func(r *kgo.Record) {
			if f.Client == nil {
				return
			}
			f.Client.MarkCommitRecords(r)
		}
	}

	checkpoints := newPartitionState(commitFn)

	if f.consumerGroup != "" {
		clientOpts = append(clientOpts,
			kgo.OnPartitionsRevoked(func(rctx context.Context, c *kgo.Client, m map[string][]int32) {
				if commitErr := c.CommitMarkedOffsets(rctx); commitErr != nil {
					f.log.Errorf("Commit error on partition revoke: %v", commitErr)
				}
				checkpoints.removeTopicPartitions(m)
			}),
			kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				// No point trying to commit our offsets, just clean up our topic map
				checkpoints.removeTopicPartitions(m)
			}),
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				for topic, parts := range m {
					for _, part := range parts {
						// Adds the partition to our checkpointer
						checkpoints.addRecords(topic, part, nil, f.cacheLimit, f.batchMaxSize)
					}
				}
			}),
			kgo.ConsumerGroup(f.consumerGroup),
			kgo.AutoCommitMarks(),
			kgo.AutoCommitInterval(f.commitPeriod),
			kgo.WithLogger(&KGoLogger{f.log}),
		)
	}

	if f.Client, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	noActivePartitionsBackOff := backoff.NewExponentialBackOff()
	noActivePartitionsBackOff.InitialInterval = time.Microsecond * 50
	noActivePartitionsBackOff.MaxInterval = time.Second
	noActivePartitionsBackOff.MaxElapsedTime = 0

	connErrBackOff := backoff.NewExponentialBackOff()
	connErrBackOff.InitialInterval = time.Millisecond * 100
	connErrBackOff.MaxInterval = time.Second
	connErrBackOff.MaxElapsedTime = 0

	// Check connectivity to cluster
	if err = f.Client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	go func() {
		var consumerLag *ConsumerLag
		if f.consumerGroup != "" {
			topicLagGauge := f.res.Metrics().NewGauge("redpanda_lag", "topic", "partition")
			consumerLag = NewConsumerLag(f.Client, f.consumerGroup, f.res.Logger(), topicLagGauge, f.topicLagRefreshPeriod)
			consumerLag.Start()
			defer consumerLag.Stop()
		}
		defer func() {
			f.Client.Close()
			if f.shutSig.IsSoftStopSignalled() {
				f.shutSig.TriggerHasStopped()
			}
		}()

		closeCtx, done := f.shutSig.SoftStopCtx(context.Background())
		defer done()

		for {
			// Using a stall prevention context here because I've realised we
			// might end up disabling literally all the partitions and topics
			// we're allocated.
			//
			// In this case we don't want to actually resume any of them yet so
			// I add a forced timeout to deal with it.
			stallCtx, pollDone := context.WithTimeout(closeCtx, time.Second)
			fetches := f.Client.PollFetches(stallCtx)
			pollDone()

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
						f.log.Errorf("Kafka poll error on topic %v, partition %v: %v", kerr.Topic, kerr.Partition, kerr.Err)
					}
				}

				if nonTemporalErr && fetches.Empty() {
					select {
					case <-time.After(connErrBackOff.NextBackOff()):
					case <-closeCtx.Done():
						return
					}
				}
			} else {
				connErrBackOff.Reset()
			}

			if closeCtx.Err() != nil {
				return
			}

			pauseTopicPartitions := map[string][]int32{}
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}

				batch := recordsToBatch(p.Records, consumerLag)
				if len(batch.b) == 0 {
					return
				}

				if checkpoints.addRecords(p.Topic, p.Partition, &batch, f.cacheLimit, f.batchMaxSize) {
					pauseTopicPartitions[p.Topic] = append(pauseTopicPartitions[p.Topic], p.Partition)
				}
			})

			pausedPartitionTopics := f.Client.PauseFetchPartitions(pauseTopicPartitions)
			noActivePartitionsBackOff.Reset()

		noActivePartitions:
			for {
				// Walk all the disabled topic partitions and check whether any
				// of them can be resumed.
				resumeTopicPartitions := map[string][]int32{}
				for pausedTopic, pausedPartitions := range pausedPartitionTopics {
					for _, pausedPartition := range pausedPartitions {
						if !checkpoints.pauseFetch(pausedTopic, pausedPartition, f.cacheLimit) {
							resumeTopicPartitions[pausedTopic] = append(resumeTopicPartitions[pausedTopic], pausedPartition)
						}
					}
				}
				if len(resumeTopicPartitions) > 0 {
					f.Client.ResumeFetchPartitions(resumeTopicPartitions)
				}

				if len(f.consumerGroup) == 0 || len(resumeTopicPartitions) > 0 || checkpoints.tallyActivePartitions(pausedPartitionTopics) > 0 {
					break noActivePartitions
				}

				select {
				case <-time.After(noActivePartitionsBackOff.NextBackOff()):
				case <-closeCtx.Done():
					return
				}

				// Unfortunately we need to re-allocate this in order to
				// correctly analyse paused topic partitions against our active
				// counts. This is because it's possible that were lost our
				// allocation to partitions of a topic, but gained others, since
				// the last call.
				pausedPartitionTopics = f.Client.PauseFetchPartitions(nil)
			}
		}
	}()

	f.partState = checkpoints
	return nil
}

// ReadBatch attempts to extract a batch of messages from the target topics.
func (f *FranzReaderOrdered) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if f.partState == nil {
		return nil, nil, service.ErrNotConnected
	}

	for {
		if mAck := f.partState.pop(); mAck != nil {
			f.readBackOff.Reset()
			return mAck.batch, func(context.Context, error) error {
				// Res will always be nil because we initialize with service.AutoRetryNacks
				mAck.onAck()
				return nil
			}, nil
		}
		select {
		case <-time.After(f.readBackOff.NextBackOff()):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// Close underlying connections.
func (f *FranzReaderOrdered) Close(ctx context.Context) error {
	go func() {
		f.shutSig.TriggerSoftStop()
		if f.partState == nil {
			// We haven't connected, so force the shutdown complete signal.
			f.shutSig.TriggerHasStopped()
		}
	}()
	select {
	case <-f.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
