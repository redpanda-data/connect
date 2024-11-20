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
	kroFieldConsumerGroup   = "consumer_group"
	kroFieldCommitPeriod    = "commit_period"
	kroFieldPartitionBuffer = "partition_buffer_bytes"
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
	}
}

//------------------------------------------------------------------------------

// FranzReaderOrdered implements a kafka reader using the franz-go library.
type FranzReaderOrdered struct {
	clientOpts func() ([]kgo.Opt, error)

	partState *partitionState

	consumerGroup string
	commitPeriod  time.Duration
	cacheLimit    uint64

	readBackOff backoff.BackOff

	res     *service.Resources
	log     *service.Logger
	shutSig *shutdown.Signaller
}

// NewFranzReaderOrderedFromConfig attempts to instantiate a new
// FranzReaderOrdered reader from a parsed config.
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

	return &f, nil
}

type batchWithRecords struct {
	b    service.MessageBatch
	r    []*kgo.Record
	size uint64
}

func (f *FranzReaderOrdered) recordsToBatch(records []*kgo.Record) *batchWithRecords {
	var length uint64
	var batch service.MessageBatch
	for _, r := range records {
		length += uint64(len(r.Value) + len(r.Key))
		batch = append(batch, FranzRecordToMessageV1(r))
		// The record lives on for checkpointing, but we don't need the contents
		// going forward so discard these. This looked fine to me but could
		// potentially be a source of problems so treat this as sus.
		r.Key = nil
		r.Value = nil
	}

	return &batchWithRecords{
		b:    batch,
		r:    records,
		size: length,
	}
}

//------------------------------------------------------------------------------

type partitionCache struct {
	mut             sync.Mutex
	pendingDispatch map[int]struct{}
	cache           []*batchWithRecords
	cacheSize       uint64
	checkpointer    *checkpoint.Uncapped[*kgo.Record]
	commitFn        func(r *kgo.Record)
}

func newPartitionCache(commitFn func(r *kgo.Record)) *partitionCache {
	pt := &partitionCache{
		pendingDispatch: map[int]struct{}{},
		checkpointer:    checkpoint.NewUncapped[*kgo.Record](),
		commitFn:        commitFn,
	}
	return pt
}

func (p *partitionCache) push(bufferSize uint64, batch *batchWithRecords) (pauseFetch bool) {
	p.mut.Lock()
	defer p.mut.Unlock()

	p.cacheSize += batch.size
	p.cache = append(p.cache, batch)

	return p.cacheSize >= bufferSize
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

	batchID := len(p.pendingDispatch)
	p.pendingDispatch[batchID] = struct{}{}

	nextBatch := p.cache[0]
	p.cache = p.cache[1:]

	dispatchCounter := int64(len(nextBatch.b))
	for i := 0; i < len(nextBatch.b); i++ {
		var incOnce sync.Once
		nextBatch.b[i] = nextBatch.b[i].WithContext(dispatch.CtxOnTriggerSignal(nextBatch.b[i].Context(), func() {
			incOnce.Do(func() {
				if atomic.AddInt64(&dispatchCounter, -1) <= 0 {
					p.mut.Lock()
					delete(p.pendingDispatch, batchID)
					p.mut.Unlock()
				}
			})
		}))
	}

	releaseFn := p.checkpointer.Track(nextBatch.r[len(nextBatch.r)-1], int64(len(nextBatch.r)))
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
		batch: nextBatch.b,
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

func (c *partitionState) addRecords(topic string, partition int32, batch *batchWithRecords, bufferSize uint64) (pauseFetch bool) {
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
		return partCache.push(bufferSize, batch)
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

	var cl *kgo.Client
	commitFn := func(r *kgo.Record) {}
	if f.consumerGroup != "" {
		commitFn = func(r *kgo.Record) {
			if cl == nil {
				return
			}
			cl.MarkCommitRecords(r)
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
						checkpoints.addRecords(topic, part, nil, f.cacheLimit)
					}
				}
			}),
			kgo.ConsumerGroup(f.consumerGroup),
			kgo.AutoCommitMarks(),
			kgo.AutoCommitInterval(f.commitPeriod),
			kgo.WithLogger(&KGoLogger{f.log}),
		)
	}

	if cl, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	go func() {
		defer func() {
			cl.Close()
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
					if errors.Is(kerr.Err, context.DeadlineExceeded) ||
						errors.Is(kerr.Err, context.Canceled) {
						continue
					}

					nonTemporalErr = true

					if !errors.Is(kerr.Err, kgo.ErrClientClosed) {
						f.log.Errorf("Kafka poll error on topic %v, partition %v: %v", kerr.Topic, kerr.Partition, kerr.Err)
					}
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
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) > 0 {
					if checkpoints.addRecords(p.Topic, p.Partition, f.recordsToBatch(p.Records), f.cacheLimit) {
						pauseTopicPartitions[p.Topic] = append(pauseTopicPartitions[p.Topic], p.Partition)
					}
				}
			})
			_ = cl.PauseFetchPartitions(pauseTopicPartitions)

		noActivePartitions:
			for {
				pausedPartitionTopics := cl.PauseFetchPartitions(nil)

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
					cl.ResumeFetchPartitions(resumeTopicPartitions)
				}

				if len(f.consumerGroup) == 0 || len(resumeTopicPartitions) > 0 || checkpoints.tallyActivePartitions(pausedPartitionTopics) > 0 {
					break noActivePartitions
				}
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
			return mAck.batch, func(ctx context.Context, res error) error {
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
