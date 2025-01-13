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

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jeffail/checkpoint"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	kruFieldConsumerGroup   = "consumer_group"
	kruFieldCheckpointLimit = "checkpoint_limit"
	kruFieldCommitPeriod    = "commit_period"
	kruFieldMultiHeader     = "multi_header"
	kruFieldBatching        = "batching"
)

// FranzReaderUnorderedConfigFields returns config fields for customising the
// behaviour of an unordered kafka reader using the franz-go library. This
// reader is naive regarding message ordering, allows parallel processing across
// a given partition, but still ensures that offsets are only committed when
// safe.
func FranzReaderUnorderedConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(kruFieldConsumerGroup).
			Description("An optional consumer group to consume as. When specified the partitions of specified topics are automatically distributed across consumers sharing a consumer group, and partition offsets are automatically committed and resumed under this name. Consumer groups are not supported when specifying explicit partitions to consume from in the `topics` field.").
			Optional(),
		service.NewIntField(kruFieldCheckpointLimit).
			Description("Determines how many messages of the same partition can be processed in parallel before applying back pressure. When a message of a given offset is delivered to the output the offset is only allowed to be committed when all messages of prior offsets have also been delivered, this ensures at-least-once delivery guarantees. However, this mechanism also increases the likelihood of duplicates in the event of crashes or server faults, reducing the checkpoint limit will mitigate this.").
			Default(1024).
			Advanced(),
		service.NewDurationField(kruFieldCommitPeriod).
			Description("The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown.").
			Default("5s").
			Advanced(),
		service.NewBoolField(kruFieldMultiHeader).
			Description("Decode headers into lists to allow handling of multiple values with the same key").
			Default(false).
			Advanced(),
		service.NewBatchPolicyField(kruFieldBatching).
			Description("Allows you to configure a xref:configuration:batching.adoc[batching policy] that applies to individual topic partitions in order to batch messages together before flushing them for processing. Batching can be beneficial for performance as well as useful for windowed processing, and doing so this way preserves the ordering of topic partitions.").
			Advanced(),
	}
}

//------------------------------------------------------------------------------

type batchWithAckFn struct {
	onAck func()
	batch service.MessageBatch
}

// FranzReaderUnordered implements a kafka reader using the franz-go library.
// FranzReaderUnordered is naive regarding message ordering, allows parallel
// processing across a given partition, but still ensures that offsets are only
// committed when safe.
type FranzReaderUnordered struct {
	clientOpts []kgo.Opt

	consumerGroup   string
	checkpointLimit int
	commitPeriod    time.Duration
	multiHeader     bool
	batchPolicy     service.BatchPolicy

	batchChan atomic.Value
	res       *service.Resources
	log       *service.Logger
	shutSig   *shutdown.Signaller
}

func (f *FranzReaderUnordered) getBatchChan() chan batchWithAckFn {
	c, _ := f.batchChan.Load().(chan batchWithAckFn)
	return c
}

func (f *FranzReaderUnordered) storeBatchChan(c chan batchWithAckFn) {
	f.batchChan.Store(c)
}

// NewFranzReaderUnorderedFromConfig attempts to instantiate a new
// FranzReaderUnordered reader from a parsed config.
func NewFranzReaderUnorderedFromConfig(conf *service.ParsedConfig, res *service.Resources, opts ...kgo.Opt) (*FranzReaderUnordered, error) {
	f := FranzReaderUnordered{
		res:     res,
		log:     res.Logger(),
		shutSig: shutdown.NewSignaller(),
	}
	f.clientOpts = append(f.clientOpts, opts...)

	f.consumerGroup, _ = conf.FieldString(kruFieldConsumerGroup)

	var err error
	if f.checkpointLimit, err = conf.FieldInt(kruFieldCheckpointLimit); err != nil {
		return nil, err
	}

	if f.commitPeriod, err = conf.FieldDuration(kruFieldCommitPeriod); err != nil {
		return nil, err
	}

	if f.batchPolicy, err = conf.FieldBatchPolicy(kruFieldBatching); err != nil {
		return nil, err
	}

	if f.multiHeader, err = conf.FieldBool(kruFieldMultiHeader); err != nil {
		return nil, err
	}

	return &f, nil
}

type msgWithRecord struct {
	msg *service.Message
	r   *kgo.Record
}

func (f *FranzReaderUnordered) recordToMessage(record *kgo.Record) *msgWithRecord {
	msg := FranzRecordToMessageV0(record, f.multiHeader)

	// The record lives on for checkpointing, but we don't need the contents
	// going forward so discard these. This looked fine to me but could
	// potentially be a source of problems so treat this as sus.
	record.Key = nil
	record.Value = nil

	return &msgWithRecord{
		msg: msg,
		r:   record,
	}
}

//------------------------------------------------------------------------------

type partitionTracker struct {
	batcherLock    sync.Mutex
	topBatchRecord *kgo.Record
	batcher        *service.Batcher

	checkpointerLock sync.Mutex
	checkpointer     *checkpoint.Uncapped[*kgo.Record]

	outBatchChan chan<- batchWithAckFn
	commitFn     func(r *kgo.Record)

	shutSig *shutdown.Signaller
}

func newPartitionTracker(batcher *service.Batcher, batchChan chan<- batchWithAckFn, commitFn func(r *kgo.Record)) *partitionTracker {
	pt := &partitionTracker{
		batcher:      batcher,
		checkpointer: checkpoint.NewUncapped[*kgo.Record](),
		outBatchChan: batchChan,
		commitFn:     commitFn,
		shutSig:      shutdown.NewSignaller(),
	}
	go pt.loop()
	return pt
}

func (p *partitionTracker) loop() {
	defer func() {
		if p.batcher != nil {
			p.batcher.Close(context.Background())
		}
		p.shutSig.TriggerHasStopped()
	}()

	// No need to loop when there's no batcher for async writes.
	if p.batcher == nil {
		return
	}

	var flushBatch <-chan time.Time
	var flushBatchTicker *time.Ticker
	adjustTimedFlush := func() {
		if flushBatch != nil || p.batcher == nil {
			return
		}

		tNext, exists := p.batcher.UntilNext()
		if !exists {
			if flushBatchTicker != nil {
				flushBatchTicker.Stop()
				flushBatchTicker = nil
			}
			return
		}

		if flushBatchTicker != nil {
			flushBatchTicker.Reset(tNext)
		} else {
			flushBatchTicker = time.NewTicker(tNext)
		}
		flushBatch = flushBatchTicker.C
	}

	closeAtLeisureCtx, done := p.shutSig.SoftStopCtx(context.Background())
	defer done()

	for {
		adjustTimedFlush()
		select {
		case <-flushBatch:
			var sendBatch service.MessageBatch
			var sendRecord *kgo.Record

			// Wrap this in a closure to make locking/unlocking easier.
			func() {
				p.batcherLock.Lock()
				defer p.batcherLock.Unlock()

				flushBatch = nil
				if tNext, exists := p.batcher.UntilNext(); !exists || tNext > 1 {
					// This can happen if a pushed message triggered a batch before
					// the last known flush period. In this case we simply enter the
					// loop again which readjusts our flush batch timer.
					return
				}

				if sendBatch, _ = p.batcher.Flush(closeAtLeisureCtx); len(sendBatch) == 0 {
					return
				}
				sendRecord = p.topBatchRecord
				p.topBatchRecord = nil
			}()

			if len(sendBatch) > 0 {
				if err := p.sendBatch(closeAtLeisureCtx, sendBatch, sendRecord); err != nil {
					return
				}
			}
		case <-p.shutSig.SoftStopChan():
			return
		}
	}
}

func (p *partitionTracker) sendBatch(ctx context.Context, b service.MessageBatch, r *kgo.Record) error {
	p.checkpointerLock.Lock()
	releaseFn := p.checkpointer.Track(r, int64(len(b)))
	p.checkpointerLock.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.outBatchChan <- batchWithAckFn{
		batch: b,
		onAck: func() {
			p.checkpointerLock.Lock()
			releaseRecord := releaseFn()
			p.checkpointerLock.Unlock()

			if releaseRecord != nil && *releaseRecord != nil {
				p.commitFn(*releaseRecord)
			}
		},
	}:
	}
	return nil
}

func (p *partitionTracker) add(ctx context.Context, m *msgWithRecord, limit int) (pauseFetch bool) {
	var sendBatch service.MessageBatch
	if p.batcher != nil {
		// Wrap this in a closure to make locking/unlocking easier.
		func() {
			p.batcherLock.Lock()
			defer p.batcherLock.Unlock()

			if p.batcher.Add(m.msg) {
				// Batch triggered, we flush it here synchronously.
				sendBatch, _ = p.batcher.Flush(ctx)
			} else {
				// Otherwise store the latest record as the representative of the
				// pending batch offset. This will be used by the timer based
				// flushing mechanism within loop() if applicable.
				p.topBatchRecord = m.r
			}
		}()
	} else {
		sendBatch = service.MessageBatch{m.msg}
	}

	if len(sendBatch) > 0 {
		// Ignoring in the error here is fine, it implies shut down has been
		// triggered and we would only acknowledge the message by committing it
		// if it were successfully delivered.
		_ = p.sendBatch(ctx, sendBatch, m.r)
	}

	p.checkpointerLock.Lock()
	pauseFetch = p.checkpointer.Pending() >= int64(limit)
	p.checkpointerLock.Unlock()
	return
}

func (p *partitionTracker) pauseFetch(limit int) (pauseFetch bool) {
	p.checkpointerLock.Lock()
	pauseFetch = p.checkpointer.Pending() >= int64(limit)
	p.checkpointerLock.Unlock()
	return
}

func (p *partitionTracker) close(ctx context.Context) error {
	p.shutSig.TriggerSoftStop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.shutSig.HasStoppedChan():
	}
	return nil
}

//------------------------------------------------------------------------------

type checkpointTracker struct {
	mut    sync.Mutex
	topics map[string]map[int32]*partitionTracker

	res       *service.Resources
	batchChan chan<- batchWithAckFn
	commitFn  func(r *kgo.Record)
	batchPol  service.BatchPolicy
}

func newCheckpointTracker(
	res *service.Resources,
	batchChan chan<- batchWithAckFn,
	releaseFn func(r *kgo.Record),
	batchPol service.BatchPolicy,
) *checkpointTracker {
	return &checkpointTracker{
		topics:    map[string]map[int32]*partitionTracker{},
		res:       res,
		batchChan: batchChan,
		commitFn:  releaseFn,
		batchPol:  batchPol,
	}
}

func (c *checkpointTracker) close() {
	c.mut.Lock()
	defer c.mut.Unlock()

	for _, partitions := range c.topics {
		for _, tracker := range partitions {
			_ = tracker.close(context.Background())
		}
	}
}

func (c *checkpointTracker) addRecord(ctx context.Context, m *msgWithRecord, limit int) (pauseFetch bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	topicTracker := c.topics[m.r.Topic]
	if topicTracker == nil {
		topicTracker = map[int32]*partitionTracker{}
		c.topics[m.r.Topic] = topicTracker
	}

	partTracker := topicTracker[m.r.Partition]
	if partTracker == nil {
		var batcher *service.Batcher
		if !c.batchPol.IsNoop() {
			var err error
			if batcher, err = c.batchPol.NewBatcher(c.res); err != nil {
				c.res.Logger().Errorf("Failed to initialise batch policy: %v, falling back to individual message delivery", err)
				batcher = nil
			}
		}
		partTracker = newPartitionTracker(batcher, c.batchChan, c.commitFn)
		topicTracker[m.r.Partition] = partTracker
	}

	return partTracker.add(ctx, m, limit)
}

func (c *checkpointTracker) pauseFetch(topic string, partition int32, limit int) bool {
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

func (c *checkpointTracker) removeTopicPartitions(ctx context.Context, m map[string][]int32) {
	c.mut.Lock()
	defer c.mut.Unlock()

	for topicName, lostTopic := range m {
		trackedTopic, exists := c.topics[topicName]
		if !exists {
			continue
		}
		for _, lostPartition := range lostTopic {
			if trackedPartition, exists := trackedTopic[lostPartition]; exists {
				_ = trackedPartition.close(ctx)
			}
			delete(trackedTopic, lostPartition)
		}
		if len(trackedTopic) == 0 {
			delete(c.topics, topicName)
		}
	}
}

//------------------------------------------------------------------------------

// Connect to the kafka seed brokers.
func (f *FranzReaderUnordered) Connect(ctx context.Context) error {
	if f.getBatchChan() != nil {
		return nil
	}

	if f.shutSig.IsSoftStopSignalled() {
		f.shutSig.TriggerHasStopped()
		return service.ErrEndOfInput
	}

	batchChan := make(chan batchWithAckFn)

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
	checkpoints := newCheckpointTracker(f.res, batchChan, commitFn, f.batchPolicy)

	var clientOpts []kgo.Opt
	clientOpts = append(clientOpts, f.clientOpts...)

	if f.consumerGroup != "" {
		clientOpts = append(clientOpts,
			kgo.OnPartitionsRevoked(func(rctx context.Context, c *kgo.Client, m map[string][]int32) {
				if commitErr := c.CommitMarkedOffsets(rctx); commitErr != nil {
					f.log.Errorf("Commit error on partition revoke: %v", commitErr)
				}
				checkpoints.removeTopicPartitions(rctx, m)
			}),
			kgo.OnPartitionsLost(func(rctx context.Context, _ *kgo.Client, m map[string][]int32) {
				// No point trying to commit our offsets, just clean up our topic map
				checkpoints.removeTopicPartitions(rctx, m)
			}),
			kgo.ConsumerGroup(f.consumerGroup),
			kgo.AutoCommitMarks(),
			kgo.AutoCommitInterval(f.commitPeriod),
			kgo.WithLogger(&KGoLogger{f.log}),
		)
	}

	var err error
	if cl, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err = cl.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	topics := cl.GetConsumeTopics()
	if len(topics) > 0 {
		f.log.Debugf("Consuming from topics: %s", topics)
	} else {
		f.log.Warn("Topic filter did not match any existing topics")
	}

	go func() {
		defer func() {
			cl.Close()
			checkpoints.close()
			f.storeBatchChan(nil)
			close(batchChan)
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
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if checkpoints.addRecord(closeCtx, f.recordToMessage(record), f.checkpointLimit) {
					pauseTopicPartitions[record.Topic] = append(pauseTopicPartitions[record.Topic], record.Partition)
				}
			}

			// Walk all the disabled topic partitions and check whether any of
			// them can be resumed.
			resumeTopicPartitions := map[string][]int32{}
			for pausedTopic, pausedPartitions := range cl.PauseFetchPartitions(pauseTopicPartitions) {
				for _, pausedPartition := range pausedPartitions {
					if !checkpoints.pauseFetch(pausedTopic, pausedPartition, f.checkpointLimit) {
						resumeTopicPartitions[pausedTopic] = append(resumeTopicPartitions[pausedTopic], pausedPartition)
					}
				}
			}
			if len(resumeTopicPartitions) > 0 {
				cl.ResumeFetchPartitions(resumeTopicPartitions)
			}
		}
	}()

	f.storeBatchChan(batchChan)
	return nil
}

// ReadBatch attempts to extract a batch of messages from the target topics.
func (f *FranzReaderUnordered) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	batchChan := f.getBatchChan()
	if batchChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var mAck batchWithAckFn
	var open bool
	select {
	case mAck, open = <-batchChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	return mAck.batch, func(ctx context.Context, res error) error {
		// Res will always be nil because we initialize with service.AutoRetryNacks
		mAck.onAck()
		return nil
	}, nil
}

// Close underlying connections.
func (f *FranzReaderUnordered) Close(ctx context.Context) error {
	go func() {
		f.shutSig.TriggerSoftStop()
		if f.getBatchChan() == nil {
			// If the record chan is already nil then we might've not been
			// connected, so force the shutdown complete signal.
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
