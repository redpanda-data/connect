package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/benthosdev/benthos/v4/public/service"
)

type closureOffsetTracker struct {
	fn func(string, int32, int64, string)
}

func (c *closureOffsetTracker) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	c.fn(topic, partition, offset, metadata)
}

func (k *kafkaReader) runPartitionConsumer(
	ctx context.Context,
	wg *sync.WaitGroup,
	topic string,
	partition int32,
	consumer sarama.PartitionConsumer,
) {
	k.mgr.Logger().Debugf("Consuming messages from topic '%v' partition '%v'\n", topic, partition)
	defer k.mgr.Logger().Debugf("Stopped consuming messages from topic '%v' partition '%v'\n", topic, partition)
	defer wg.Done()

	batchPolicy, err := k.batching.NewBatcher(k.mgr)
	if err != nil {
		k.mgr.Logger().Errorf("Failed to initialise batch policy: %v, falling back to no policy.\n", err)
		conf := service.BatchPolicy{Count: 1}
		if batchPolicy, err = conf.NewBatcher(k.mgr); err != nil {
			panic(err)
		}
	}
	defer batchPolicy.Close(context.Background())

	var nextTimedBatchChan <-chan time.Time
	var flushBatch func(context.Context, chan<- asyncMessage, service.MessageBatch, int64) bool
	if k.checkpointLimit > 1 {
		flushBatch = k.asyncCheckpointer(topic, partition)
	} else {
		flushBatch = k.syncCheckpointer(topic, partition)
	}

	var latestOffset int64

partMsgLoop:
	for {
		if nextTimedBatchChan == nil {
			if tNext, exists := batchPolicy.UntilNext(); exists {
				nextTimedBatchChan = time.After(tNext)
			}
		}
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := batchPolicy.Flush(ctx)
			if err != nil {
				k.mgr.Logger().Debugf("Timed flush batch error: %w", err)
				break partMsgLoop
			}
			if !flushBatch(ctx, k.msgChan, flushedBatch, latestOffset+1) {
				break partMsgLoop
			}
		case data, open := <-consumer.Messages():
			if !open {
				break partMsgLoop
			}
			k.mgr.Logger().Tracef("Received message from topic %v partition %v\n", topic, partition)

			latestOffset = data.Offset
			part := dataToPart(consumer.HighWaterMarkOffset(), data, k.multiHeader)

			if batchPolicy.Add(part) {
				nextTimedBatchChan = nil
				flushedBatch, err := batchPolicy.Flush(ctx)
				if err != nil {
					k.mgr.Logger().Debugf("Flush batch error: %w", err)
					break partMsgLoop
				}
				if !flushBatch(ctx, k.msgChan, flushedBatch, latestOffset+1) {
					break partMsgLoop
				}
			}
		case err, open := <-consumer.Errors():
			if !open {
				break partMsgLoop
			}
			if err != nil && !strings.HasSuffix(err.Error(), "EOF") {
				k.mgr.Logger().Errorf("Kafka message recv error: %v\n", err)
			}
		case <-ctx.Done():
			break partMsgLoop
		}
	}
	// Drain everything that's left.
	for range consumer.Messages() {
	}
	for range consumer.Errors() {
	}
}

func (k *kafkaReader) offsetVersion() int16 {
	// - 0 (kafka 0.8.1 and later)
	// - 1 (kafka 0.8.2 and later)
	// - 2 (kafka 0.9.0 and later)
	// - 3 (kafka 0.11.0 and later)
	// - 4 (kafka 2.0.0 and later)
	var v int16 = 1
	// TODO: Increase this if we drop support for v0.8.2, or if we allow a
	// custom retention period.
	return v
}

func (k *kafkaReader) offsetPartitionPutRequest(consumerGroup string) *sarama.OffsetCommitRequest {
	v := k.offsetVersion()
	req := &sarama.OffsetCommitRequest{
		ConsumerGroup:           consumerGroup,
		Version:                 v,
		ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
		ConsumerID:              "",
	}
	return req
}

func (k *kafkaReader) connectExplicitTopics(ctx context.Context, config *sarama.Config) (err error) {
	var coordinator *sarama.Broker
	var consumer sarama.Consumer
	var client sarama.Client

	defer func() {
		if err != nil {
			if consumer != nil {
				consumer.Close()
			}
			if coordinator != nil {
				coordinator.Close()
			}
			if client != nil {
				client.Close()
			}
		}
	}()

	if client, err = sarama.NewClient(k.addresses, config); err != nil {
		return err
	}
	if k.consumerGroup != "" {
		if coordinator, err = client.Coordinator(k.consumerGroup); err != nil {
			return err
		}
	}
	if consumer, err = sarama.NewConsumerFromClient(client); err != nil {
		return err
	}

	offsetGetReq := sarama.OffsetFetchRequest{
		Version:       k.offsetVersion(),
		ConsumerGroup: k.consumerGroup,
	}
	for topic, parts := range k.topicPartitions {
		for _, part := range parts {
			offsetGetReq.AddPartition(topic, part)
		}
	}

	var offsetRes *sarama.OffsetFetchResponse
	if coordinator != nil {
		if offsetRes, err = coordinator.FetchOffset(&offsetGetReq); err != nil {
			if errors.Is(err, io.EOF) {
				offsetRes = &sarama.OffsetFetchResponse{}
			} else {
				return fmt.Errorf("failed to acquire offsets from broker: %v", err)
			}
		}
	} else {
		offsetRes = &sarama.OffsetFetchResponse{}
	}

	offsetPutReq := k.offsetPartitionPutRequest(k.consumerGroup)
	offsetTracker := &closureOffsetTracker{
		// Note: We don't need to wrap this call in a mutex lock because the
		// checkpointer that uses it already does this, but it's not
		// particularly clear, hence this comment.
		fn: func(topic string, partition int32, offset int64, metadata string) {
			// TODO: Since offsetVersion() returns v1 we can set leaderEpoch to 0 for now
			// Per sarama and kafka protocol docs leaderEpoch is in v7 payload
			offsetPutReq.AddBlock(topic, partition, offset, time.Now().Unix(), metadata)
		},
	}

	partConsumers := []sarama.PartitionConsumer{}
	consumerWG := sync.WaitGroup{}
	msgChan := make(chan asyncMessage)
	ctx, doneFn := context.WithCancel(context.Background())

	for topic, partitions := range k.topicPartitions {
		for _, partition := range partitions {
			topic := topic
			partition := partition

			offset := sarama.OffsetNewest
			if k.startFromOldest {
				offset = sarama.OffsetOldest
			}
			if block := offsetRes.GetBlock(topic, partition); block != nil {
				if block.Err == sarama.ErrNoError {
					if block.Offset > 0 {
						offset = block.Offset
					}
				} else {
					k.mgr.Logger().Debugf("Failed to acquire offset for topic %v partition %v: %v\n", topic, partition, block.Err)
				}
			} else {
				k.mgr.Logger().Debugf("Failed to acquire offset for topic %v partition %v\n", topic, partition)
			}

			var partConsumer sarama.PartitionConsumer
			if partConsumer, err = consumer.ConsumePartition(topic, partition, offset); err != nil {
				// TODO: Actually verify the error was caused by a non-existent offset
				if k.startFromOldest {
					offset = sarama.OffsetOldest
					k.mgr.Logger().Warnf("Failed to read from stored offset, restarting from oldest offset: %v\n", err)
				} else {
					offset = sarama.OffsetNewest
					k.mgr.Logger().Warnf("Failed to read from stored offset, restarting from newest offset: %v\n", err)
				}
				if partConsumer, err = consumer.ConsumePartition(topic, partition, offset); err != nil {
					doneFn()
					return fmt.Errorf("failed to consume topic %v partition %v: %v", topic, partition, err)
				}
			}

			consumerWG.Add(1)
			partConsumers = append(partConsumers, partConsumer)
			go k.runPartitionConsumer(ctx, &consumerWG, topic, partition, partConsumer)
		}
	}

	doneCtx, doneFn := context.WithCancel(context.Background())
	go func() {
		defer doneFn()
		looping := true
		for looping {
			select {
			case <-ctx.Done():
				looping = false
			case <-time.After(k.commitPeriod):
			}
			k.cMut.Lock()
			putReq := offsetPutReq
			offsetPutReq = k.offsetPartitionPutRequest(k.consumerGroup)
			k.cMut.Unlock()
			if coordinator != nil {
				if _, err := coordinator.CommitOffset(putReq); err != nil {
					k.mgr.Logger().Errorf("Failed to commit offsets: %v\n", err)
				}
			}
		}
		for _, consumer := range partConsumers {
			consumer.AsyncClose()
		}
		consumerWG.Done()

		k.cMut.Lock()
		if k.msgChan != nil {
			close(k.msgChan)
			k.msgChan = nil
		}
		k.cMut.Unlock()

		if coordinator != nil {
			coordinator.Close()
		}
		client.Close()
	}()

	k.consumerCloseFn = doneFn
	k.consumerDoneCtx = doneCtx
	k.session = offsetTracker
	k.msgChan = msgChan
	return nil
}
