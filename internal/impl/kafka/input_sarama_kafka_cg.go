package kafka

import (
	"context"
	"io"
	"time"

	"github.com/Shopify/sarama"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *kafkaReader) Setup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = sesh
	k.cMut.Unlock()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have
// exited but before the offsets are committed for the very last time.
func (k *kafkaReader) Cleanup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = nil
	k.cMut.Unlock()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *kafkaReader) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic, partition := claim.Topic(), claim.Partition()
	k.log.Debugf("Consuming messages from topic '%v' partition '%v'\n", topic, partition)
	defer k.log.Debugf("Stopped consuming messages from topic '%v' partition '%v'\n", topic, partition)

	latestOffset := claim.InitialOffset()
	batchPolicy, err := policy.New(k.conf.Batching, k.mgr.IntoPath("kafka", "batching"))
	if err != nil {
		k.log.Errorf("Failed to initialise batch policy: %v.\n", err)
		// The consume claim gets reopened immediately so let's try and
		// avoid a busy loop (this should never happen anyway).
		<-time.After(time.Second)
		return err
	}
	defer batchPolicy.Close(context.Background())

	var nextTimedBatchChan <-chan time.Time
	var flushBatch func(context.Context, chan<- asyncMessage, message.Batch, int64) bool
	if k.conf.CheckpointLimit > 1 {
		flushBatch = k.asyncCheckpointer(claim.Topic(), claim.Partition())
	} else {
		flushBatch = k.syncCheckpointer(claim.Topic(), claim.Partition())
	}

	for {
		if nextTimedBatchChan == nil {
			if tNext := batchPolicy.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			if !flushBatch(sess.Context(), k.msgChan, batchPolicy.Flush(sess.Context()), latestOffset+1) {
				return nil
			}
		case data, open := <-claim.Messages():
			if !open {
				return nil
			}

			latestOffset = data.Offset
			part := dataToPart(claim.HighWaterMarkOffset(), data, k.conf.MultiHeader)

			if batchPolicy.Add(part) {
				nextTimedBatchChan = nil
				if !flushBatch(sess.Context(), k.msgChan, batchPolicy.Flush(sess.Context()), latestOffset+1) {
					return nil
				}
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}

//------------------------------------------------------------------------------

func (k *kafkaReader) connectBalancedTopics(ctx context.Context, config *sarama.Config) error {
	// Start a new consumer group
	group, err := sarama.NewConsumerGroup(k.addresses, k.conf.ConsumerGroup, config)
	if err != nil {
		return err
	}

	// Handle errors
	go func() {
		for {
			gerr, open := <-group.Errors()
			if !open {
				return
			}
			if gerr != nil {
				k.log.Errorf("Kafka group message recv error: %v\n", gerr)
				if cerr, ok := gerr.(*sarama.ConsumerError); ok {
					if cerr.Err == sarama.ErrUnknownMemberId {
						// Sarama doesn't seem to recover from this error.
						go k.closeGroupAndConsumers()
					}
				}
			}
		}
	}()

	consumerDoneCtx, finishedFn := context.WithCancel(context.Background())
	go func() {
		defer finishedFn()
	groupLoop:
		for {
			ctx, doneFn := context.WithCancel(context.Background())

			k.cMut.Lock()
			k.consumerCloseFn = doneFn
			k.cMut.Unlock()

			k.log.Debugln("Starting consumer group")
			gerr := group.Consume(ctx, k.balancedTopics, k)
			select {
			case <-ctx.Done():
				break groupLoop
			default:
			}
			doneFn()
			if gerr != nil {
				if gerr != io.EOF {
					k.log.Errorf("Kafka group session error: %v\n", gerr)
				}
				break groupLoop
			}
		}
		k.log.Debugln("Closing consumer group")

		group.Close()

		k.cMut.Lock()
		if k.msgChan != nil {
			close(k.msgChan)
			k.msgChan = nil
		}
		k.cMut.Unlock()
	}()

	k.msgChan = make(chan asyncMessage)
	k.consumerDoneCtx = consumerDoneCtx
	k.log.Infof("Consuming kafka topics %v from brokers %s as group '%v'\n", k.balancedTopics, k.addresses, k.conf.ConsumerGroup)
	return nil
}
