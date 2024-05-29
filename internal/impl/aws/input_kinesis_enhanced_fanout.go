package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"
)

type efoConsumer struct {
	consumerName string
}

func NewEfoConsumer(consumerName string) *efoConsumer {
	return &efoConsumer{
		consumerName: consumerName,
	}
}

func (k *kinesisReader) getEfoConsumer(ctx context.Context, info streamInfo, consumerName string) (*string, error) {
	res, err := k.svc.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerName: &consumerName,
		StreamARN:    &info.arn,
	})
	var consumer *string
	// if the consumer isnt already registered, register it
	if err != nil || res.ConsumerDescription == nil {
		if k.conf.EFOConsumerRegister {
			streamOutput, err := k.svc.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
				ConsumerName: &consumerName,
				StreamARN:    &info.arn,
			})

			if err != nil {
				return nil, err
			}

			// As RegisterStreamConsumer is async, wait at most a minute for the consumer to be registered
			timeoutChan := time.NewTimer(time.Minute)
			streamActive := streamOutput.Consumer.ConsumerStatus != types.ConsumerStatusActive
			for !streamActive {
				select {
				case <-timeoutChan.C:
					return nil, fmt.Errorf("timed out after waiting 1 minute for newly registered stream consumer %v to be active", consumerName)
				default:
					res, _ = k.svc.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
						ConsumerName: &consumerName,
						StreamARN:    &info.arn,
					})
					if res != nil {
						streamActive = res.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive
					}
					// Dont spam it
					time.Sleep(time.Second * 5)
				}
			}

			if streamOutput.Consumer == nil {
				return nil, errors.New("failed to register consumer - RegisterStreamConsumer returned nil")
			}

			consumer = streamOutput.Consumer.ConsumerARN
		} else {
			return nil, fmt.Errorf("failed to describe kinesis consumer - consumer with name %v not found", consumerName)
		}

	} else {
		consumer = res.ConsumerDescription.ConsumerARN
	}

	return consumer, nil
}

func (k *kinesisReader) runEfoConsumer(wg *sync.WaitGroup, info streamInfo, shardID, startingSequence string) (initErr error) {
	//register consumer
	consumerARN, err := k.getEfoConsumer(k.ctx, info, k.conf.EFOConsumerName)
	if err != nil {
		return err
	}
	iterType := types.ShardIteratorTypeTrimHorizon
	if !k.conf.StartFromOldest {
		iterType = types.ShardIteratorTypeLatest
	}
	var seq *string
	if len(startingSequence) > 0 {
		iterType = types.ShardIteratorTypeAfterSequenceNumber
		seq = &startingSequence
	}

	subToShard := func() (*kinesis.SubscribeToShardOutput, error) {
		return k.svc.SubscribeToShard(k.ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN: consumerARN,
			ShardId:     &shardID,
			StartingPosition: &types.StartingPosition{
				Type:           iterType,
				SequenceNumber: seq,
			},
		})
	}
	// busy read from consumer
	// though we need to recall this every 5 mins
	subOutput, err := subToShard()
	if err != nil {
		return err
	}
	pendingChan := make(chan []types.Record, 10)

	go func() {
		lastSubTime := time.Now()
		for {
			select {
			case <-k.ctx.Done():
				subOutput.GetStream().Close()
				close(pendingChan)
				return

			case ev := <-subOutput.GetStream().Events():
				if event, ok := ev.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); !ok {
					if ev == nil {
						continue
					}
					k.log.Errorf("Received unexpected non nil event type: %T", ev)
				} else {
					seq = event.Value.ContinuationSequenceNumber
					if len(event.Value.Records) > 0 {
						pendingChan <- event.Value.Records
					}
				}
			default:
				// Connection is only alive for 5 minutes, so give enough time
				if time.Since(lastSubTime) >= 4*time.Minute {
					subOutput.GetStream().Close()
					subOutput, err = subToShard()
					if err != nil {
						k.log.Errorf("Resubscribe to EFO shard failed: %v", err)
						close(pendingChan)
						return
					}
				}
				lastSubTime = time.Now()
			}
		}
	}()
	// copy code from runConsumer and use a channel to pass to pending
	defer func() {
		if initErr != nil {
			wg.Done()
			if _, err := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, startingSequence, true); err != nil {
				k.log.Errorf("Failed to gracefully yield checkpoint: %v\n", err)
			}
		}
	}()

	// Stores records, batches them up, and provides the batches for dispatch,
	// whilst ensuring only N records are in flight at a given time.
	var recordBatcher *awsKinesisRecordBatcher
	if recordBatcher, initErr = k.newAWSKinesisRecordBatcher(info, shardID, startingSequence); initErr != nil {
		return initErr
	}

	// Keeps track of retry attempts.
	boff := k.boffPool.Get().(backoff.BackOff)

	// Stores consumed records that have yet to be added to the batcher.
	var pending []types.Record

	// Keeps track of the latest state of the consumer.
	state := awsKinesisConsumerConsuming
	var pendingMsg asyncMessage

	unblockedChan, blockedChan := make(chan time.Time), make(chan time.Time)
	close(unblockedChan)

	// Channels (and contexts) representing the four main actions of the
	// consumer goroutine:
	// 1. Timed batches, this might be nil when timed batches are disabled.
	// 2. Record pulling, this might be unblocked (closed channel) when we run
	//    out of pending records, or a timed channel when our last attempt
	//    yielded zero records.
	// 3. Message flush, this is the target of our current batched message, and
	//    is nil when our current batched message is a zero value (we don't have
	//    one prepared).
	// 4. Next commit, is "done" when the next commit is due.
	var nextTimedBatchChan <-chan time.Time
	var nextPullChan <-chan time.Time = unblockedChan
	var nextFlushChan chan<- asyncMessage
	commitCtx, commitCtxClose := context.WithTimeout(k.ctx, k.commitPeriod)

	go func() {
		defer func() {
			commitCtxClose()
			recordBatcher.Close(context.Background(), state == awsKinesisConsumerFinished)
			boff.Reset()
			k.boffPool.Put(boff)

			reason := ""
			switch state {
			case awsKinesisConsumerFinished:
				k.log.Info("kinesis marked as closed")
				reason = " because the shard is closed"
				if err := k.checkpointer.Delete(k.ctx, info.id, shardID); err != nil {
					k.log.Errorf("Failed to remove checkpoint for finished stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := k.checkpointer.Yield(k.ctx, info.id, shardID, recordBatcher.GetSequence()); err != nil {
					k.log.Errorf("Failed to yield checkpoint for stolen stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			case awsKinesisConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := k.checkpointer.Checkpoint(context.Background(), info.id, shardID, recordBatcher.GetSequence(), true); err != nil {
					k.log.Errorf("Failed to store final checkpoint for stream '%v' shard '%v': %v", info.id, shardID, err)
				}
			}

			wg.Done()
			k.log.Debugf("Closing stream '%v' shard '%v' as client '%v'%v", info.id, shardID, k.checkpointer.clientID, reason)
		}()

		k.log.Debugf("Consuming stream '%v' shard '%v' as client '%v'", info.id, shardID, k.checkpointer.clientID)

		// Switches our pull chan to unblocked only if it's currently blocked,
		// as otherwise it's set to a timed channel that we do not want to
		// disturb.
		unblockPullChan := func() {
			if nextPullChan == blockedChan {
				nextPullChan = unblockedChan
			}
		}

		for {
			var err error
			// Recieve on the channel being populated by the EFO Stream we've subscribed to.
			// If its closed, then assume the consumer is finished

			if state == awsKinesisConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan {
				select {
				case p, ok := <-pendingChan:
					if !ok {
						state = awsKinesisConsumerFinished
					} else {
						if len(p) > 0 {
							pending = p
						}
					}
				default:
					// if there is nothing on the stream, unblock
				}
				if len(pending) == 0 {
					nextPullChan = time.After(boff.NextBackOff())
				} else {
					boff.Reset()
					nextPullChan = blockedChan
				}

			} else {
				unblockPullChan()
			}

			if pendingMsg.msg == nil {
				// If our consumer is finished and we've run out of pending
				// records then we're done.
				if len(pending) == 0 && state == awsKinesisConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(k.ctx); pendingMsg.msg == nil {
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var r types.Record
					for i, r = range pending {
						k.log.Infof("adding record to batcher %v", string(r.Data))
						if recordBatcher.AddRecord(r) {
							if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
								k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
							}
							break
						}
					}
					if pending = pending[i+1:]; len(pending) == 0 {
						unblockPullChan()
					}
				} else {
					unblockPullChan()
				}
			}

			if pendingMsg.msg != nil {
				nextFlushChan = k.msgChan
			} else {
				nextFlushChan = nil
			}

			if nextTimedBatchChan == nil {
				k.log.Infof("no timed batcher chan")
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
					k.log.Infof("new timed chan %v", tNext)
				} else {
					k.log.Infof("no timed batcher exists")
				}
			}

			select {
			case <-commitCtx.Done():
				if k.ctx.Err() != nil {
					// It could've been our parent context that closed, in which
					// case we exit.
					state = awsKinesisConsumerClosing
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(k.ctx, k.commitPeriod)

				stillOwned, err := k.checkpointer.Checkpoint(k.ctx, info.id, shardID, recordBatcher.GetSequence(), false)
				if err != nil {
					k.log.Errorf("Failed to store checkpoint for Kinesis stream '%v' shard '%v': %v", info.id, shardID, err)
				} else if !stillOwned {
					state = awsKinesisConsumerYielding
					return
				}
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}
			case <-nextPullChan:
				nextPullChan = unblockedChan
			case <-k.ctx.Done():
				state = awsKinesisConsumerClosing
				return
			}
		}
	}()
	return nil
}
