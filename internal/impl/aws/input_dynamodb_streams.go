package aws

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	dynsiface "github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// DynamoDB Streams Input DynDB Fields
	dynsdbFieldTable              = "table"
	dynsdbFieldCreate             = "create"
	dynsdbFieldReadCapacityUnits  = "read_capacity_units"
	dynsdbFieldWriteCapacityUnits = "write_capacity_units"
	dynsdbFieldBillingMode        = "billing_mode"

	// DynamoDB Streams Input Fields
	dynsFieldDynamoDB        = "dynamodb"
	dynsFieldStreams         = "streams"
	dynsFieldCheckpointLimit = "checkpoint_limit"
	dynsFieldCommitPeriod    = "commit_period"
	dynsFieldLeasePeriod     = "lease_period"
	dynsFieldRebalancePeriod = "rebalance_period"
	dynsFieldStartFromOldest = "start_from_oldest"
	dynsFieldBatching        = "batching"
)

type dynsConfig struct {
	Streams         []string
	DynamoDB        dynamoCheckpointConfig
	CheckpointLimit int
	CommitPeriod    string
	LeasePeriod     string
	RebalancePeriod string
	StartFromOldest bool
}

func dynamoDBStreamsInputConfigFromParsed(pConf *service.ParsedConfig) (conf dynsConfig, err error) {
	if conf.Streams, err = pConf.FieldStringList(dynsFieldStreams); err != nil {
		return
	}
	if pConf.Contains(dynsFieldDynamoDB) {
		if conf.DynamoDB, err = inputDynamoDBConfigFromParsed(pConf.Namespace(dynsFieldDynamoDB)); err != nil {
			return
		}
	}
	if conf.CheckpointLimit, err = pConf.FieldInt(dynsFieldCheckpointLimit); err != nil {
		return
	}
	if conf.CommitPeriod, err = pConf.FieldString(dynsFieldCommitPeriod); err != nil {
		return
	}
	if conf.LeasePeriod, err = pConf.FieldString(dynsFieldLeasePeriod); err != nil {
		return
	}
	if conf.RebalancePeriod, err = pConf.FieldString(dynsFieldRebalancePeriod); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(dynsFieldStartFromOldest); err != nil {
		return
	}
	return
}

func dynamoDBStreamsInputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Version("4.22.0").
		Categories("Services", "AWS").
		Summary("Receive messages from one or more DynamoDB Streams.").
		Description(`
Consumes messages from one or more DynamoDB Streams either by automatically balancing shards across other instances of this input. The latest message sequence consumed by this input is stored within a [DynamoDB table](#table-schema), which allows it to resume at the correct sequence of the shard during restarts. This table is also used for coordination across distributed inputs when shard balancing.

Benthos will not store a consumed sequence unless it is acknowledged at the output level, which ensures at-least-once delivery guarantees.

### Ordering

By default messages of a shard can be processed in parallel, up to a limit determined by the field `+"`checkpoint_limit`"+`. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step. When doing so it is recommended that you perform batching at this component for performance as it will not be possible to batch lock-stepped messages at the output level.

### Table Schema

It's possible to configure Benthos to create the DynamoDB table required for coordination if it does not already exist. However, if you wish to create this yourself (recommended) then create a table with a string HASH key `+"`StreamID`"+` and a string RANGE key `+"`ShardID`"+`.

### Batching

Use the `+"`batching`"+` fields to configure an optional [batching policy](/docs/configuration/batching#batch-policy). Each stream shard will be batched separately in order to ensure that acknowledgements aren't contaminated.
`).Fields(
		service.NewStringListField(dynsFieldStreams).
			Description("One or more DynamoDB Streams to consume from. Shards of a stream are automatically balanced across consumers by coordinating through the provided DynamoDB table. Multiple comma separated streams can be listed in a single element. Shards are automatically distributed across consumers of a stream by coordinating through the provided DynamoDB table."),
		service.NewObjectField(dynsFieldDynamoDB,
			service.NewStringField(dynsdbFieldTable).
				Description("The name of the table to access.").
				Default(""),
			service.NewBoolField(dynsdbFieldCreate).
				Description("Whether, if the table does not exist, it should be created.").
				Default(false),
			service.NewStringEnumField(dynsdbFieldBillingMode, "PROVISIONED", "PAY_PER_REQUEST").
				Description("When creating the table determines the billing mode.").
				Default("PAY_PER_REQUEST").
				Advanced(),
			service.NewIntField(dynsdbFieldReadCapacityUnits).
				Description("Set the provisioned read capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
			service.NewIntField(dynsdbFieldWriteCapacityUnits).
				Description("Set the provisioned write capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
				Default(0).
				Advanced(),
		).
			Description("Determines the table used for storing and accessing the latest consumed sequence for shards, and for coordinating balanced consumers of streams."),
		service.NewIntField(dynsFieldCheckpointLimit).
			Description("The maximum gap between the in flight sequence versus the latest acknowledged sequence at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual shards. Any given sequence will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewDurationField(dynsFieldCommitPeriod).
			Description("The period of time between each update to the checkpoint table.").
			Default("5s"),
		service.NewDurationField(dynsFieldRebalancePeriod).
			Description("The period of time between each attempt to rebalance shards across clients.").
			Default("30s").
			Advanced(),
		service.NewDurationField(dynsFieldLeasePeriod).
			Description("The period of time after which a client that has failed to update a shard checkpoint is assumed to be inactive.").
			Default("30s").
			Advanced(),
		service.NewBoolField(dynsFieldStartFromOldest).
			Description("Whether to consume from the oldest message when a sequence does not yet exist for the stream.").
			Default(true),
	).
		Fields(config.SessionFields()...).
		Field(service.NewBatchPolicyField(dynsFieldBatching))
	return spec
}

func init() {
	err := service.RegisterBatchInput("aws_dynamodb_streams", dynamoDBStreamsInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newDynamoDBStreamsReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(r), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

var awsDynamoDbDefaultLimit = int64(1000)

type dynamoDBStreamsReader struct {
	conf     dynsConfig
	clientID string

	sess    *session.Session
	batcher service.BatchPolicy
	log     *service.Logger
	mgr     *service.Resources

	backoffCtor func() backoff.BackOff
	boffPool    sync.Pool

	svc          dynsiface.DynamoDBStreamsAPI
	checkpointer *awsStreamCheckpointer

	streamShards    map[string][]string
	balancedStreams []string

	commitPeriod    time.Duration
	leasePeriod     time.Duration
	rebalancePeriod time.Duration

	cMut    sync.Mutex
	msgChan chan asyncMessage

	ctx  context.Context
	done func()

	closeOnce  sync.Once
	closedChan chan struct{}
}

func newDynamoDBStreamsReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBStreamsReader, error) {
	conf, err := dynamoDBStreamsInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	sess, err := GetSession(pConf)
	if err != nil {
		return nil, err
	}
	batcher, err := pConf.FieldBatchPolicy(dynsFieldBatching)
	if err != nil {
		return nil, err
	}
	return newDynamoDBStreamsReaderFromConfig(conf, batcher, sess, mgr)
}

func newDynamoDBStreamsReaderFromConfig(conf dynsConfig, batcher service.BatchPolicy, sess *session.Session, mgr *service.Resources) (*dynamoDBStreamsReader, error) {
	if batcher.IsNoop() {
		batcher.Count = 1
	}

	k := dynamoDBStreamsReader{
		conf:         conf,
		sess:         sess,
		batcher:      batcher,
		log:          mgr.Logger(),
		mgr:          mgr,
		closedChan:   make(chan struct{}),
		streamShards: map[string][]string{},
	}
	k.ctx, k.done = context.WithCancel(context.Background())

	u4, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	k.clientID = u4.String()

	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "300ms"
	rConf.Backoff.MaxInterval = "5s"
	if k.backoffCtor, err = rConf.GetCtor(); err != nil {
		return nil, err
	}
	k.boffPool = sync.Pool{
		New: func() any {
			return k.backoffCtor()
		},
	}

	for _, t := range conf.Streams {
		for _, splitStreams := range strings.Split(t, ",") {
			if trimmed := strings.TrimSpace(splitStreams); len(trimmed) > 0 {
				k.balancedStreams = append(k.balancedStreams, trimmed)
			}
		}
	}
	if k.commitPeriod, err = time.ParseDuration(k.conf.CommitPeriod); err != nil {
		return nil, fmt.Errorf("failed to parse commit period string: %v", err)
	}
	if k.leasePeriod, err = time.ParseDuration(k.conf.LeasePeriod); err != nil {
		return nil, fmt.Errorf("failed to parse lease period string: %v", err)
	}
	if k.rebalancePeriod, err = time.ParseDuration(k.conf.RebalancePeriod); err != nil {
		return nil, fmt.Errorf("failed to parse rebalance period string: %v", err)
	}
	return &k, nil
}

//------------------------------------------------------------------------------

func (k *dynamoDBStreamsReader) getIter(streamID, shardID, sequence string) (string, error) {
	iterType := dynamodbstreams.ShardIteratorTypeTrimHorizon
	if !k.conf.StartFromOldest {
		iterType = dynamodbstreams.ShardIteratorTypeLatest
	}
	var startingSequence *string
	if len(sequence) > 0 {
		iterType = dynamodbstreams.ShardIteratorTypeAfterSequenceNumber
		startingSequence = &sequence
	}

	res, err := k.svc.GetShardIteratorWithContext(k.ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         &streamID,
		ShardId:           &shardID,
		SequenceNumber:    startingSequence,
		ShardIteratorType: &iterType,
	})
	if err != nil {
		return "", err
	}

	var iter string
	if res.ShardIterator != nil {
		iter = *res.ShardIterator
	}
	if iter == "" {
		// If we failed to obtain from a sequence we start from beginning
		iterType = dynamodbstreams.ShardIteratorTypeTrimHorizon

		res, err := k.svc.GetShardIteratorWithContext(k.ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         &streamID,
			ShardId:           &shardID,
			ShardIteratorType: &iterType,
		})
		if err != nil {
			return "", err
		}

		if res.ShardIterator != nil {
			iter = *res.ShardIterator
		}
	}
	if iter == "" {
		return "", errors.New("failed to obtain shard iterator")
	}
	return iter, nil
}

// IMPORTANT TO NOTE: The returned shard iterator (second return parameter) will
// always be the input iterator when the error parameter is nil, therefore
// replacing the current iterator with this return param should always be safe.
//
// Do NOT modify this method without preserving this behaviour.
func (k *dynamoDBStreamsReader) getRecords(streamID, shardID, shardIter string) ([]*dynamodbstreams.Record, string, error) {
	res, err := k.svc.GetRecordsWithContext(k.ctx, &dynamodbstreams.GetRecordsInput{
		Limit:         &awsDynamoDbDefaultLimit,
		ShardIterator: &shardIter,
	})
	if err != nil {
		return nil, shardIter, err
	}

	nextIter := ""
	if res.NextShardIterator != nil {
		nextIter = *res.NextShardIterator
	}
	return res.Records, nextIter, nil
}

type awsDynamoDbConsumerState int

const (
	awsDynamoDbConsumerConsuming awsDynamoDbConsumerState = iota
	awsDynamoDbConsumerYielding
	awsDynamoDbConsumerFinished
	awsDynamoDbConsumerClosing
)

func (k *dynamoDBStreamsReader) runConsumer(wg *sync.WaitGroup, streamID, shardID, startingSequence string) (initErr error) {
	defer func() {
		if initErr != nil {
			wg.Done()
			if _, err := k.checkpointer.Checkpoint(context.Background(), streamID, shardID, startingSequence, true); err != nil {
				k.log.Errorf("Failed to gracefully yield checkpoint: %v\n", err)
			}
		}
	}()

	// Stores records, batches them up, and provides the batches for dispatch,
	// whilst ensuring only N records are in flight at a given time.
	var recordBatcher *awsDynamoDBRecordBatcher
	if recordBatcher, initErr = k.newDynamoDBRecordBatcher(streamID, shardID, startingSequence); initErr != nil {
		return initErr
	}

	// Keeps track of retry attempts.
	boff := k.boffPool.Get().(backoff.BackOff)

	// Stores consumed records that have yet to be added to the batcher.
	var pending []*dynamodbstreams.Record
	var iter string
	if iter, initErr = k.getIter(streamID, shardID, startingSequence); initErr != nil {
		return initErr
	}

	// Keeps track of the latest state of the consumer.
	state := awsDynamoDbConsumerConsuming
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
			recordBatcher.Close(context.Background(), state == awsDynamoDbConsumerFinished)
			boff.Reset()
			k.boffPool.Put(boff)

			reason := ""
			switch state {
			case awsDynamoDbConsumerFinished:
				reason = " because the shard is closed"
				if err := k.checkpointer.Delete(k.ctx, streamID, shardID); err != nil {
					k.log.Errorf("Failed to remove checkpoint for finished stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			case awsDynamoDbConsumerYielding:
				reason = " because the shard has been claimed by another client"
				if err := k.checkpointer.Yield(k.ctx, streamID, shardID, recordBatcher.GetSequence()); err != nil {
					k.log.Errorf("Failed to yield checkpoint for stolen stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			case awsDynamoDbConsumerClosing:
				reason = " because the pipeline is shutting down"
				if _, err := k.checkpointer.Checkpoint(context.Background(), streamID, shardID, recordBatcher.GetSequence(), true); err != nil {
					k.log.Errorf("Failed to store final checkpoint for stream '%v' shard '%v': %v\n", streamID, shardID, err)
				}
			}

			wg.Done()
			k.log.Debugf("Closing stream '%v' shard '%v' as client '%v'%v\n", streamID, shardID, k.checkpointer.clientID, reason)
		}()

		k.log.Debugf("Consuming stream '%v' shard '%v' as client '%v'\n", streamID, shardID, k.checkpointer.clientID)

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
			if state == awsDynamoDbConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan {
				if pending, iter, err = k.getRecords(streamID, shardID, iter); err != nil {
					if !awsErrIsTimeout(err) {
						nextPullChan = time.After(boff.NextBackOff())

						if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodbstreams.ErrCodeExpiredIteratorException {
							k.log.Warn("Shard iterator expired, attempting to refresh")
							newIter, err := k.getIter(streamID, shardID, recordBatcher.GetSequence())
							if err != nil {
								k.log.Errorf("Failed to refresh shard iterator: %v", err)
							} else {
								iter = newIter
							}
						} else {
							k.log.Errorf("Failed to pull DynamoDB Streams records: %v\n", err)
						}
					}
				} else if len(pending) == 0 {
					nextPullChan = time.After(boff.NextBackOff())
				} else {
					boff.Reset()
					nextPullChan = blockedChan
				}
				// The getRecords method ensures that it returns the input
				// iterator whenever it errors out. Therefore, regardless of the
				// outcome of the call if iter is now empty we have definitely
				// reached the end of the shard.
				if iter == "" {
					state = awsDynamoDbConsumerFinished
				}
			} else {
				unblockPullChan()
			}

			if pendingMsg.msg == nil {
				// If our consumer is finished and we've run out of pending
				// records then we're done.
				if len(pending) == 0 && state == awsDynamoDbConsumerFinished {
					if pendingMsg, _ = recordBatcher.FlushMessage(k.ctx); pendingMsg.msg == nil {
						return
					}
				} else if recordBatcher.HasPendingMessage() {
					if pendingMsg, err = recordBatcher.FlushMessage(commitCtx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				} else if len(pending) > 0 {
					var i int
					var r *dynamodbstreams.Record
					for i, r = range pending {
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
				if tNext, exists := recordBatcher.UntilNext(); exists {
					nextTimedBatchChan = time.After(tNext)
				}
			}

			select {
			case <-commitCtx.Done():
				if k.ctx.Err() != nil {
					// It could've been our parent context that closed, in which
					// case we exit.
					state = awsDynamoDbConsumerClosing
					return
				}

				commitCtxClose()
				commitCtx, commitCtxClose = context.WithTimeout(k.ctx, k.commitPeriod)

				stillOwned, err := k.checkpointer.Checkpoint(k.ctx, streamID, shardID, recordBatcher.GetSequence(), false)
				if err != nil {
					k.log.Errorf("Failed to store checkpoint for DynamoDB Stream '%v' shard '%v': %v\n", streamID, shardID, err)
				} else if !stillOwned {
					state = awsDynamoDbConsumerYielding
					return
				}
			case <-nextTimedBatchChan:
				nextTimedBatchChan = nil
			case nextFlushChan <- pendingMsg:
				pendingMsg = asyncMessage{}
			case <-nextPullChan:
				nextPullChan = unblockedChan
			case <-k.ctx.Done():
				state = awsDynamoDbConsumerClosing
				return
			}
		}
	}()
	return nil
}

//------------------------------------------------------------------------------

func isDynShardFinished(s *dynamodbstreams.Shard) bool {
	if s.SequenceNumberRange == nil {
		return false
	}
	if s.SequenceNumberRange.EndingSequenceNumber == nil {
		return false
	}
	return *s.SequenceNumberRange.EndingSequenceNumber != "null"
}

func (k *dynamoDBStreamsReader) runBalancedShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		k.closeOnce.Do(func() {
			close(k.msgChan)
			close(k.closedChan)
		})
	}()

	for {
		for _, streamID := range k.balancedStreams {
			stream, err := k.svc.DescribeStreamWithContext(k.ctx, &dynamodbstreams.DescribeStreamInput{
				StreamArn: aws.String(streamID),
			})
			shardsRes := stream.StreamDescription

			var clientClaims map[string][]awsStreamClientClaim
			if err == nil {
				clientClaims, err = k.checkpointer.AllClaims(k.ctx, streamID)
			}
			if err != nil {
				if k.ctx.Err() != nil {
					return
				}
				k.log.Errorf("Failed to obtain stream '%v' shards or claims: %v\n", streamID, err)
				continue
			}

			totalShards := len(shardsRes.Shards)
			unclaimedShards := make(map[string]string, totalShards)
			for _, s := range shardsRes.Shards {
				if !isDynShardFinished(s) {
					unclaimedShards[*s.ShardId] = ""
				}
			}
			for clientID, claims := range clientClaims {
				for _, claim := range claims {
					if time.Since(claim.LeaseTimeout) > k.leasePeriod*2 {
						unclaimedShards[claim.ShardID] = clientID
					} else {
						delete(unclaimedShards, claim.ShardID)
					}
				}
			}

			// Have a go at grabbing any unclaimed shards
			if len(unclaimedShards) > 0 {
				for shardID, clientID := range unclaimedShards {
					sequence, err := k.checkpointer.Claim(k.ctx, streamID, shardID, clientID)
					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to claim unclaimed shard '%v': %v\n", shardID, err)
						}
						continue
					}
					wg.Add(1)
					if err = k.runConsumer(&wg, streamID, shardID, sequence); err != nil {
						k.log.Errorf("Failed to start consumer: %v\n", err)
					}
				}

				// If there are unclaimed shards then let's not resort to
				// thievery just yet.
				continue
			}

			// There were no unclaimed shards, let's look for a shard to steal.
			selfClaims := len(clientClaims[k.clientID])
			for clientID, claims := range clientClaims {
				if clientID == k.clientID {
					// Don't steal from ourself, we're not at that point yet.
					continue
				}

				// This is an extremely naive "algorithm", we simply randomly
				// iterate all other clients with shards and if any have two
				// more shards than we do then it's fair game. Using two here
				// so that we don't play hot potatoes with an odd shard.
				if len(claims) > (selfClaims + 1) {
					randomShard := claims[(rand.Int() % len(claims))].ShardID
					k.log.Debugf(
						"Attempting to steal stream '%v' shard '%v' from client '%v' as client '%v'\n",
						streamID, randomShard, clientID, k.clientID,
					)

					sequence, err := k.checkpointer.Claim(k.ctx, streamID, randomShard, clientID)
					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to steal shard '%v': %v\n", randomShard, err)
						}
						k.log.Debugf(
							"Aborting theft of stream '%v' shard '%v' from client '%v' as client '%v'\n",
							streamID, randomShard, clientID, k.clientID,
						)
						continue
					}

					k.log.Debugf(
						"Successfully stole stream '%v' shard '%v' from client '%v' as client '%v'\n",
						streamID, randomShard, clientID, k.clientID,
					)
					wg.Add(1)
					if err = k.runConsumer(&wg, streamID, randomShard, sequence); err != nil {
						k.log.Errorf("Failed to start consumer: %v\n", err)
					} else {
						// If we successfully stole the shard then that's enough
						// for now.
						break
					}
				}
			}
		}

		select {
		case <-time.After(k.rebalancePeriod):
		case <-k.ctx.Done():
			return
		}
	}
}

func (k *dynamoDBStreamsReader) waitUntilStreamsExists(ctx context.Context) error {
	streams := append([]string{}, k.balancedStreams...)
	for k := range k.streamShards {
		streams = append(streams, k)
	}

	results := make(chan error, len(streams))
	for _, s := range streams {
		go func(stream string) {
			_, err := k.svc.DescribeStreamWithContext(ctx, &dynamodbstreams.DescribeStreamInput{
				StreamArn: &stream,
			})
			results <- err
		}(s)
	}

	for i := 0; i < len(streams); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// Connect establishes a dynamoDBStreamsReader connection.
func (k *dynamoDBStreamsReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	svc := dynamodbstreams.New(k.sess)
	checkpointer, err := newAWSStreamCheckpointer(k.sess, k.clientID, k.conf.DynamoDB, k.leasePeriod, k.commitPeriod)
	if err != nil {
		return err
	}

	k.svc = svc
	k.checkpointer = checkpointer
	k.msgChan = make(chan asyncMessage)

	if err = k.waitUntilStreamsExists(ctx); err != nil {
		return err
	}

	go k.runBalancedShards()

	return nil
}

// ReadBatch attempts to read a message from DynamoDB Streams.
func (k *dynamoDBStreamsReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, component.ErrTimeout
}

// CloseAsync shuts down the DynamoDB Streams input and stops processing requests.
func (k *dynamoDBStreamsReader) Close(ctx context.Context) error {
	k.done()
	select {
	case <-k.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
