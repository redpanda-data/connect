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

package aws

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid/v5"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	// Kinesis Input DynDB Fields
	kiddbFieldTable              = "table"
	kiddbFieldCreate             = "create"
	kiddbFieldReadCapacityUnits  = "read_capacity_units"
	kiddbFieldWriteCapacityUnits = "write_capacity_units"
	kiddbFieldBillingMode        = "billing_mode"

	// Kinesis Input Fields
	kiFieldDynamoDB        = "dynamodb"
	kiFieldStreams         = "streams"
	kiFieldCheckpointLimit = "checkpoint_limit"
	kiFieldCommitPeriod    = "commit_period"
	kiFieldLeasePeriod     = "lease_period"
	kiFieldRebalancePeriod = "rebalance_period"
	kiFieldRebalanceOps    = "rebalance_ops"
	kiFieldStartFromOldest = "start_from_oldest"
	kiFieldBatching        = "batching"
)

// These control shard balancing behavior
const (
	// prevents trying to claim the same shard too frequently
	minReclaimInterval = 10 * time.Second

	// ensures that each balancing cycle takes at least this long
	minIterationTime = 1 * time.Second

	// metrics
	metricShardsPerClient = "kinesis_client_shards"
	metricShardsStolen    = "kinesis_shards_stolen_total"
)

type kiConfig struct {
	Streams         []string
	DynamoDB        kiddbConfig
	CheckpointLimit int
	CommitPeriod    string
	LeasePeriod     string
	RebalancePeriod string
	RebalanceOps    int
	StartFromOldest bool
}

func kinesisInputConfigFromParsed(pConf *service.ParsedConfig) (conf kiConfig, err error) {
	if conf.Streams, err = pConf.FieldStringList(kiFieldStreams); err != nil {
		return
	}
	if pConf.Contains(kiFieldDynamoDB) {
		if conf.DynamoDB, err = kinesisInputDynamoDBConfigFromParsed(pConf.Namespace(kiFieldDynamoDB)); err != nil {
			return
		}
	}
	if conf.CheckpointLimit, err = pConf.FieldInt(kiFieldCheckpointLimit); err != nil {
		return
	}
	if conf.CommitPeriod, err = pConf.FieldString(kiFieldCommitPeriod); err != nil {
		return
	}
	if conf.LeasePeriod, err = pConf.FieldString(kiFieldLeasePeriod); err != nil {
		return
	}
	if conf.RebalancePeriod, err = pConf.FieldString(kiFieldRebalancePeriod); err != nil {
		return
	}
	if conf.RebalanceOps, err = pConf.FieldInt(kiFieldRebalanceOps); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(kiFieldStartFromOldest); err != nil {
		return
	}
	return
}

func kinesisInputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary("Receive messages from one or more Kinesis streams.").
		Description(`
Consumes messages from one or more Kinesis streams either by automatically balancing shards across other instances of this input, or by consuming shards listed explicitly. The latest message sequence consumed by this input is stored within a <<table-schema,DynamoDB table>>, which allows it to resume at the correct sequence of the shard during restarts. This table is also used for coordination across distributed inputs when shard balancing.

Redpanda Connect will not store a consumed sequence unless it is acknowledged at the output level, which ensures at-least-once delivery guarantees.

== Ordering

By default messages of a shard can be processed in parallel, up to a limit determined by the field `+"`checkpoint_limit`"+`. However, if strict ordered processing is required then this value must be set to 1 in order to process shard messages in lock-step. When doing so it is recommended that you perform batching at this component for performance as it will not be possible to batch lock-stepped messages at the output level.

== Table schema

It's possible to configure Redpanda Connect to create the DynamoDB table required for coordination if it does not already exist. However, if you wish to create this yourself (recommended) then create a table with a string HASH key `+"`StreamID`"+` and a string RANGE key `+"`ShardID`"+`.

== Batching

Use the `+"`batching`"+` fields to configure an optional xref:configuration:batching.adoc#batch-policy[batching policy]. Each stream shard will be batched separately in order to ensure that acknowledgements aren't contaminated.
`).Fields(
		service.NewStringListField(kiFieldStreams).
			Description("One or more Kinesis data streams to consume from. Streams can either be specified by their name or full ARN. Shards of a stream are automatically balanced across consumers by coordinating through the provided DynamoDB table. Multiple comma separated streams can be listed in a single element. Shards are automatically distributed across consumers of a stream by coordinating through the provided DynamoDB table. Alternatively, it's possible to specify an explicit shard to consume from with a colon after the stream name, e.g. `foo:0` would consume the shard `0` of the stream `foo`.").
			Examples([]any{"foo", "arn:aws:kinesis:*:111122223333:stream/my-stream"}),
		service.NewObjectField(kiFieldDynamoDB,
			append([]*service.ConfigField{service.NewStringField(kiddbFieldTable).
				Description("The name of the table to access.").
				Default(""),
				service.NewBoolField(kiddbFieldCreate).
					Description("Whether, if the table does not exist, it should be created.").
					Default(false),
				service.NewStringEnumField(kiddbFieldBillingMode, "PROVISIONED", "PAY_PER_REQUEST").
					Description("When creating the table determines the billing mode.").
					Default("PAY_PER_REQUEST").
					Advanced(),
				service.NewIntField(kiddbFieldReadCapacityUnits).
					Description("Set the provisioned read capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
					Default(0).
					Advanced(),
				service.NewIntField(kiddbFieldWriteCapacityUnits).
					Description("Set the provisioned write capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
					Default(0).
					Advanced()},
				config.SessionFields()...,
			)...,
		).
			Description("Determines the table used for storing and accessing the latest consumed sequence for shards, and for coordinating balanced consumers of streams."),
		service.NewIntField(kiFieldCheckpointLimit).
			Description("The maximum gap between the in flight sequence versus the latest acknowledged sequence at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual shards. Any given sequence will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			Default(1024),
		service.NewAutoRetryNacksToggleField(),
		service.NewDurationField(kiFieldCommitPeriod).
			Description("The period of time between each update to the checkpoint table.").
			Default("5s"),
		service.NewDurationField(kiFieldRebalancePeriod).
			Description("The period of time between each attempt to rebalance shards across clients.").
			Default("30s").
			Advanced(),
		service.NewIntField(kiFieldRebalanceOps).
			Description("The number of shard claim operations can happen in one rebalance cycle").
			Default(10).
			Advanced(),
		service.NewDurationField(kiFieldLeasePeriod).
			Description("The period of time after which a client that has failed to update a shard checkpoint is assumed to be inactive.").
			Default("30s").
			Advanced(),
		service.NewBoolField(kiFieldStartFromOldest).
			Description("Whether to consume from the oldest message when a sequence does not yet exist for the stream.").
			Default(true),
	).
		Fields(config.SessionFields()...).
		Field(service.NewBatchPolicyField(kiFieldBatching))
	return spec
}

func init() {
	err := service.RegisterBatchInput("aws_kinesis", kinesisInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newKinesisReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, r)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

var awsKinesisDefaultLimit = int32(10e3)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type streamInfo struct {
	explicitShards []string
	id             string // Either a name or arn, extracted from config and used for balancing shards
	arn            string
}

type kinesisReader struct {
	conf     kiConfig
	clientID string

	sess    aws.Config
	ddbSess aws.Config
	batcher service.BatchPolicy
	log     *service.Logger
	mgr     *service.Resources

	boffPool sync.Pool

	svc          *kinesis.Client
	checkpointer *awsKinesisCheckpointer

	streams []*streamInfo

	commitPeriod    time.Duration
	leasePeriod     time.Duration
	rebalancePeriod time.Duration

	cMut    sync.Mutex
	msgChan chan asyncMessage

	ctx  context.Context
	done func()

	closeOnce  sync.Once
	closedChan chan struct{}

	clientShardsMetric *service.MetricGauge
	shardsStolenMetric *service.MetricCounter
}

var errCannotMixBalancedShards = errors.New("it is not currently possible to include balanced and explicit shard streams in the same kinesis input")

func newKinesisReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*kinesisReader, error) {
	conf, err := kinesisInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	sess, err := GetSession(context.TODO(), pConf)
	if err != nil {
		return nil, err
	}
	batcher, err := pConf.FieldBatchPolicy(kiFieldBatching)
	if err != nil {
		return nil, err
	}

	var ddbSess aws.Config
	ddbCredsConf := pConf.Namespace("dynamodb")
	if ddbCredsConf.Contains("region") || ddbCredsConf.Contains("endpoint") || ddbCredsConf.Contains("credentials") {
		if ddbSess, err = GetSession(context.TODO(), ddbCredsConf); err != nil {
			return nil, err
		}
	} else {
		// Reuse the Kinesis config if the DynamoDB config is empty
		ddbSess = sess
	}

	return newKinesisReaderFromConfig(conf, batcher, sess, ddbSess, mgr)
}

func parseStreamID(id string) (remaining, shard string, err error) {
	if streamStartsAt := strings.LastIndex(id, "/"); streamStartsAt > 0 {
		remaining = id[0:streamStartsAt]
		id = id[streamStartsAt:]
	}

	withShards := strings.Split(id, ":")
	if len(withShards) > 2 {
		err = fmt.Errorf("stream '%v' is invalid, only one shard should be specified and the same stream can be listed multiple times, e.g. use `foo:0,foo:1` not `foo:0:1`", id)
		return
	}
	remaining += strings.TrimSpace(withShards[0])
	if len(withShards) > 1 {
		shard = strings.TrimSpace(withShards[1])
	}
	return
}

func newKinesisReaderFromConfig(conf kiConfig, batcher service.BatchPolicy, sess, ddbSess aws.Config, mgr *service.Resources) (*kinesisReader, error) {
	if batcher.IsNoop() {
		batcher.Count = 1
	}

	k := kinesisReader{
		conf:       conf,
		sess:       sess,
		ddbSess:    ddbSess,
		batcher:    batcher,
		log:        mgr.Logger(),
		mgr:        mgr,
		closedChan: make(chan struct{}),
	}
	k.ctx, k.done = context.WithCancel(context.Background())

	u4, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	k.clientID = u4.String()

	k.boffPool = sync.Pool{
		New: func() any {
			boff := backoff.NewExponentialBackOff()
			boff.InitialInterval = 300 * time.Millisecond
			boff.MaxInterval = 5 * time.Second
			boff.MaxElapsedTime = 0
			return boff
		},
	}

	shardsByStream := map[string][]string{}
	for _, t := range conf.Streams {
		for _, splitStreams := range strings.Split(t, ",") {
			trimmed := strings.TrimSpace(splitStreams)
			if trimmed == "" {
				continue
			}

			var shardID string
			if trimmed, shardID, err = parseStreamID(trimmed); err != nil {
				return nil, err
			}

			if shardID != "" {
				if len(k.streams) > 0 {
					return nil, errCannotMixBalancedShards
				}
				shardsByStream[trimmed] = append(shardsByStream[trimmed], shardID)
			} else {
				if len(shardsByStream) > 0 {
					return nil, errCannotMixBalancedShards
				}
				k.streams = append(k.streams, &streamInfo{
					id: trimmed,
				})
			}

		}
	}

	for id, shards := range shardsByStream {
		k.streams = append(k.streams, &streamInfo{
			id:             id,
			explicitShards: shards,
		})
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

	// Initialize metrics
	k.clientShardsMetric = mgr.Metrics().NewGauge(metricShardsPerClient)
	k.shardsStolenMetric = mgr.Metrics().NewCounter(metricShardsStolen)

	return &k, nil
}

//------------------------------------------------------------------------------

const (
	// ErrCodeKMSThrottlingException is defined in the API Reference
	// https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

func (k *kinesisReader) getIter(info streamInfo, shardID, sequence string) (string, error) {
	iterType := types.ShardIteratorTypeTrimHorizon
	if !k.conf.StartFromOldest {
		iterType = types.ShardIteratorTypeLatest
	}
	var startingSequence *string
	if sequence != "" {
		iterType = types.ShardIteratorTypeAfterSequenceNumber
		startingSequence = &sequence
	}

	res, err := k.svc.GetShardIterator(k.ctx, &kinesis.GetShardIteratorInput{
		StreamARN:              &info.arn,
		ShardId:                &shardID,
		StartingSequenceNumber: startingSequence,
		ShardIteratorType:      iterType,
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
		iterType = types.ShardIteratorTypeTrimHorizon

		res, err := k.svc.GetShardIterator(k.ctx, &kinesis.GetShardIteratorInput{
			StreamARN:         &info.arn,
			ShardId:           &shardID,
			ShardIteratorType: iterType,
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
func (k *kinesisReader) getRecords(info streamInfo, shardID, shardIter string) ([]types.Record, string, error) {
	res, err := k.svc.GetRecords(k.ctx, &kinesis.GetRecordsInput{
		StreamARN:     &info.arn,
		Limit:         &awsKinesisDefaultLimit,
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

func awsErrIsTimeout(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		(err != nil && strings.HasSuffix(err.Error(), "context canceled"))
}

type awsKinesisConsumerState int

const (
	awsKinesisConsumerConsuming awsKinesisConsumerState = iota
	awsKinesisConsumerYielding
	awsKinesisConsumerFinished
	awsKinesisConsumerClosing
)

func (k *kinesisReader) runConsumer(wg *sync.WaitGroup, info streamInfo, shardID, startingSequence string) (initErr error) {
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
	var iter string
	if iter, initErr = k.getIter(info, shardID, startingSequence); initErr != nil {
		return initErr
	}

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
			if state == awsKinesisConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan {
				if pending, iter, err = k.getRecords(info, shardID, iter); err != nil {
					if !awsErrIsTimeout(err) {
						nextPullChan = time.After(boff.NextBackOff())

						var aerr *types.ExpiredIteratorException
						if errors.As(err, &aerr) {
							k.log.Warn("Shard iterator expired, attempting to refresh")
							newIter, err := k.getIter(info, shardID, recordBatcher.GetSequence())
							if err != nil {
								k.log.Errorf("Failed to refresh shard iterator: %v", err)
							} else {
								iter = newIter
							}
						} else {
							k.log.Errorf("Failed to pull Kinesis records: %v\n", err)
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
					state = awsKinesisConsumerFinished
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

//------------------------------------------------------------------------------

func isShardFinished(s types.Shard) bool {
	if s.SequenceNumberRange == nil {
		return false
	}
	if s.SequenceNumberRange.EndingSequenceNumber == nil {
		return false
	}
	return *s.SequenceNumberRange.EndingSequenceNumber != "null"
}

func (k *kinesisReader) runBalancedShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		k.closeOnce.Do(func() {
			close(k.msgChan)
			close(k.closedChan)
		})
	}()

	// Add jitter to prevent thundering herd problem
	jitter := time.Duration(rand.Intn(500)) * time.Millisecond
	select {
	case <-time.After(jitter):
	case <-k.ctx.Done():
		return
	}

	for {
		k.log.Debugf("Rebalancing shards for client '%v'", k.clientID)
		loopStart := time.Now()
		claimOps := 0

		for _, info := range k.streams {
			if claimOps >= k.conf.RebalanceOps {
				k.log.Debugf("Reached claim operation limit (%d) for this iteration, continuing in next cycle", k.conf.RebalanceOps)
				break
			}

			shardsRes, err := k.svc.ListShards(k.ctx, &kinesis.ListShardsInput{
				StreamARN: &info.arn,
			})
			if err != nil {
				if k.ctx.Err() != nil {
					return
				}
				k.log.Errorf("Failed to list shards: %v", err)
				continue
			}

			clientClaims, err := k.checkpointer.AllClaims(k.ctx, info.id)
			if err != nil {
				if k.ctx.Err() != nil {
					return
				}
				k.log.Errorf("Failed to obtain claims for stream '%v': %v", info.id, err)
				continue
			}

			if claims, exists := clientClaims[k.clientID]; exists {
				k.clientShardsMetric.Set(int64(len(claims)))
			} else {
				k.clientShardsMetric.Set(0)
			}

			totalShards := len(shardsRes.Shards)
			prioritizedShards := make(map[string]string, totalShards)

			// First process unclaimed or expired shards
			for _, s := range shardsRes.Shards {
				if s.ShardId != nil && !isShardFinished(s) {
					prioritizedShards[*s.ShardId] = ""
				}
			}

			for clientID, claims := range clientClaims {
				for _, claim := range claims {
					if time.Since(claim.LeaseTimeout) > k.leasePeriod*2 {
						// Expired lease
						prioritizedShards[claim.ShardID] = clientID
					} else {
						// Valid lease, don't try to claim
						delete(prioritizedShards, claim.ShardID)
					}
				}
			}

			// Calculate fair distribution target
			selfClaimCount := len(clientClaims[k.clientID])
			targetShardCount := 0

			if totalClients := len(clientClaims); totalClients > 0 {
				targetShardCount = totalShards / totalClients
				k.log.Debugf("Current claims: %d, target shard count: %d", selfClaimCount, targetShardCount)
			}

			// Process unclaimed/expired shards first
			if len(prioritizedShards) > 0 {
				shardIDs := make([]string, 0, len(prioritizedShards))
				for shardID := range prioritizedShards {
					shardIDs = append(shardIDs, shardID)
				}
				sort.Strings(shardIDs)

				for _, shardID := range shardIDs {
					if claimOps >= k.conf.RebalanceOps {
						break
					}

					clientID := prioritizedShards[shardID]
					sequence, err := k.checkpointer.Claim(k.ctx, info.id, shardID, clientID)

					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to claim shard '%v': %v", shardID, err)
						}
						continue
					} else {
						k.log.Debugf("Successfully claimed shard '%v'", shardID)
						selfClaimCount++
						claimOps++
					}

					wg.Add(1)
					if err = k.runConsumer(&wg, *info, shardID, sequence); err != nil {
						k.log.Errorf("Failed to start consumer: %v", err)
					}
				}
			}

			// If we have fewer shards than our target, attempt to steal some
			if selfClaimCount < targetShardCount {
				for clientID, claims := range clientClaims {
					if clientID == k.clientID {
						continue
					}
					if claimOps >= k.conf.RebalanceOps {
						break
					}

					if len(claims) > (selfClaimCount + 1) {
						randomShard := claims[(rand.Int() % len(claims))].ShardID
						k.log.Debugf(
							"Attempting to steal stream '%v' shard '%v' from client '%v' as client '%v'",
							info.id, randomShard, clientID, k.clientID,
						)

						sequence, err := k.checkpointer.Claim(k.ctx, info.id, randomShard, clientID)
						if err != nil {
							if k.ctx.Err() != nil {
								return
							}
							if !errors.Is(err, ErrLeaseNotAcquired) {
								k.log.Errorf("Failed to steal shard '%v': %v", randomShard, err)
							}
							k.log.Debugf(
								"Aborting theft of stream '%v' shard '%v' from client '%v' as client '%v'",
								info.id, randomShard, clientID, k.clientID,
							)
							continue
						} else {
							k.log.Debugf(
								"Successfully stole stream '%v' shard '%v' from client '%v' as client '%v'",
								info.id, randomShard, clientID, k.clientID,
							)
							k.shardsStolenMetric.Incr(1)
							selfClaimCount++
							claimOps++
						}

						wg.Add(1)
						if err = k.runConsumer(&wg, *info, randomShard, sequence); err != nil {
							k.log.Errorf("Failed to start consumer: %v\n", err)
						}
					}
				}
			}
		}

		// Ensure minimum iteration time to prevent CPU spikes
		iterationDuration := time.Since(loopStart)
		if iterationDuration < minIterationTime {
			sleepTime := minIterationTime - iterationDuration
			select {
			case <-time.After(sleepTime):
			case <-k.ctx.Done():
				return
			}
		}

		select {
		case <-time.After(k.rebalancePeriod):
		case <-k.ctx.Done():
			return
		}
	}
}

func (k *kinesisReader) runExplicitShards() {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		k.closeOnce.Do(func() {
			close(k.msgChan)
			close(k.closedChan)
		})
	}()

	pendingShards := map[string]streamInfo{}
	for _, v := range k.streams {
		pendingShards[v.id] = *v
	}

	for {
		for id, info := range pendingShards {
			var failedShards []string
			for _, shardID := range info.explicitShards {
				sequence, err := k.checkpointer.Claim(k.ctx, id, shardID, "")
				if err == nil {
					wg.Add(1)
					err = k.runConsumer(&wg, info, shardID, sequence)
				}
				if err != nil {
					if k.ctx.Err() != nil {
						return
					}
					failedShards = append(failedShards, shardID)
					k.log.Errorf("Failed to start stream '%v' shard '%v' consumer: %v", id, shardID, err)
				}
			}
			if len(failedShards) > 0 {
				tmp := pendingShards[id]
				tmp.explicitShards = failedShards
				pendingShards[id] = tmp
			} else {
				delete(pendingShards, id)
			}
		}
		if len(pendingShards) == 0 {
			break
		}

		<-time.After(time.Second)
	}
}

func (k *kinesisReader) waitUntilStreamsExists(ctx context.Context) error {
	results := make(chan error, len(k.streams))
	for _, s := range k.streams {
		s := s
		go func(info *streamInfo) {
			waiter := kinesis.NewStreamExistsWaiter(k.svc)
			input := &kinesis.DescribeStreamInput{}
			if strings.HasPrefix(info.id, "arn:") {
				input.StreamARN = &info.id
			} else {
				input.StreamName = &info.id
			}
			out, err := waiter.WaitForOutput(ctx, input, time.Minute)
			if err == nil {
				info.arn = *out.StreamDescription.StreamARN
			}
			results <- err
		}(s)
	}

	for i := 0; i < len(k.streams); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// Connect establishes a kinesisReader connection.
func (k *kinesisReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	svc := kinesis.NewFromConfig(k.sess)
	checkpointer, err := newAWSKinesisCheckpointer(k.ddbSess, k.clientID, k.conf.DynamoDB, k.leasePeriod, k.commitPeriod)
	if err != nil {
		return err
	}

	k.svc = svc
	k.checkpointer = checkpointer
	k.msgChan = make(chan asyncMessage)

	if err = k.waitUntilStreamsExists(ctx); err != nil {
		return err
	}

	if len(k.streams[0].explicitShards) > 0 {
		go k.runExplicitShards()
	} else {
		go k.runBalancedShards()
	}

	return nil
}

// ReadBatch attempts to read a message from Kinesis.
func (k *kinesisReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
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
		return nil, nil, ctx.Err()
	}
}

// CloseAsync shuts down the Kinesis input and stops processing requests.
func (k *kinesisReader) Close(ctx context.Context) error {
	k.done()
	select {
	case <-k.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
