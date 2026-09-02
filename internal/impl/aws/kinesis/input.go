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

package kinesis

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
	baws "github.com/redpanda-data/connect/v4/internal/impl/aws"
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
	kiFieldDynamoDB         = "dynamodb"
	kiFieldStreams          = "streams"
	kiFieldCheckpointLimit  = "checkpoint_limit"
	kiFieldCommitPeriod     = "commit_period"
	kiFieldStealGracePeriod = "steal_grace_period"
	kiFieldLeasePeriod      = "lease_period"
	kiFieldRebalancePeriod  = "rebalance_period"
	kiFieldStartFromOldest  = "start_from_oldest"
	kiFieldPollPeriod       = "poll_period"
	kiFieldEnhancedFanOut   = "enhanced_fan_out"
	kiFieldBatching         = "batching"

	// Kinesis Enhanced Fan-Out Fields
	kiefoFieldEnabled                = "enabled"
	kiefoFieldConsumerName           = "consumer_name"
	kiefoFieldActivationTimeout      = "consumer_activation_timeout"
	kiefoFieldMaxResubscribeInterval = "max_resubscribe_interval"

	// Kinesis metrics
	metricShardsPerClient = "kinesis_client_shards"
	metricShardsStolen    = "kinesis_shards_stolen_total"
)

type kiConfig struct {
	Streams                   []string
	DynamoDB                  kiddbConfig
	CheckpointLimit           int
	CommitPeriod              string
	StealGracePeriod          string
	LeasePeriod               string
	RebalancePeriod           string
	StartFromOldest           bool
	PollPeriod                time.Duration
	EFOEnabled                bool
	EFOConsumerName           string
	EFOActivationTimeout      time.Duration
	EFOMaxResubscribeInterval time.Duration
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
	if conf.StealGracePeriod, err = pConf.FieldString(kiFieldStealGracePeriod); err != nil {
		return
	}
	if conf.LeasePeriod, err = pConf.FieldString(kiFieldLeasePeriod); err != nil {
		return
	}
	if conf.RebalancePeriod, err = pConf.FieldString(kiFieldRebalancePeriod); err != nil {
		return
	}
	if conf.StartFromOldest, err = pConf.FieldBool(kiFieldStartFromOldest); err != nil {
		return
	}
	if conf.PollPeriod, err = pConf.FieldDuration(kiFieldPollPeriod); err != nil {
		return
	}
	{
		efoConf := pConf.Namespace(kiFieldEnhancedFanOut)
		if conf.EFOEnabled, err = efoConf.FieldBool(kiefoFieldEnabled); err != nil {
			return
		}
		if conf.EFOConsumerName, err = efoConf.FieldString(kiefoFieldConsumerName); err != nil {
			return
		}
		if conf.EFOEnabled && conf.EFOConsumerName == "" {
			err = fmt.Errorf("%v.%v is required when %v.%v is true", kiFieldEnhancedFanOut, kiefoFieldConsumerName, kiFieldEnhancedFanOut, kiefoFieldEnabled)
			return
		}
		if conf.EFOActivationTimeout, err = efoConf.FieldDuration(kiefoFieldActivationTimeout); err != nil {
			return
		}
		if conf.EFOEnabled && conf.EFOActivationTimeout <= 0 {
			err = fmt.Errorf("%v.%v must be greater than zero", kiFieldEnhancedFanOut, kiefoFieldActivationTimeout)
			return
		}
		if conf.EFOMaxResubscribeInterval, err = efoConf.FieldDuration(kiefoFieldMaxResubscribeInterval); err != nil {
			return
		}
		if conf.EFOEnabled && conf.EFOMaxResubscribeInterval < time.Second {
			err = fmt.Errorf("%v.%v must be at least 1s", kiFieldEnhancedFanOut, kiefoFieldMaxResubscribeInterval)
			return
		}
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

== Enhanced fan-out

Kinesis enforces a shared limit of 5 GetRecords calls per second per shard across all polling consumers of a stream. When multiple applications consume the same stream this budget is quickly exhausted and consumers receive ReadProvisionedThroughputExceeded errors. There are two remedies:

- Set the `+"`poll_period`"+` field to bound how frequently this input polls each shard, leaving headroom for other consumers.
- Enable `+"`enhanced_fan_out`"+`, which registers this pipeline as a dedicated stream consumer with its own 2MB/s per shard read throughput, delivered over HTTP/2 push rather than polling. Enhanced fan-out requires the IAM permissions `+"`kinesis:DescribeStreamConsumer` and `kinesis:SubscribeToShard`"+`, and incurs additional AWS charges per consumer-shard-hour plus data retrieval. The named consumer is registered automatically on first use and is never deregistered; that registration additionally requires `+"`kinesis:RegisterStreamConsumer`"+`, which may be omitted when a consumer with the configured name already exists on every stream (for example one provisioned through infrastructure-as-code).

== Table schema

It's possible to configure Redpanda Connect to create the DynamoDB table required for coordination if it does not already exist. However, if you wish to create this yourself (recommended) then create a table with a string HASH key `+"`StreamID`"+` and a string RANGE key `+"`ShardID`"+`.

== Batching

Use the `+"`batching`"+` fields to configure an optional xref:configuration:batching.adoc#batch-policy[batching policy]. Each stream shard will be batched separately in order to ensure that acknowledgements aren't contaminated.
`).Fields(
		service.NewStringListField(kiFieldStreams).
			Description("One or more Kinesis data streams to consume from. Streams can either be specified by their name or full ARN. Shards of a stream are automatically balanced across consumers by coordinating through the provided DynamoDB table. Multiple comma separated streams can be listed in a single element. Shards are automatically distributed across consumers of a stream by coordinating through the provided DynamoDB table. Alternatively, it's possible to specify an explicit shard to consume from with a colon after the stream name, e.g. `foo:0` would consume the shard `0` of the stream `foo`.").
			ShortDescription("One or more Kinesis data streams to consume from, by name or full ARN. Shards are balanced across consumers automatically.").
			Examples([]any{"foo", "arn:aws:kinesis:*:111122223333:stream/my-stream"}),
		service.NewObjectField(kiFieldDynamoDB,
			append([]*service.ConfigField{
				service.NewStringField(kiddbFieldTable).
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
					ShortDescription("Provisioned read capacity when creating the table with a billing_mode of PROVISIONED.").
					Default(0).
					Advanced(),
				service.NewIntField(kiddbFieldWriteCapacityUnits).
					Description("Set the provisioned write capacity when creating the table with a `billing_mode` of `PROVISIONED`.").
					ShortDescription("Provisioned write capacity when creating the table with a billing_mode of PROVISIONED.").
					Default(0).
					Advanced(),
			},
				config.SessionFields()...,
			)...,
		).
			Description("Determines the table used for storing and accessing the latest consumed sequence for shards, and for coordinating balanced consumers of streams.").
			ShortDescription("The table used to store consumed sequence numbers per shard and coordinate balanced consumers."),
		service.NewIntField(kiFieldCheckpointLimit).
			Description("The maximum gap between the in flight sequence versus the latest acknowledged sequence at a given time. Increasing this limit enables parallel processing and batching at the output level to work on individual shards. Any given sequence will not be committed unless all messages under that offset are delivered in order to preserve at least once delivery guarantees.").
			ShortDescription("Maximum gap between the in-flight sequence and the latest acknowledged sequence.").
			Default(1024),
		service.NewDurationField(kiFieldPollPeriod).
			Description("An optional minimum period between GetRecords calls made against each shard. Kinesis allows a shared budget of 5 GetRecords calls per second per shard across all consumers of a stream, so setting this to e.g. `250ms` bounds this consumer to roughly four reads per second per shard, leaving headroom for other consumers of the same stream. The default of `0s` polls as fast as records are consumed. This setting has no effect when `enhanced_fan_out` is enabled. A shard is polled at most once per period, so the committed sequence advances no faster than that; values above `lease_period` are rejected.").
			ShortDescription("Minimum period between record polls of a shard, for staying under the shared Kinesis read limit.").
			Default("0s").
			Version("4.107.0").
			Advanced(),
		service.NewObjectField(kiFieldEnhancedFanOut,
			service.NewBoolField(kiefoFieldEnabled).
				Description("Whether to consume the stream using enhanced fan-out.").
				Default(false),
			service.NewStringField(kiefoFieldConsumerName).
				Description("The name of the enhanced fan-out consumer to register. Required when `enabled` is true. Each distinct pipeline (application) consuming a stream must use its own consumer name, as Kinesis permits only one active subscription per consumer per shard. Instances of the same pipeline sharing a DynamoDB checkpoint table should share this name.").
				ShortDescription("The name of the enhanced fan-out consumer to register, unique per distinct pipeline.").
				Default(""),
			service.NewDurationField(kiefoFieldActivationTimeout).
				Description("The maximum amount of time to wait on connect for the registered consumer to become active before failing. Newly registered consumers on streams with many shards can take tens of seconds to activate.").
				Default("1m").
				Advanced(),
			service.NewDurationField(kiefoFieldMaxResubscribeInterval).
				Description("The ceiling on the exponential backoff between SubscribeToShard attempts after a subscription ends without delivering any events. This bounds how long a shard may sit unsubscribed after repeated failures.").
				Default("30s").
				Advanced(),
		).
			Description("Consume the stream using https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html[enhanced fan-out^], which provides this consumer dedicated read throughput of 2MB/s per shard via HTTP/2 push delivery, avoiding the 5 reads per second per shard limit that polling consumers share. The named consumer is registered on each stream automatically if it does not already exist (and is never deregistered). Requires the IAM permissions `kinesis:DescribeStreamConsumer` and `kinesis:SubscribeToShard`, plus `kinesis:RegisterStreamConsumer` unless a consumer with the configured name already exists on every stream (for example one provisioned through infrastructure-as-code). Note that AWS bills enhanced fan-out consumers per consumer-shard-hour plus data retrieval.").
			ShortDescription("Consume the stream using enhanced fan-out for dedicated read throughput.").
			Version("4.107.0").
			Advanced().
			LintRule(`root = if this.enabled && this.consumer_name == "" { [ "consumer_name is required when enabled is true" ] }`),
		service.NewAutoRetryNacksToggleField(),
		service.NewDurationField(kiFieldCommitPeriod).
			Description("The period of time between each update to the checkpoint table.").
			Default("5s"),
		service.NewDurationField(kiFieldStealGracePeriod).
			Description("Determines how long beyond the next commit period a client will wait when stealing a shard for the current owner to store a checkpoint. A longer value increases the time taken to balance shards but reduces the likelihood of processing duplicate messages.").
			ShortDescription("How long past the next commit period to wait for a shard's current owner to store a checkpoint.").
			Default("2s"),
		service.NewDurationField(kiFieldRebalancePeriod).
			Description("The period of time between each attempt to rebalance shards across clients.").
			Default("30s").
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
	service.MustRegisterBatchInput("aws_kinesis", kinesisInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			r, err := newKinesisReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, r)
		})
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
	consumerARN    string // Enhanced fan-out consumer ARN, set when EFO is enabled
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

	commitPeriod     time.Duration
	stealGracePeriod time.Duration
	leasePeriod      time.Duration
	rebalancePeriod  time.Duration
	pollPeriod       time.Duration
	batchPeriod      time.Duration

	cMut    sync.Mutex
	msgChan chan asyncMessage

	ctx  context.Context //nolint:containedctx // lifecycle context for consumer goroutines
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
	sess, err := baws.GetSession(context.TODO(), pConf)
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
		if ddbSess, err = baws.GetSession(context.TODO(), ddbCredsConf); err != nil {
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
			boff.InitialInterval = time.Millisecond * 300
			boff.MaxInterval = time.Second * 5
			boff.MaxElapsedTime = 0
			return boff
		},
	}

	shardsByStream := map[string][]string{}
	for _, t := range conf.Streams {
		for splitStreams := range strings.SplitSeq(t, ",") {
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
		return nil, fmt.Errorf("parsing commit period string: %v", err)
	}
	if k.stealGracePeriod, err = time.ParseDuration(k.conf.StealGracePeriod); err != nil {
		return nil, fmt.Errorf("parsing steal grace period string: %v", err)
	}
	if k.leasePeriod, err = time.ParseDuration(k.conf.LeasePeriod); err != nil {
		return nil, fmt.Errorf("parsing lease period string: %v", err)
	}
	if k.rebalancePeriod, err = time.ParseDuration(k.conf.RebalancePeriod); err != nil {
		return nil, fmt.Errorf("parsing rebalance period string: %v", err)
	}
	// batcher.Period is an optional Go-duration string on service.BatchPolicy;
	// an empty string means no timed flush is configured. A value that fails
	// to parse is left at zero here: the batcher's own construction parses the
	// same string and surfaces the error, and this field only feeds the
	// fetch-wait bound below, so a conservative zero is harmless.
	if batcher.Period != "" {
		k.batchPeriod, _ = time.ParseDuration(batcher.Period)
	}

	k.pollPeriod = conf.PollPeriod
	if k.pollPeriod > k.leasePeriod {
		return nil, fmt.Errorf("%v (%v) must not exceed %v (%v)", kiFieldPollPeriod, k.pollPeriod, kiFieldLeasePeriod, k.leasePeriod)
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
	var source shardRecordSource
	if source, initErr = k.newShardRecordSource(info, shardID, startingSequence, recordBatcher.GetSequence); initErr != nil {
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
			source.Close()
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
			if state == awsKinesisConsumerConsuming && len(pending) == 0 && nextPullChan == unblockedChan && pendingMsg.msg == nil {
				var done bool
				if pending, done, err = source.Fetch(k.ctx); err != nil {
					if !awsErrIsTimeout(err) && !errors.Is(err, errPollGateWaiting) {
						nextPullChan = time.After(boff.NextBackOff())
						k.log.Errorf("Failed to pull Kinesis records: %v\n", err)
					}
				} else if len(pending) == 0 {
					// A blocking source waits for data internally, so an empty
					// result must be retried immediately rather than backed off.
					if !source.Blocking() {
						nextPullChan = time.After(boff.NextBackOff())
					}
				} else {
					boff.Reset()
					nextPullChan = blockedChan
				}
				if done {
					state = awsKinesisConsumerFinished
				}
			} else if pendingMsg.msg != nil {
				// Park pulls while a message awaits delivery so that a
				// blocking Fetch cannot delay its flush, and so the
				// always-ready unblocked channel cannot outcompete the
				// flush case in the select below.
				if nextPullChan == unblockedChan {
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

				// Only allow a timed batch flush if we do not have a pending
				// message.
				if nextTimedBatchChan == nil {
					if tNext, exists := recordBatcher.UntilNext(); exists {
						nextTimedBatchChan = time.After(tNext)
					}
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
				if pendingMsg.msg == nil {
					if pendingMsg, err = recordBatcher.FlushMessage(k.ctx); err != nil {
						k.log.Errorf("Failed to dispatch message due to checkpoint error: %v\n", err)
					}
				}
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

// maxShardFetchWait is the initial (commit/batch-period-independent) bound on
// how long a single Fetch may block, companion to the floor enforced by
// minBound below.
const maxShardFetchWait = time.Second

// shardFetchWaitBound bounds how long a shard record source may internally
// block per Fetch so that the consumer loop keeps servicing its commit timer
// and timed batch flushes.
func shardFetchWaitBound(commitPeriod, batchPeriod time.Duration) time.Duration {
	bound := maxShardFetchWait
	if half := commitPeriod / 2; half < bound {
		bound = half
	}
	if batchPeriod > 0 {
		if half := batchPeriod / 2; half < bound {
			bound = half
		}
	}
	const minBound = 5 * time.Millisecond
	if bound < minBound {
		bound = minBound
	}
	return bound
}

// newShardRecordSource creates the record source for a single claimed shard.
func (k *kinesisReader) newShardRecordSource(info streamInfo, shardID, startingSequence string, sequenceFn func() string) (shardRecordSource, error) {
	// Bound how long a single Fetch may wait internally, whether that's the
	// enhanced fan-out push wait or the poll_period gate, so the consumer loop
	// keeps servicing its commit timer and timed batch flushes well within
	// each period.
	fetchTimeout := shardFetchWaitBound(k.commitPeriod, k.batchPeriod)
	if k.conf.EFOEnabled {
		return newEFORecordSource(k.ctx, kinesisEFOSubscribeFn(k.svc, info.consumerARN, shardID), shardID, startingSequence, k.conf.StartFromOldest, fetchTimeout, k.conf.EFOMaxResubscribeInterval, k.log)
	}
	return newPollingRecordSource(k.ctx, k.svc, info.arn, shardID, startingSequence, k.conf.StartFromOldest, k.pollPeriod, fetchTimeout, sequenceFn, k.log)
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

	for {
		for _, info := range k.streams {
			shardsRes, err := k.svc.ListShards(k.ctx, &kinesis.ListShardsInput{
				StreamARN: &info.arn,
			})

			var clientClaims map[string][]awsKinesisClientClaim
			if err == nil {
				clientClaims, err = k.checkpointer.AllClaims(k.ctx, info.id)
			}
			if err != nil {
				if k.ctx.Err() != nil {
					return
				}
				k.log.Errorf("Failed to obtain stream '%v' shards or claims: %v", info.id, err)
				continue
			}

			if claims, exists := clientClaims[k.clientID]; exists {
				k.clientShardsMetric.Set(int64(len(claims)))
			} else {
				k.clientShardsMetric.Set(0)
			}

			totalShards := len(shardsRes.Shards)
			unclaimedShards := make(map[string]string, totalShards)
			for _, s := range shardsRes.Shards {
				if !isShardFinished(s) {
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
					sequence, err := k.checkpointer.Claim(k.ctx, info.id, shardID, clientID)
					if err != nil {
						if k.ctx.Err() != nil {
							return
						}
						if !errors.Is(err, ErrLeaseNotAcquired) {
							k.log.Errorf("Failed to claim unclaimed shard '%v': %v", shardID, err)
						}
						continue
					}
					wg.Add(1)
					if err = k.runConsumer(&wg, *info, shardID, sequence); err != nil {
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
					}

					k.log.Debugf(
						"Successfully stole stream '%v' shard '%v' from client '%v' as client '%v'",
						info.id, randomShard, clientID, k.clientID,
					)
					k.shardsStolenMetric.Incr(1)

					wg.Add(1)
					if err = k.runConsumer(&wg, *info, randomShard, sequence); err != nil {
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

	for range k.streams {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// ConnectionTest attempts to test the connection configuration of this input
// without actually consuming data. The connection, if successful, is then
// closed.
func (k *kinesisReader) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	svc := kinesis.NewFromConfig(k.sess)

	// Test connection to at least one stream
	if len(k.streams) == 0 {
		return service.ConnectionTestFailed(errors.New("no streams configured")).AsList()
	}

	// Test the first stream to verify connectivity
	streamInfo := k.streams[0]
	_, err := svc.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamInfo.id),
	})
	if err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("describing stream %s: %w", streamInfo.id, err)).AsList()
	}

	return service.ConnectionTestSucceeded().AsList()
}

// Connect establishes a kinesisReader connection.
func (k *kinesisReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	svc := kinesis.NewFromConfig(k.sess)
	checkpointer, err := newAWSKinesisCheckpointer(ctx, k.ddbSess, k.clientID, k.conf.DynamoDB, k.leasePeriod, k.commitPeriod, k.stealGracePeriod)
	if err != nil {
		return err
	}

	k.svc = svc
	k.checkpointer = checkpointer

	if err = k.waitUntilStreamsExists(ctx); err != nil {
		return err
	}

	if k.conf.EFOEnabled {
		// Registering a consumer involves waiting for it to become ACTIVE, so
		// resolve every stream concurrently rather than serialising a minute
		// long wait per stream.
		results := make(chan error, len(k.streams))
		for _, s := range k.streams {
			go func(info *streamInfo) {
				// A previous Connect attempt may already have resolved this
				// stream, in which case there is nothing left to do.
				if info.consumerARN != "" {
					results <- nil
					return
				}
				arn, err := ensureEFOConsumer(ctx, svc, info.arn, k.conf.EFOConsumerName, k.conf.EFOActivationTimeout, k.log)
				if err == nil {
					info.consumerARN = arn
				}
				results <- err
			}(s)
		}

		// Every goroutine is drained before returning so that no writer to
		// info.consumerARN outlives this call and races a retried Connect.
		var efoErr error
		for range k.streams {
			if err := <-results; err != nil && efoErr == nil {
				efoErr = err
			}
		}
		if efoErr != nil {
			return efoErr
		}
	}

	// Only mark the connection as established once every fallible step above
	// has succeeded; otherwise a retried Connect would see a non-nil msgChan
	// and return early without ever starting the shard runners.
	k.msgChan = make(chan asyncMessage)

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
