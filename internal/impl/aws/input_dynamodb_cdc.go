// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/dynamocdc"
)

const (
	defaultDynamoDBBatchSize       = 1000 // AWS max limit
	defaultDynamoDBPollInterval    = "1s"
	defaultDynamoDBThrottleBackoff = "100ms"
	defaultShutdownTimeout         = 10 * time.Second

	// Metrics
	metricShardsTracked = "dynamodb_cdc_shards_tracked"
	metricShardsActive  = "dynamodb_cdc_shards_active"
)

func dynamoDBCDCInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("4.79.0").
		Categories("Services", "AWS").
		Summary("Reads change data capture (CDC) events from DynamoDB Streams.").
		Description(`
Consumes records from DynamoDB Streams with automatic checkpointing and shard management.

DynamoDB Streams capture item-level changes in DynamoDB tables. This input supports:

- Automatic shard discovery and management
- Checkpoint-based resumption after restarts
- Concurrent processing of multiple shards

### Prerequisites

The source DynamoDB table must have streams enabled. You can enable streams with one of these view types:

- `+"`KEYS_ONLY`"+` - Only the key attributes of the modified item
- `+"`NEW_IMAGE`"+` - The entire item as it appears after the modification
- `+"`OLD_IMAGE`"+` - The entire item as it appeared before the modification
- `+"`NEW_AND_OLD_IMAGES`"+` - Both the new and old item images

### Checkpointing

Checkpoints are stored in a separate DynamoDB table (configured via `+"`checkpoint_table`"+`). This table is created automatically if it does not exist. On restart, the input resumes from the last checkpointed position for each shard.

### Alternative

For better performance and longer retention (up to 1 year vs 24 hours), consider using Kinesis Data Streams for DynamoDB with the `+"`aws_kinesis`"+` input instead.

### Metadata

This input adds the following metadata fields to each message:

- `+"`dynamodb_shard_id`"+` - The shard ID from which the record was read
- `+"`dynamodb_sequence_number`"+` - The sequence number of the record in the stream
- `+"`dynamodb_event_name`"+` - The type of change: INSERT, MODIFY, or REMOVE
- `+"`dynamodb_table`"+` - The name of the DynamoDB table

### Metrics

This input emits the following metrics:

- `+"`dynamodb_cdc_shards_tracked`"+` - Total number of shards being tracked (gauge)
- `+"`dynamodb_cdc_shards_active`"+` - Number of shards currently being read from (gauge)
`).
		Fields(
			service.NewStringField("table").
				Description("The name of the DynamoDB table to read streams from.").
				LintRule(`root = if this == "" { ["table name cannot be empty"] }`),
			service.NewStringField("checkpoint_table").
				Description("DynamoDB table name for storing checkpoints. Will be created if it doesn't exist.").
				Default("redpanda_dynamodb_checkpoints"),
			service.NewIntField("batch_size").
				Description("Maximum number of records to read per shard in a single request. Valid range: 1-1000.").
				Default(defaultDynamoDBBatchSize).
				Advanced(),
			service.NewDurationField("poll_interval").
				Description("Time to wait between polling attempts when no records are available.").
				Default(defaultDynamoDBPollInterval).
				Advanced(),
			service.NewStringEnumField("start_from", "trim_horizon", "latest").
				Description("Where to start reading when no checkpoint exists. `trim_horizon` starts from the oldest available record, `latest` starts from new records.").
				Default("trim_horizon"),
			service.NewIntField("checkpoint_limit").
				Description("Maximum number of unacknowledged messages before forcing a checkpoint update. Lower values provide better recovery guarantees but increase write overhead.").
				Default(1000).
				Advanced(),
			service.NewIntField("max_tracked_shards").
				Description("Maximum number of shards to track simultaneously. Prevents memory issues with extremely large tables.").
				Default(10000).
				Advanced(),
			service.NewDurationField("throttle_backoff").
				Description("Time to wait when applying backpressure due to too many in-flight messages.").
				Default(defaultDynamoDBThrottleBackoff).
				Advanced(),
		).
		Fields(config.SessionFields()...).
		Example(
			"Consume CDC events",
			"Read change events from a DynamoDB table with streams enabled.",
			`
input:
  aws_dynamodb_cdc:
    table: my-table
    region: us-east-1
`,
		).
		Example(
			"Start from latest",
			"Only process new changes, ignoring existing stream data.",
			`
input:
  aws_dynamodb_cdc:
    table: orders
    start_from: latest
    region: us-west-2
`,
		)
}

func init() {
	err := service.RegisterBatchInput(
		"aws_dynamodb_cdc", dynamoDBCDCInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newDynamoDBCDCInputFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type dynamoDBCDCConfig struct {
	table            string
	checkpointTable  string
	batchSize        int
	pollInterval     time.Duration
	startFrom        string
	checkpointLimit  int
	maxTrackedShards int
	throttleBackoff  time.Duration
}

type dynamoDBCDCInput struct {
	conf          dynamoDBCDCConfig
	awsConf       aws.Config
	dynamoClient  *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	streamArn     *string
	log           *service.Logger
	metrics       dynamoDBCDCMetrics

	mu            sync.RWMutex
	msgChan       chan asyncMessage
	shutSig       *shutdown.Signaller
	checkpointer  *dynamocdc.Checkpointer
	recordBatcher *dynamocdc.RecordBatcher
	shardReaders  map[string]*dynamoDBShardReader

	pendingAcks sync.WaitGroup
	closed      atomic.Bool
}

type dynamoDBCDCMetrics struct {
	shardsTracked *service.MetricGauge
	shardsActive  *service.MetricGauge
}

type dynamoDBShardReader struct {
	shardID   string
	iterator  *string
	exhausted bool
}

func dynamoCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf dynamoDBCDCConfig, err error) {
	if conf.table, err = pConf.FieldString("table"); err != nil {
		return
	}
	if conf.checkpointTable, err = pConf.FieldString("checkpoint_table"); err != nil {
		return
	}
	if conf.batchSize, err = pConf.FieldInt("batch_size"); err != nil {
		return
	}
	if conf.pollInterval, err = pConf.FieldDuration("poll_interval"); err != nil {
		return
	}
	if conf.startFrom, err = pConf.FieldString("start_from"); err != nil {
		return
	}
	if conf.checkpointLimit, err = pConf.FieldInt("checkpoint_limit"); err != nil {
		return
	}
	if conf.maxTrackedShards, err = pConf.FieldInt("max_tracked_shards"); err != nil {
		return
	}
	if conf.throttleBackoff, err = pConf.FieldDuration("throttle_backoff"); err != nil {
		return
	}
	return
}

func newDynamoDBCDCInputFromConfig(pConf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBCDCInput, error) {
	conf, err := dynamoCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}

	awsConf, err := GetSession(context.Background(), pConf)
	if err != nil {
		return nil, err
	}

	return &dynamoDBCDCInput{
		conf:         conf,
		awsConf:      awsConf,
		shardReaders: make(map[string]*dynamoDBShardReader),
		shutSig:      shutdown.NewSignaller(),
		log:          mgr.Logger(),
		metrics: dynamoDBCDCMetrics{
			shardsTracked: mgr.Metrics().NewGauge(metricShardsTracked),
			shardsActive:  mgr.Metrics().NewGauge(metricShardsActive),
		},
	}, nil
}

func (d *dynamoDBCDCInput) Connect(ctx context.Context) error {
	d.dynamoClient = dynamodb.NewFromConfig(d.awsConf)
	d.streamsClient = dynamodbstreams.NewFromConfig(d.awsConf)

	// Get stream ARN
	descTable, err := d.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &d.conf.table,
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return fmt.Errorf("table %s does not exist", d.conf.table)
		}
		return fmt.Errorf("failed to describe table %s: %w", d.conf.table, err)
	}

	d.streamArn = descTable.Table.LatestStreamArn
	if d.streamArn == nil {
		return fmt.Errorf("no stream enabled on table %s", d.conf.table)
	}

	// Initialize checkpointer
	d.checkpointer, err = dynamocdc.NewCheckpointer(ctx, d.dynamoClient, d.conf.checkpointTable, *d.streamArn, d.conf.checkpointLimit, d.log)
	if err != nil {
		return fmt.Errorf("failed to create checkpointer: %w", err)
	}

	// Initialize record batcher
	d.recordBatcher = dynamocdc.NewRecordBatcher(d.conf.maxTrackedShards, d.conf.checkpointLimit, d.log)

	// Initialize message channel
	d.msgChan = make(chan asyncMessage)

	d.log.Infof("Connected to DynamoDB stream: %s", *d.streamArn)

	// Initialize shards
	if err := d.refreshShards(ctx); err != nil {
		return fmt.Errorf("failed to initialize shards: %w", err)
	}

	// Verify at least one shard reader started successfully
	d.mu.Lock()
	activeCount := len(d.shardReaders)
	d.mu.Unlock()

	if activeCount == 0 {
		return errors.New("no active shard readers available - stream may have no shards or all failed to initialize")
	}

	// Start background goroutine to coordinate shard readers
	coordinatorCtx, coordinatorCancel := d.shutSig.SoftStopCtx(context.Background())
	go func() {
		defer coordinatorCancel()
		d.startShardCoordinator(coordinatorCtx)
	}()

	return nil
}

func (d *dynamoDBCDCInput) refreshShards(ctx context.Context) error {
	streamDesc, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: d.streamArn,
	})
	if err != nil {
		return err
	}

	// Collect new shards to add without holding locks during I/O operations
	type shardToAdd struct {
		shardID  string
		iterator *string
	}
	var newShards []shardToAdd

	for _, shard := range streamDesc.StreamDescription.Shards {
		shardID := *shard.ShardId

		// Check if shard already exists (minimize lock hold time)
		d.mu.RLock()
		_, exists := d.shardReaders[shardID]
		d.mu.RUnlock()

		if exists {
			continue
		}

		// Check checkpoint (I/O operation - do not hold lock)
		checkpoint, err := d.checkpointer.Get(ctx, shardID)
		if err != nil {
			d.log.Warnf("Failed to get checkpoint for shard %s: %v", shardID, err)
		}

		var (
			iteratorType   types.ShardIteratorType
			sequenceNumber *string
		)

		if checkpoint != "" {
			iteratorType = types.ShardIteratorTypeAfterSequenceNumber
			sequenceNumber = &checkpoint
			d.log.Infof("Resuming shard %s from checkpoint: %s", shardID, checkpoint)
		} else {
			if d.conf.startFrom == "latest" {
				iteratorType = types.ShardIteratorTypeLatest
			} else {
				iteratorType = types.ShardIteratorTypeTrimHorizon
			}
			d.log.Infof("Starting shard %s from %s", shardID, d.conf.startFrom)
		}

		// Get shard iterator (I/O operation - do not hold lock)
		iter, err := d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         d.streamArn,
			ShardId:           shard.ShardId,
			ShardIteratorType: iteratorType,
			SequenceNumber:    sequenceNumber,
		})
		if err != nil {
			return fmt.Errorf("failed to get iterator for shard %s: %w", shardID, err)
		}

		newShards = append(newShards, shardToAdd{
			shardID:  shardID,
			iterator: iter.ShardIterator,
		})
	}

	// Add all new shard readers in a single critical section
	if len(newShards) > 0 {
		d.mu.Lock()
		for _, s := range newShards {
			// Double-check shard wasn't added by another goroutine
			if _, exists := d.shardReaders[s.shardID]; !exists {
				d.shardReaders[s.shardID] = &dynamoDBShardReader{
					shardID:   s.shardID,
					iterator:  s.iterator,
					exhausted: false,
				}
			}
		}
		totalShards := len(d.shardReaders)
		d.mu.Unlock()

		d.log.Infof("Tracking %d shards", totalShards)
		d.metrics.shardsTracked.Set(int64(totalShards))
	}

	return nil
}

// startShardCoordinator spawns goroutines for each shard and manages shard refresh.
func (d *dynamoDBCDCInput) startShardCoordinator(ctx context.Context) {
	defer func() {
		close(d.msgChan)
		d.shutSig.TriggerHasStopped()
	}()

	// Track running shard readers
	activeShards := make(map[string]context.CancelFunc)
	defer func() {
		// Cancel all active shard readers on shutdown
		for _, cancelFn := range activeShards {
			cancelFn()
		}
	}()

	refreshTicker := time.NewTicker(30 * time.Second)
	defer refreshTicker.Stop()

	for {
		// Get current shard readers
		d.mu.RLock()
		currentReaders := make(map[string]*dynamoDBShardReader)
		maps.Copy(currentReaders, d.shardReaders)
		d.mu.RUnlock()

		// Start new shard readers for any new shards
		for shardID, reader := range currentReaders {
			if _, exists := activeShards[shardID]; !exists && !reader.exhausted {
				shardCtx, shardCancel := context.WithCancel(ctx)
				activeShards[shardID] = shardCancel
				go d.startShardReader(shardCtx, shardID)
			}
		}

		// Update active shards metric
		activeCount := 0
		for shardID := range activeShards {
			d.mu.RLock()
			reader, exists := d.shardReaders[shardID]
			d.mu.RUnlock()
			if exists && !reader.exhausted {
				activeCount++
			}
		}
		d.metrics.shardsActive.Set(int64(activeCount))

		select {
		case <-ctx.Done():
			return
		case <-refreshTicker.C:
			// Refresh shards periodically to discover new shards
			// Use a timeout context to prevent blocking on shutdown
			refreshCtx, refreshCancel := context.WithTimeout(ctx, 30*time.Second)
			if err := d.refreshShards(refreshCtx); err != nil && !errors.Is(err, context.Canceled) {
				d.log.Warnf("Failed to refresh shards: %v", err)
			}
			refreshCancel()
		}
	}
}

// startShardReader continuously reads from a single shard and sends batches to the channel
func (d *dynamoDBCDCInput) startShardReader(ctx context.Context, shardID string) {
	d.log.Debugf("Starting reader for shard %s", shardID)
	defer d.log.Debugf("Stopped reader for shard %s", shardID)

	pollTicker := time.NewTicker(d.conf.pollInterval)
	defer pollTicker.Stop()

	// Initialize backoff for throttling errors
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = 200 * time.Millisecond
	boff.MaxInterval = 2 * time.Second
	boff.MaxElapsedTime = 0 // Never give up

	for {
		select {
		case <-ctx.Done():
			return
		case <-pollTicker.C:
			// Check for cancellation before expensive operations
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Apply backpressure if too many messages are in flight
			for d.recordBatcher != nil && d.recordBatcher.ShouldThrottle() {
				d.log.Debugf("Throttling shard %s due to too many in-flight messages", shardID)
				select {
				case <-ctx.Done():
					return
				case <-time.After(d.conf.throttleBackoff):
				}
			}

			// Get current reader state
			d.mu.RLock()
			reader, exists := d.shardReaders[shardID]
			if !exists {
				d.mu.RUnlock()
				d.log.Errorf("BUG: shard reader for %s not found in map", shardID)
				return
			}
			if reader.exhausted || reader.iterator == nil {
				d.mu.RUnlock()
				return
			}
			iterator := reader.iterator
			d.mu.RUnlock()

			// Read records from the shard (I/O operation - no lock held)
			getRecords, err := d.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iterator,
				Limit:         aws.Int32(int32(d.conf.batchSize)),
			})
			if err != nil {
				if isThrottlingError(err) {
					wait := boff.NextBackOff()
					d.log.Debugf("Throttled on shard %s, backing off for %v", shardID, wait)
					time.Sleep(wait)
					continue
				}
				d.log.Errorf("Failed to get records from shard %s: %v", shardID, err)
				// On error, wait and retry (don't mark as exhausted)
				continue
			}

			// Success - reset backoff
			boff.Reset()

			// Update iterator
			d.mu.Lock()
			reader.iterator = getRecords.NextShardIterator
			if reader.iterator == nil {
				reader.exhausted = true
				d.log.Infof("Shard %s exhausted", shardID)
				d.mu.Unlock()
				return
			}
			d.mu.Unlock()

			if len(getRecords.Records) == 0 {
				continue
			}

			// Convert records to messages
			batch := d.convertRecordsToBatch(getRecords.Records, shardID)
			if len(batch) == 0 {
				continue
			}

			// Track messages in batcher
			batch = d.recordBatcher.AddMessages(batch, shardID)

			// Track pending ack
			d.pendingAcks.Add(1)

			// Create ack function
			checkpointer := d.checkpointer
			recordBatcher := d.recordBatcher
			ackFunc := func(ackCtx context.Context, err error) error {
				defer d.pendingAcks.Done()

				// Check if already closed
				if d.closed.Load() {
					d.log.Warn("Received ack after close, dropping")
					if err == nil && recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return nil
				}

				if err != nil {
					d.log.Warnf("Batch nacked from shard %s: %v", shardID, err)
					if recordBatcher != nil {
						recordBatcher.RemoveMessages(batch)
					}
					return err // Propagate nack error
				}

				// Mark messages as acked and checkpoint if needed
				if recordBatcher != nil && checkpointer != nil {
					if ackErr := recordBatcher.AckMessages(ackCtx, checkpointer, batch); ackErr != nil {
						d.log.Errorf("Failed to checkpoint shard %s after ack: %v", shardID, ackErr)
						return ackErr // Propagate checkpoint failure
					}
					d.log.Debugf("Successfully checkpointed %d messages from shard %s", len(batch), shardID)
				}
				return nil
			}

			// Send to channel
			select {
			case <-ctx.Done():
				return
			case d.msgChan <- asyncMessage{msg: batch, ackFn: ackFunc}:
				d.log.Debugf("Sent batch of %d records from shard %s", len(batch), shardID)
			}
		}
	}
}

// convertRecordsToBatch converts DynamoDB Stream records to Benthos messages
func (d *dynamoDBCDCInput) convertRecordsToBatch(records []types.Record, shardID string) service.MessageBatch {
	batch := make(service.MessageBatch, 0, len(records))

	for _, record := range records {
		msg := service.NewMessage(nil)

		// Structure similar to Kinesis format for consistency
		recordData := map[string]any{
			"tableName":    d.conf.table,
			"eventID":      aws.ToString(record.EventID),
			"eventName":    string(record.EventName),
			"eventVersion": aws.ToString(record.EventVersion),
			"eventSource":  aws.ToString(record.EventSource),
			"awsRegion":    aws.ToString(record.AwsRegion),
		}

		var sequenceNumber string
		if record.Dynamodb != nil {
			dynamoData := map[string]any{
				"sequenceNumber": aws.ToString(record.Dynamodb.SequenceNumber),
				"streamViewType": string(record.Dynamodb.StreamViewType),
			}

			if record.Dynamodb.Keys != nil {
				dynamoData["keys"] = convertAttributeMap(record.Dynamodb.Keys)
			}
			if record.Dynamodb.NewImage != nil {
				dynamoData["newImage"] = convertAttributeMap(record.Dynamodb.NewImage)
			}
			if record.Dynamodb.OldImage != nil {
				dynamoData["oldImage"] = convertAttributeMap(record.Dynamodb.OldImage)
			}
			if record.Dynamodb.SizeBytes != nil {
				dynamoData["sizeBytes"] = *record.Dynamodb.SizeBytes
			}

			recordData["dynamodb"] = dynamoData
			sequenceNumber = aws.ToString(record.Dynamodb.SequenceNumber)
		}

		msg.SetStructured(recordData)

		// Set metadata
		msg.MetaSetMut("dynamodb_shard_id", shardID)
		msg.MetaSetMut("dynamodb_sequence_number", sequenceNumber)
		msg.MetaSetMut("dynamodb_event_name", string(record.EventName))
		msg.MetaSetMut("dynamodb_table", d.conf.table)

		batch = append(batch, msg)
	}

	return batch
}

func (d *dynamoDBCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	d.mu.RLock()
	msgChan := d.msgChan
	shutSig := d.shutSig
	d.mu.RUnlock()

	if msgChan == nil || shutSig == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-shutSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case am, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return am.msg, am.ackFn, nil
	}
}

func (d *dynamoDBCDCInput) Close(ctx context.Context) error {
	// Mark as closed to reject new acks
	d.closed.Store(true)

	d.mu.RLock()
	shutSig := d.shutSig
	checkpointer := d.checkpointer
	batcher := d.recordBatcher
	d.mu.RUnlock()

	// Trigger graceful shutdown
	d.log.Debug("Initiating graceful shutdown")
	shutSig.TriggerSoftStop()

	// Wait for background goroutines to stop
	select {
	case <-shutSig.HasStoppedChan():
		d.log.Debug("Background goroutines stopped")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for background goroutines to stop")
		// Trigger hard stop if graceful shutdown times out
		shutSig.TriggerHardStop()
	}

	// Wait for pending acknowledgments with timeout
	d.log.Debug("Waiting for pending acknowledgments")
	acksDone := make(chan struct{})
	go func() {
		d.pendingAcks.Wait()
		close(acksDone)
	}()

	select {
	case <-acksDone:
		d.log.Debug("All pending acks completed")
	case <-time.After(defaultShutdownTimeout):
		d.log.Warn("Timeout waiting for pending acks, proceeding with shutdown")
	}

	// Flush any pending checkpoints
	if checkpointer != nil && batcher != nil {
		pendingCheckpoints := batcher.GetPendingCheckpoints()
		if len(pendingCheckpoints) > 0 {
			d.log.Infof("Flushing %d pending checkpoints on close", len(pendingCheckpoints))
			if err := checkpointer.FlushCheckpoints(ctx, pendingCheckpoints); err != nil {
				d.log.Errorf("Failed to flush checkpoints: %v", err)
				// Don't return error - continue cleanup to avoid resource leaks
			}
		}
	} else {
		d.log.Debug("Skipping checkpoint flush - components not initialized")
	}

	// Clear references to help GC
	d.mu.Lock()
	d.dynamoClient = nil
	d.streamsClient = nil
	d.shardReaders = nil
	d.checkpointer = nil
	d.recordBatcher = nil
	d.msgChan = nil
	d.shutSig = nil
	d.mu.Unlock()

	return nil
}

// Helper to convert DynamoDB attribute values to Go types
func convertAttributeMap(attrs map[string]types.AttributeValue) map[string]any {
	result := make(map[string]any)
	for k, v := range attrs {
		result[k] = convertAttributeValue(v)
	}
	return result
}

// isThrottlingError checks if an error is due to AWS throttling.
func isThrottlingError(err error) bool {
	if err == nil {
		return false
	}
	var limitErr *types.LimitExceededException
	var throttleErr *types.TrimmedDataAccessException
	return errors.As(err, &limitErr) || errors.As(err, &throttleErr)
}

func convertAttributeValue(attr types.AttributeValue) any {
	switch v := attr.(type) {
	case *types.AttributeValueMemberS:
		return v.Value
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberB:
		return v.Value
	case *types.AttributeValueMemberSS:
		return v.Value
	case *types.AttributeValueMemberNS:
		return v.Value
	case *types.AttributeValueMemberBS:
		return v.Value
	case *types.AttributeValueMemberM:
		return convertAttributeMap(v.Value)
	case *types.AttributeValueMemberL:
		list := make([]any, len(v.Value))
		for i, item := range v.Value {
			list[i] = convertAttributeValue(item)
		}
		return list
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberBOOL:
		return v.Value
	default:
		return nil
	}
}
