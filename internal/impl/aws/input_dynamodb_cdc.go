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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
)

const (
	defaultDynamoDBBatchSize    = 1000 // AWS max limit
	defaultDynamoDBPollInterval = "1s"
)

func dynamoDBCDCInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("1.0.0").
		Categories("Services", "AWS").
		Summary("Reads change data capture (CDC) events from DynamoDB Streams").
		Description(`
Consumes records from DynamoDB Streams with automatic checkpointing and shard management.

DynamoDB Streams capture item-level changes in DynamoDB tables. This input supports:
- Automatic shard discovery and management
- Checkpoint-based resumption after crashes
- Multiple shard processing

For better performance and longer retention, consider using Kinesis Data Streams for DynamoDB
with the `+"`aws_kinesis`"+` input instead.
`).
		Fields(
			service.NewStringField("table").
				Description("The name of the DynamoDB table to read streams from."),
			service.NewStringField("checkpoint_table").
				Description("DynamoDB table name for storing checkpoints. Will be created if it doesn't exist.").
				Default("redpanda_dynamodb_checkpoints"),
			service.NewIntField("batch_size").
				Description("Maximum number of records to read in a single batch.").
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
				Description("Maximum number of messages to process before updating checkpoint.").
				Default(1000).
				Advanced(),
			service.NewIntField("max_tracked_shards").
				Description("Maximum number of shards to track simultaneously. Prevents memory issues with extremely large tables.").
				Default(10000).
				Advanced(),
		).
		Fields(config.SessionFields()...)
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

type dynamoDBCDCInput struct {
	table            string
	checkpointTable  string
	batchSize        int32
	pollInterval     time.Duration
	startFrom        string
	checkpointLimit  int
	maxTrackedShards int

	awsConf       aws.Config
	dynamoClient  *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	streamArn     *string

	checkpointer  *dynamoDBCDCCheckpointer
	recordBatcher *dynamoDBCDCRecordBatcher

	shardReaders map[string]*dynamoDBShardReader
	mu           sync.Mutex
	closed       bool

	log *service.Logger
}

type dynamoDBShardReader struct {
	shardID   string
	iterator  *string
	exhausted bool
}

func newDynamoDBCDCInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*dynamoDBCDCInput, error) {
	awsConf, err := GetSession(context.Background(), conf)
	if err != nil {
		return nil, err
	}

	table, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}

	checkpointTable, err := conf.FieldString("checkpoint_table")
	if err != nil {
		return nil, err
	}

	batchSize, err := conf.FieldInt("batch_size")
	if err != nil {
		return nil, err
	}

	pollInterval, err := conf.FieldDuration("poll_interval")
	if err != nil {
		return nil, err
	}

	startFrom, err := conf.FieldString("start_from")
	if err != nil {
		return nil, err
	}

	checkpointLimit, err := conf.FieldInt("checkpoint_limit")
	if err != nil {
		return nil, err
	}

	maxTrackedShards, err := conf.FieldInt("max_tracked_shards")
	if err != nil {
		return nil, err
	}

	return &dynamoDBCDCInput{
		awsConf:          awsConf,
		table:            table,
		checkpointTable:  checkpointTable,
		batchSize:        int32(batchSize),
		pollInterval:     pollInterval,
		startFrom:        startFrom,
		checkpointLimit:  checkpointLimit,
		maxTrackedShards: maxTrackedShards,
		shardReaders:     make(map[string]*dynamoDBShardReader),
		log:              mgr.Logger(),
	}, nil
}

func (d *dynamoDBCDCInput) Connect(ctx context.Context) error {
	d.dynamoClient = dynamodb.NewFromConfig(d.awsConf)
	d.streamsClient = dynamodbstreams.NewFromConfig(d.awsConf)

	// Get stream ARN
	descTable, err := d.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &d.table,
	})
	if err != nil {
		return fmt.Errorf("failed to describe table %s: %w", d.table, err)
	}

	d.streamArn = descTable.Table.LatestStreamArn
	if d.streamArn == nil {
		return fmt.Errorf("no stream enabled on table %s", d.table)
	}

	// Initialize checkpointer
	d.checkpointer, err = newDynamoDBCDCCheckpointer(ctx, d.dynamoClient, d.checkpointTable, *d.streamArn, d.checkpointLimit, d.log)
	if err != nil {
		return fmt.Errorf("failed to create checkpointer: %w", err)
	}

	// Initialize record batcher
	d.recordBatcher = newDynamoDBCDCRecordBatcher(d.maxTrackedShards, d.log)

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

	return nil
}

func (d *dynamoDBCDCInput) refreshShards(ctx context.Context) error {
	streamDesc, err := d.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: d.streamArn,
	})
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, shard := range streamDesc.StreamDescription.Shards {
		shardID := *shard.ShardId

		if _, exists := d.shardReaders[shardID]; exists {
			continue // Already tracking
		}

		// Check checkpoint
		checkpoint, err := d.checkpointer.Get(ctx, shardID)
		if err != nil {
			d.log.Warnf("Failed to get checkpoint for shard %s: %w", shardID, err)
		}

		var iteratorType types.ShardIteratorType
		var sequenceNumber *string

		if checkpoint != "" {
			iteratorType = types.ShardIteratorTypeAfterSequenceNumber
			sequenceNumber = &checkpoint
			d.log.Infof("Resuming shard %s from checkpoint: %s", shardID, checkpoint)
		} else {
			if d.startFrom == "latest" {
				iteratorType = types.ShardIteratorTypeLatest
			} else {
				iteratorType = types.ShardIteratorTypeTrimHorizon
			}
			d.log.Infof("Starting shard %s from %s", shardID, d.startFrom)
		}

		// Get shard iterator
		iter, err := d.streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         d.streamArn,
			ShardId:           shard.ShardId,
			ShardIteratorType: iteratorType,
			SequenceNumber:    sequenceNumber,
		})
		if err != nil {
			return fmt.Errorf("failed to get iterator for shard %s: %w", shardID, err)
		}

		d.shardReaders[shardID] = &dynamoDBShardReader{
			shardID:   shardID,
			iterator:  iter.ShardIterator,
			exhausted: false,
		}
	}

	d.log.Infof("Tracking %d shards", len(d.shardReaders))
	return nil
}

func (d *dynamoDBCDCInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	d.mu.Lock()
	if d.closed || d.dynamoClient == nil {
		d.mu.Unlock()
		return nil, nil, service.ErrNotConnected
	}

	activeReaders := make([]*dynamoDBShardReader, 0, len(d.shardReaders))
	for _, reader := range d.shardReaders {
		if !reader.exhausted && reader.iterator != nil {
			activeReaders = append(activeReaders, reader)
		}
	}
	d.mu.Unlock()

	if len(activeReaders) == 0 {
		// Try refreshing shards
		if err := d.refreshShards(ctx); err != nil {
			d.log.Warnf("Failed to refresh shards: %w", err)
		}

		time.Sleep(d.pollInterval)
		return nil, nil, nil
	}

	// Read from all shards and aggregate results for better throughput
	allMessages := make(service.MessageBatch, 0)

	for _, reader := range activeReaders {
		// Skip if iterator is nil (shouldn't happen but safety check)
		if reader.iterator == nil {
			d.mu.Lock()
			reader.exhausted = true
			d.mu.Unlock()
			d.log.Warnf("Shard %s has nil iterator, marking as exhausted", reader.shardID)
			continue
		}

		getRecords, err := d.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: reader.iterator,
			Limit:         aws.Int32(d.batchSize),
		})
		if err != nil {
			d.log.Errorf("Failed to get records from shard %s: %w", reader.shardID, err)
			d.mu.Lock()
			reader.exhausted = true
			d.mu.Unlock()
			continue
		}

		// Update iterator
		d.mu.Lock()
		reader.iterator = getRecords.NextShardIterator
		if reader.iterator == nil {
			reader.exhausted = true
			d.log.Infof("Shard %s exhausted", reader.shardID)
		}
		d.mu.Unlock()

		if len(getRecords.Records) == 0 {
			continue
		}

		// Convert records to Benthos messages
		batch := make(service.MessageBatch, 0, len(getRecords.Records))
		lastSequenceNum := ""

		for _, record := range getRecords.Records {
			msg := service.NewMessage(nil)

			// Structure similar to Kinesis format for consistency
			recordData := map[string]any{
				"eventID":      aws.ToString(record.EventID),
				"eventName":    string(record.EventName),
				"eventVersion": aws.ToString(record.EventVersion),
				"eventSource":  aws.ToString(record.EventSource),
				"awsRegion":    aws.ToString(record.AwsRegion),
			}

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
				lastSequenceNum = aws.ToString(record.Dynamodb.SequenceNumber)
			}

			msg.SetStructured(recordData)

			// Set metadata
			msg.MetaSetMut("dynamodb_shard_id", reader.shardID)
			msg.MetaSetMut("dynamodb_sequence_number", lastSequenceNum)
			msg.MetaSetMut("dynamodb_event_name", string(record.EventName))
			msg.MetaSetMut("dynamodb_table", d.table)

			batch = append(batch, msg)
		}

		// Use record batcher for tracking
		batchedMsgs := d.recordBatcher.AddMessages(batch, reader.shardID)
		allMessages = append(allMessages, batchedMsgs...)

		d.log.Debugf("Read batch of %d records from shard %s", len(batchedMsgs), reader.shardID)
	}

	// Return all messages if we got any
	if len(allMessages) > 0 {
		// Create ack function for all messages
		ackFunc := func(ctx context.Context, err error) error {
			if err != nil {
				d.log.Warnf("Batch nacked: %w", err)
				d.recordBatcher.RemoveMessages(allMessages)
				return nil
			}

			// Mark messages as acked and checkpoint if needed
			return d.recordBatcher.AckMessages(ctx, d.checkpointer, allMessages)
		}

		return allMessages, ackFunc, nil
	}

	// No records from any shard
	time.Sleep(d.pollInterval)
	return nil, nil, nil
}

func (d *dynamoDBCDCInput) Close(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true

	// Capture resources for cleanup (may be nil if Connect() never completed)
	checkpointer := d.checkpointer
	batcher := d.recordBatcher

	// Clear references to help GC and prevent accidental reuse
	d.dynamoClient = nil
	d.streamsClient = nil
	d.shardReaders = nil
	d.checkpointer = nil
	d.recordBatcher = nil
	d.mu.Unlock()

	// Flush any pending checkpoints outside the lock
	if checkpointer != nil && batcher != nil {
		pendingCheckpoints := batcher.GetPendingCheckpoints()
		if len(pendingCheckpoints) > 0 {
			d.log.Infof("Flushing %d pending checkpoints on close", len(pendingCheckpoints))
			if err := checkpointer.FlushCheckpoints(ctx, pendingCheckpoints); err != nil {
				d.log.Errorf("Failed to flush checkpoints: %w", err)
				return err
			}
		}
	}

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
