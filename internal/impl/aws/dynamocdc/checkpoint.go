// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamocdc

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Checkpointer manages checkpoints for DynamoDB CDC shards in a DynamoDB table.
// It stores the last processed sequence number for each shard, enabling resumption
// from the last checkpoint after restarts.
type Checkpointer struct {
	tableName       string
	streamArn       string
	checkpointLimit int
	svc             *dynamodb.Client
	log             *service.Logger
}

// NewCheckpointer creates a new [Checkpointer] for DynamoDB CDC.
func NewCheckpointer(
	ctx context.Context,
	svc *dynamodb.Client,
	tableName,
	streamArn string,
	checkpointLimit int,
	log *service.Logger,
) (*Checkpointer, error) {
	c := &Checkpointer{
		tableName:       tableName,
		streamArn:       streamArn,
		checkpointLimit: checkpointLimit,
		svc:             svc,
		log:             log,
	}

	if err := c.ensureTableExists(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Checkpointer) ensureTableExists(ctx context.Context) error {
	_, err := c.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})

	var aerr *types.ResourceNotFoundException
	if err == nil || !errors.As(err, &aerr) {
		return err
	}

	// Table doesn't exist, create it
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("StreamArn"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ShardID"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("StreamArn"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
		},
		TableName: aws.String(c.tableName),
	}

	if _, err = c.svc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	c.log.Infof("Created checkpoint table: %s", c.tableName)
	return nil
}

// Get retrieves the checkpoint for a shard.
func (c *Checkpointer) Get(ctx context.Context, shardID string) (string, error) {
	result, err := c.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			"StreamArn": &types.AttributeValueMemberS{Value: c.streamArn},
			"ShardID":   &types.AttributeValueMemberS{Value: shardID},
		},
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return "", nil
		}
		return "", fmt.Errorf("failed to get checkpoint for table=%s stream=%s shard=%s: %w",
			c.tableName, c.streamArn, shardID, err)
	}

	if result.Item == nil {
		return "", nil
	}

	if s, ok := result.Item["SequenceNumber"].(*types.AttributeValueMemberS); ok {
		return s.Value, nil
	}

	return "", nil
}

// Set stores a checkpoint for a shard.
func (c *Checkpointer) Set(ctx context.Context, shardID, sequenceNumber string) error {
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item: map[string]types.AttributeValue{
			"StreamArn":      &types.AttributeValueMemberS{Value: c.streamArn},
			"ShardID":        &types.AttributeValueMemberS{Value: shardID},
			"SequenceNumber": &types.AttributeValueMemberS{Value: sequenceNumber},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to set checkpoint for table=%s stream=%s shard=%s seq=%s: %w",
			c.tableName, c.streamArn, shardID, sequenceNumber, err)
	}
	return nil
}

// GetCheckpointLimit returns the checkpoint limit for the checkpointer.
func (c *Checkpointer) GetCheckpointLimit() int {
	return c.checkpointLimit
}

// FlushCheckpoints writes all pending checkpoints to DynamoDB.
func (c *Checkpointer) FlushCheckpoints(ctx context.Context, checkpoints map[string]string) error {
	for shardID, seq := range checkpoints {
		if seq == "" {
			continue
		}
		if err := c.Set(ctx, shardID, seq); err != nil {
			c.log.Errorf("Failed to flush checkpoint for shard %s: %v", shardID, err)
			return err
		}
		c.log.Infof("Flushed checkpoint for shard %s at sequence %s", shardID, seq)
	}
	return nil
}

// GetSnapshotProgress retrieves the snapshot checkpoint
func (c *Checkpointer) GetSnapshotProgress(ctx context.Context) (*SnapshotCheckpoint, error) {
	checkpoint := NewSnapshotCheckpoint()

	// Check if snapshot is complete
	result, err := c.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			"StreamArn": &types.AttributeValueMemberS{Value: c.streamArn},
			"ShardID":   &types.AttributeValueMemberS{Value: "snapshot#complete"},
		},
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if !errors.As(err, &aerr) {
			return nil, fmt.Errorf("failed to get snapshot completion status: %w", err)
		}
	}

	if result != nil && result.Item != nil {
		if complete, ok := result.Item["Complete"].(*types.AttributeValueMemberBOOL); ok && complete.Value {
			checkpoint.MarkComplete()
			return checkpoint, nil
		}
	}

	// Query for segment progress
	queryResult, err := c.svc.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(c.tableName),
		KeyConditionExpression: aws.String("StreamArn = :stream_arn AND begins_with(ShardID, :snapshot_prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":stream_arn":      &types.AttributeValueMemberS{Value: c.streamArn},
			":snapshot_prefix": &types.AttributeValueMemberS{Value: "snapshot#segment#"},
		},
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if !errors.As(err, &aerr) {
			return nil, fmt.Errorf("failed to query snapshot progress: %w", err)
		}
		// Table doesn't exist yet, return empty checkpoint
		return checkpoint, nil
	}

	// Parse segment progress from items
	for _, item := range queryResult.Items {
		shardID, ok := item["ShardID"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}

		var segmentID int
		if _, err := fmt.Sscanf(shardID.Value, "snapshot#segment#%d", &segmentID); err != nil {
			c.log.Warnf("Failed to parse segment ID from %s: %v", shardID.Value, err)
			continue
		}

		state := &SegmentState{}

		if lastKey, ok := item["LastKey"].(*types.AttributeValueMemberM); ok {
			state.LastKey = lastKey.Value
		}

		if recordsRead, ok := item["RecordsRead"].(*types.AttributeValueMemberN); ok {
			if _, err := fmt.Sscanf(recordsRead.Value, "%d", &state.RecordsRead); err != nil {
				c.log.Warnf("Failed to parse RecordsRead from checkpoint: %v", err)
			}
		}

		if complete, ok := item["Complete"].(*types.AttributeValueMemberBOOL); ok {
			state.Complete = complete.Value
		}

		checkpoint.SegmentProgress[segmentID] = state
	}

	return checkpoint, nil
}

// UpdateSnapshotProgress updates the checkpoint for a snapshot segment
func (c *Checkpointer) UpdateSnapshotProgress(ctx context.Context, segment int, lastKey map[string]types.AttributeValue, recordsRead int64) error {
	shardID := fmt.Sprintf("snapshot#segment#%d", segment)

	item := map[string]types.AttributeValue{
		"StreamArn":   &types.AttributeValueMemberS{Value: c.streamArn},
		"ShardID":     &types.AttributeValueMemberS{Value: shardID},
		"RecordsRead": &types.AttributeValueMemberN{Value: strconv.FormatInt(recordsRead, 10)},
	}

	if lastKey == nil {
		// Segment complete
		item["Complete"] = &types.AttributeValueMemberBOOL{Value: true}
	} else {
		// Store last key for resume
		item["LastKey"] = &types.AttributeValueMemberM{Value: lastKey}
		item["Complete"] = &types.AttributeValueMemberBOOL{Value: false}
	}

	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to update snapshot progress for segment %d: %w", segment, err)
	}

	return nil
}

// MarkSnapshotComplete marks the entire snapshot as complete
func (c *Checkpointer) MarkSnapshotComplete(ctx context.Context) error {
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item: map[string]types.AttributeValue{
			"StreamArn": &types.AttributeValueMemberS{Value: c.streamArn},
			"ShardID":   &types.AttributeValueMemberS{Value: "snapshot#complete"},
			"Complete":  &types.AttributeValueMemberBOOL{Value: true},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to mark snapshot complete: %w", err)
	}

	c.log.Info("Marked snapshot as complete in checkpoint table")
	return nil
}
