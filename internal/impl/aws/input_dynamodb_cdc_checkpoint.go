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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// dynamoDBCDCCheckpointer manages checkpoints for DynamoDB CDC shards.
type dynamoDBCDCCheckpointer struct {
	tableName       string
	streamArn       string
	checkpointLimit int
	svc             *dynamodb.Client
	log             *service.Logger
}

// newDynamoDBCDCCheckpointer creates a new checkpointer for DynamoDB CDC.
func newDynamoDBCDCCheckpointer(
	ctx context.Context,
	svc *dynamodb.Client,
	tableName,
	streamArn string,
	checkpointLimit int,
	log *service.Logger,
) (*dynamoDBCDCCheckpointer, error) {
	c := &dynamoDBCDCCheckpointer{
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

func (c *dynamoDBCDCCheckpointer) ensureTableExists(ctx context.Context) error {
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
func (c *dynamoDBCDCCheckpointer) Get(ctx context.Context, shardID string) (string, error) {
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
		return "", err
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
func (c *dynamoDBCDCCheckpointer) Set(ctx context.Context, shardID, sequenceNumber string) error {
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item: map[string]types.AttributeValue{
			"StreamArn":      &types.AttributeValueMemberS{Value: c.streamArn},
			"ShardID":        &types.AttributeValueMemberS{Value: shardID},
			"SequenceNumber": &types.AttributeValueMemberS{Value: sequenceNumber},
		},
	})
	return err
}

// FlushCheckpoints writes all pending checkpoints to DynamoDB.
func (c *dynamoDBCDCCheckpointer) FlushCheckpoints(ctx context.Context, checkpoints map[string]string) error {
	// Flush all pending checkpoints
	for shardID, seq := range checkpoints {
		if seq == "" {
			continue
		}
		if err := c.Set(ctx, shardID, seq); err != nil {
			c.log.Errorf("Failed to flush checkpoint for shard %s: %w", shardID, err)
			return err
		}
		c.log.Infof("Flushed checkpoint for shard %s at sequence %s", shardID, seq)
	}
	return nil
}
