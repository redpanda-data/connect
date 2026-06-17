// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// checkpointDynamoAPI is the subset of the DynamoDB client the Checkpointer
// uses. *dynamodb.Client satisfies it; tests provide a fake.
type checkpointDynamoAPI interface {
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// CheckpointerConfig carries the construction inputs for a Checkpointer.
type CheckpointerConfig struct {
	TableName       string // checkpoint table name
	SourceTable     string // source table name (portable key in global mode)
	StreamArn       string // current region's stream ARN
	CheckpointLimit int
	GlobalTable     bool
	Region          string   // pipeline's own region (global mode)
	ReplicaRegions  []string // other regions to replicate to (global mode)
}

// Checkpointer manages checkpoints for DynamoDB CDC shards in a DynamoDB table.
// It stores the last processed sequence number for each shard, enabling resumption
// from the last checkpoint after restarts.
type Checkpointer struct {
	tableName       string
	sourceTable     string
	streamArn       string
	checkpointLimit int
	globalTable     bool
	region          string
	replicaRegions  []string
	svc             checkpointDynamoAPI
	log             *service.Logger

	// resume resolution cache (global mode only)
	resumeOnce     sync.Once
	resumeErr      error
	ownSeqs        map[string]string
	hasForeignRows bool
	cutoff         time.Time
}

// NewCheckpointer creates a new [Checkpointer] for DynamoDB CDC.
func NewCheckpointer(
	ctx context.Context,
	svc checkpointDynamoAPI,
	cfg CheckpointerConfig,
	log *service.Logger,
) (*Checkpointer, error) {
	c := &Checkpointer{
		tableName:       cfg.TableName,
		sourceTable:     cfg.SourceTable,
		streamArn:       cfg.StreamArn,
		checkpointLimit: cfg.CheckpointLimit,
		globalTable:     cfg.GlobalTable,
		region:          cfg.Region,
		replicaRegions:  cfg.ReplicaRegions,
		svc:             svc,
		log:             log,
	}

	if err := c.ensureTableExists(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Checkpointer) ensureTableExists(ctx context.Context) error {
	desc, err := c.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})
	if err == nil {
		// Table exists. In global mode, reconcile missing replicas only.
		if c.globalTable {
			return c.reconcileReplicas(ctx, desc.Table)
		}
		return nil
	}
	if _, ok := errors.AsType[*types.ResourceNotFoundException](err); !ok {
		return err
	}

	// Table doesn't exist, create it.
	hashAttr := "StreamArn"
	if c.globalTable {
		hashAttr = "TableId"
	}
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(hashAttr), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ShardID"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(hashAttr), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
		},
		TableName: aws.String(c.tableName),
	}
	if c.globalTable {
		// Global Tables v2 require DynamoDB Streams with NEW_AND_OLD_IMAGES.
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		}
	}

	if _, err = c.svc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("creating checkpoint table: %w", err)
	}
	c.log.Infof("Created checkpoint table: %s", c.tableName)

	if c.globalTable {
		if err := c.waitForTableActive(ctx); err != nil {
			return err
		}
		desc, derr := c.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(c.tableName)})
		if derr != nil {
			return fmt.Errorf("describing checkpoint table after create: %w", derr)
		}
		return c.reconcileReplicas(ctx, desc.Table)
	}
	return nil
}

// desiredReplicaRegions returns the home region plus configured replicas.
func (c *Checkpointer) desiredReplicaRegions() []string {
	out := []string{c.region}
	for _, r := range c.replicaRegions {
		if r != c.region {
			out = append(out, r)
		}
	}
	return out
}

// reconcileReplicas adds any configured replica region (excluding the home
// region, which is the table's own region) that is not already present. One
// replica is created per UpdateTable call, waiting for ACTIVE between calls.
func (c *Checkpointer) reconcileReplicas(ctx context.Context, table *types.TableDescription) error {
	existing := map[string]struct{}{}
	if table != nil {
		for _, r := range table.Replicas {
			existing[aws.ToString(r.RegionName)] = struct{}{}
		}
	}
	for _, region := range c.desiredReplicaRegions() {
		if region == c.region {
			continue // home region is implicit, never added as a replica
		}
		if _, ok := existing[region]; ok {
			continue
		}
		c.log.Infof("Adding global table replica %s to %s", region, c.tableName)
		if _, err := c.svc.UpdateTable(ctx, &dynamodb.UpdateTableInput{
			TableName: aws.String(c.tableName),
			ReplicaUpdates: []types.ReplicationGroupUpdate{
				{Create: &types.CreateReplicationGroupMemberAction{RegionName: aws.String(region)}},
			},
		}); err != nil {
			return fmt.Errorf("adding replica %s to checkpoint table: %w", region, err)
		}
		if err := c.waitForTableActive(ctx); err != nil {
			return fmt.Errorf("waiting for replica %s to become active: %w", region, err)
		}
	}
	return nil
}

// waitForTableActive blocks until the checkpoint table reports ACTIVE.
func (c *Checkpointer) waitForTableActive(ctx context.Context) error {
	waiter := dynamodb.NewTableExistsWaiter(c.svc)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	}, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for checkpoint table %s to become active: %w", c.tableName, err)
	}
	return nil
}

// checkpointKey builds the primary key for a shard's checkpoint row. In global
// mode the hash key is the source table name (portable across regions); in the
// default mode it is the current stream ARN.
func (c *Checkpointer) checkpointKey(shardID string) map[string]types.AttributeValue {
	if c.globalTable {
		return map[string]types.AttributeValue{
			"TableId": &types.AttributeValueMemberS{Value: c.sourceTable},
			"ShardID": &types.AttributeValueMemberS{Value: shardID},
		}
	}
	return map[string]types.AttributeValue{
		"StreamArn": &types.AttributeValueMemberS{Value: c.streamArn},
		"ShardID":   &types.AttributeValueMemberS{Value: shardID},
	}
}

// Get retrieves the checkpoint for a shard.
func (c *Checkpointer) Get(ctx context.Context, shardID string) (string, error) {
	result, err := c.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       c.checkpointKey(shardID),
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
			return "", nil
		}
		return "", fmt.Errorf("getting checkpoint for table=%s stream=%s shard=%s: %w",
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

// Set stores a checkpoint for a shard. approxCreationTime is the RFC3339Nano
// ApproximateCreationDateTime of the checkpointed record (empty if unknown); it
// is persisted only in global mode, where it drives time-based failover resume.
func (c *Checkpointer) Set(ctx context.Context, shardID, sequenceNumber, approxCreationTime string) error {
	item := c.checkpointKey(shardID)
	item["SequenceNumber"] = &types.AttributeValueMemberS{Value: sequenceNumber}
	if c.globalTable {
		item["StreamArn"] = &types.AttributeValueMemberS{Value: c.streamArn}
		if approxCreationTime != "" {
			item["ApproximateCreationTime"] = &types.AttributeValueMemberS{Value: approxCreationTime}
		}
	}
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("setting checkpoint for table=%s stream=%s shard=%s seq=%s: %w",
			c.tableName, c.streamArn, shardID, sequenceNumber, err)
	}
	return nil
}

// CheckpointLimit returns the checkpoint limit for the checkpointer.
func (c *Checkpointer) CheckpointLimit() int {
	return c.checkpointLimit
}

type resumeMode int

const (
	// resumeDefault: no usable checkpoint; caller applies start_from.
	resumeDefault resumeMode = iota
	// resumeExact: resume via AfterSequenceNumber (in-region restart).
	resumeExact
	// resumeFailover: resume from TrimHorizon, skipping records at/under Cutoff.
	resumeFailover
)

// ResumeDecision tells the caller how to begin reading a shard.
type ResumeDecision struct {
	Mode           resumeMode
	SequenceNumber string    // set when Mode == resumeExact
	Cutoff         time.Time // set when Mode == resumeFailover (zero => no skip)
}

// ResolveResume decides how to resume the given shard. In the default
// (non-global) mode it preserves today's behavior exactly. In global mode it
// queries the table-name partition once, caches the result, and returns an
// exact resume if rows exist for the current stream, a time-based failover
// resume if only another region's rows exist, or default otherwise.
func (c *Checkpointer) ResolveResume(ctx context.Context, shardID string) (ResumeDecision, error) {
	if !c.globalTable {
		seq, err := c.Get(ctx, shardID)
		if err != nil {
			return ResumeDecision{}, err
		}
		if seq != "" {
			return ResumeDecision{Mode: resumeExact, SequenceNumber: seq}, nil
		}
		return ResumeDecision{Mode: resumeDefault}, nil
	}

	c.resumeOnce.Do(func() { c.resumeErr = c.prepareResume(ctx) })
	if c.resumeErr != nil {
		return ResumeDecision{}, c.resumeErr
	}

	if len(c.ownSeqs) > 0 {
		if seq, ok := c.ownSeqs[shardID]; ok {
			return ResumeDecision{Mode: resumeExact, SequenceNumber: seq}, nil
		}
		return ResumeDecision{Mode: resumeDefault}, nil
	}
	if c.hasForeignRows {
		return ResumeDecision{Mode: resumeFailover, Cutoff: c.cutoff}, nil
	}
	return ResumeDecision{Mode: resumeDefault}, nil
}

// prepareResume queries the whole table-name partition once and classifies rows
// into "own" (current stream) sequence numbers and the minimum
// ApproximateCreationTime across foreign-stream shard rows.
func (c *Checkpointer) prepareResume(ctx context.Context) error {
	c.ownSeqs = map[string]string{}
	var cutoffSet bool

	var startKey map[string]types.AttributeValue
	for {
		out, err := c.svc.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(c.tableName),
			KeyConditionExpression: aws.String("TableId = :hash"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hash": &types.AttributeValueMemberS{Value: c.sourceTable},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
				return nil
			}
			return fmt.Errorf("querying checkpoint partition for %s: %w", c.sourceTable, err)
		}
		for _, item := range out.Items {
			seqAttr, isShardRow := item["SequenceNumber"].(*types.AttributeValueMemberS)
			if !isShardRow {
				continue // snapshot row, not a shard checkpoint
			}
			shardAttr, _ := item["ShardID"].(*types.AttributeValueMemberS)
			streamAttr, _ := item["StreamArn"].(*types.AttributeValueMemberS)
			if streamAttr != nil && streamAttr.Value == c.streamArn {
				if shardAttr != nil {
					c.ownSeqs[shardAttr.Value] = seqAttr.Value
				}
				continue
			}
			// Foreign-stream row: contribute to the low-water-mark cutoff.
			c.hasForeignRows = true
			if tsAttr, ok := item["ApproximateCreationTime"].(*types.AttributeValueMemberS); ok && tsAttr.Value != "" {
				if ts, perr := time.Parse(time.RFC3339Nano, tsAttr.Value); perr == nil {
					if !cutoffSet || ts.Before(c.cutoff) {
						c.cutoff = ts
						cutoffSet = true
					}
				}
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return nil
}

// CheckpointValue is the persisted state for one shard: the sequence number and
// the ApproximateCreationDateTime (RFC3339Nano) of that record.
type CheckpointValue struct {
	SequenceNumber     string
	ApproxCreationTime string
}

// FlushCheckpoints writes all pending checkpoints to DynamoDB.
func (c *Checkpointer) FlushCheckpoints(ctx context.Context, checkpoints map[string]CheckpointValue) error {
	for shardID, v := range checkpoints {
		if v.SequenceNumber == "" {
			continue
		}
		if err := c.Set(ctx, shardID, v.SequenceNumber, v.ApproxCreationTime); err != nil {
			c.log.Errorf("Failed to flush checkpoint for shard %s: %v", shardID, err)
			return err
		}
		c.log.Infof("Flushed checkpoint for shard %s at sequence %s", shardID, v.SequenceNumber)
	}
	return nil
}

// SnapshotProgress retrieves the snapshot checkpoint.
func (c *Checkpointer) SnapshotProgress(ctx context.Context) (*SnapshotCheckpoint, error) {
	checkpoint := NewSnapshotCheckpoint()

	res, err := c.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       c.checkpointKey("snapshot#complete"),
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); !ok {
			return nil, fmt.Errorf("getting snapshot completion status: %w", err)
		}
	}

	if res != nil && res.Item != nil {
		if complete, ok := res.Item["Complete"].(*types.AttributeValueMemberBOOL); ok && complete.Value {
			checkpoint.MarkComplete()
			return checkpoint, nil
		}
	}

	hashName, hashVal := "StreamArn", c.streamArn
	if c.globalTable {
		hashName, hashVal = "TableId", c.sourceTable
	}
	queryRes, err := c.svc.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(c.tableName),
		KeyConditionExpression: aws.String(hashName + " = :hash AND begins_with(ShardID, :snapshot_prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":hash":            &types.AttributeValueMemberS{Value: hashVal},
			":snapshot_prefix": &types.AttributeValueMemberS{Value: "snapshot#segment#"},
		},
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); !ok {
			return nil, fmt.Errorf("querying snapshot progress: %w", err)
		}
		return checkpoint, nil
	}

	for _, item := range queryRes.Items {
		shardID, ok := item["ShardID"].(*types.AttributeValueMemberS)
		if !ok {
			c.log.Warn("Unexpected ShardID type in snapshot checkpoint item, skipping.")
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

// UpdateSnapshotProgress updates the checkpoint for a snapshot segment.
func (c *Checkpointer) UpdateSnapshotProgress(ctx context.Context, segment int, lastKey map[string]types.AttributeValue, recordsRead int64) error {
	shardID := fmt.Sprintf("snapshot#segment#%d", segment)

	item := c.checkpointKey(shardID)
	item["RecordsRead"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(recordsRead, 10)}

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
		return fmt.Errorf("updating snapshot progress for segment %d: %w", segment, err)
	}

	return nil
}

// MarkSnapshotComplete marks the entire snapshot as complete.
func (c *Checkpointer) MarkSnapshotComplete(ctx context.Context) error {
	item := c.checkpointKey("snapshot#complete")
	item["Complete"] = &types.AttributeValueMemberBOOL{Value: true}
	_, err := c.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("marking snapshot complete: %w", err)
	}

	c.log.Info("Marked snapshot as complete in checkpoint table")
	return nil
}
