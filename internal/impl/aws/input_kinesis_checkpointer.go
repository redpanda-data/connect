package aws

// Inspired by Patrick Robinson https://github.com/patrobinson/gokini

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/benthosdev/benthos/v4/public/service"
)

// Common errors that might occur throughout checkpointing.
var (
	ErrLeaseNotAcquired = errors.New("the shard could not be leased due to a collision")
)

type kiddbConfig struct {
	Table              string
	Create             bool
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
	BillingMode        string
}

func kinesisInputDynamoDBConfigFromParsed(pConf *service.ParsedConfig) (conf kiddbConfig, err error) {
	if conf.Table, err = pConf.FieldString(kiddbFieldTable); err != nil {
		return
	}
	if conf.Create, err = pConf.FieldBool(kiddbFieldCreate); err != nil {
		return
	}
	if conf.ReadCapacityUnits, err = int64Field(pConf, kiddbFieldReadCapacityUnits); err != nil {
		return
	}
	if conf.WriteCapacityUnits, err = int64Field(pConf, kiddbFieldWriteCapacityUnits); err != nil {
		return
	}
	if conf.BillingMode, err = pConf.FieldString(kiddbFieldBillingMode); err != nil {
		return
	}
	return
}

// awsKinesisCheckpointer manages the shard checkpointing for a given client
// identifier.
type awsKinesisCheckpointer struct {
	conf kiddbConfig

	clientID      string
	leaseDuration time.Duration
	commitPeriod  time.Duration
	svc           *dynamodb.Client
}

// newAWSKinesisCheckpointer creates a new DynamoDB checkpointer from an AWS
// session and a configuration struct.
func newAWSKinesisCheckpointer(
	aConf aws.Config,
	clientID string,
	conf kiddbConfig,
	leaseDuration time.Duration,
	commitPeriod time.Duration,
) (*awsKinesisCheckpointer, error) {
	c := &awsKinesisCheckpointer{
		conf:          conf,
		leaseDuration: leaseDuration,
		commitPeriod:  commitPeriod,
		svc:           dynamodb.NewFromConfig(aConf),
		clientID:      clientID,
	}

	if err := c.ensureTableExists(context.TODO()); err != nil {
		return nil, err
	}
	return c, nil
}

//------------------------------------------------------------------------------

func (k *awsKinesisCheckpointer) ensureTableExists(ctx context.Context) error {
	_, err := k.svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(k.conf.Table),
	})
	{
		var aerr *types.ResourceNotFoundException
		if err == nil || !errors.As(err, &aerr) {
			return err
		}
	}
	if !k.conf.Create {
		return fmt.Errorf("target table %v does not exist", k.conf.Table)
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("StreamID"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ShardID"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingMode(k.conf.BillingMode),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("StreamID"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ShardID"), KeyType: types.KeyTypeRange},
		},
		TableName: aws.String(k.conf.Table),
	}
	if k.conf.BillingMode == "PROVISIONED" {
		input.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  &k.conf.ReadCapacityUnits,
			WriteCapacityUnits: &k.conf.WriteCapacityUnits,
		}
	}
	if _, err = k.svc.CreateTable(ctx, input); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

// awsKinesisCheckpoint contains details of a shard checkpoint.
type awsKinesisCheckpoint struct {
	SequenceNumber string
	ClientID       *string
	LeaseTimeout   *time.Time
}

// Both checkpoint and err can be nil when the item does not exist.
func (k *awsKinesisCheckpointer) getCheckpoint(ctx context.Context, streamID, shardID string) (*awsKinesisCheckpoint, error) {
	rawItem, err := k.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]types.AttributeValue{
			"ShardID": &types.AttributeValueMemberS{
				Value: shardID,
			},
			"StreamID": &types.AttributeValueMemberS{
				Value: streamID,
			},
		},
	})
	if err != nil {
		var aerr *types.ResourceNotFoundException
		if errors.As(err, &aerr) {
			return nil, nil
		}
		return nil, err
	}

	c := awsKinesisCheckpoint{}

	if s, ok := rawItem.Item["SequenceNumber"].(*types.AttributeValueMemberS); ok {
		c.SequenceNumber = s.Value
	} else {
		return nil, errors.New("sequence ID was not found in checkpoint")
	}

	if s, ok := rawItem.Item["ClientID"].(*types.AttributeValueMemberS); ok {
		c.ClientID = &s.Value
	}

	if s, ok := rawItem.Item["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
		timeout, err := time.Parse(time.RFC3339Nano, s.Value)
		if err != nil {
			return nil, err
		}
		c.LeaseTimeout = &timeout
	}

	return &c, nil
}

//------------------------------------------------------------------------------

// awsKinesisClientClaim represents a shard claimed by a client.
type awsKinesisClientClaim struct {
	ShardID      string
	LeaseTimeout time.Time
}

// AllClaims returns a map of client IDs to shards claimed by that client,
// including the lease timeout of the claim.
func (k *awsKinesisCheckpointer) AllClaims(ctx context.Context, streamID string) (map[string][]awsKinesisClientClaim, error) {
	clientClaims := make(map[string][]awsKinesisClientClaim)
	var scanErr error

	scanRes, err := k.svc.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(k.conf.Table),
		FilterExpression: aws.String("StreamID = :stream_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":stream_id": &types.AttributeValueMemberS{
				Value: streamID,
			},
		},
	})
	if err != nil {
		return nil, err
	}

	for _, i := range scanRes.Items {
		var clientID string
		if s, ok := i["ClientID"].(*types.AttributeValueMemberS); ok {
			clientID = s.Value
		} else {
			continue
		}

		var claim awsKinesisClientClaim
		if s, ok := i["ShardID"].(*types.AttributeValueMemberS); ok {
			claim.ShardID = s.Value
		}
		if claim.ShardID == "" {
			return nil, errors.New("failed to extract shard id from claim")
		}

		if s, ok := i["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
			if claim.LeaseTimeout, scanErr = time.Parse(time.RFC3339Nano, s.Value); scanErr != nil {
				return nil, fmt.Errorf("failed to parse claim lease: %w", scanErr)
			}
		}
		if claim.LeaseTimeout.IsZero() {
			return nil, errors.New("failed to extract lease timeout from claim")
		}

		clientClaims[clientID] = append(clientClaims[clientID], claim)
	}

	return clientClaims, scanErr
}

// Claim attempts to claim a shard for a particular stream ID. If fromClientID
// is specified the shard is stolen from that particular client, and the
// operation fails if a different client ID has it claimed.
//
// If fromClientID is specified this call will claim the new shard but block
// for a period of time before reacquiring the sequence ID. This allows the
// client we're claiming from to gracefully update the sequence number before
// stopping.
func (k *awsKinesisCheckpointer) Claim(ctx context.Context, streamID, shardID, fromClientID string) (string, error) {
	newLeaseTimeoutString := time.Now().Add(k.leaseDuration).Format(time.RFC3339Nano)

	var conditionalExpression string
	expressionAttributeValues := map[string]types.AttributeValue{
		":new_client_id": &types.AttributeValueMemberS{
			Value: k.clientID,
		},
		":new_lease_timeout": &types.AttributeValueMemberS{
			Value: newLeaseTimeoutString,
		},
	}

	if len(fromClientID) > 0 {
		conditionalExpression = "ClientID = :old_client_id"
		expressionAttributeValues[":old_client_id"] = &types.AttributeValueMemberS{
			Value: fromClientID,
		}
	} else {
		conditionalExpression = "attribute_not_exists(ClientID)"
	}

	exp := "SET ClientID = :new_client_id, LeaseTimeout = :new_lease_timeout"
	res, err := k.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		ReturnValues:              types.ReturnValueAllOld,
		TableName:                 &k.conf.Table,
		ConditionExpression:       &conditionalExpression,
		UpdateExpression:          &exp,
		ExpressionAttributeValues: expressionAttributeValues,
		Key: map[string]types.AttributeValue{
			"StreamID": &types.AttributeValueMemberS{
				Value: streamID,
			},
			"ShardID": &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})
	if err != nil {
		var aerr *types.ConditionalCheckFailedException
		if errors.As(err, &aerr) {
			return "", ErrLeaseNotAcquired
		}
		return "", err
	}

	var startingSequence string
	if s, ok := res.Attributes["SequenceNumber"].(*types.AttributeValueMemberS); ok {
		startingSequence = s.Value
	}

	var currentLease time.Time
	if s, ok := res.Attributes["LeaseTimeout"].(*types.AttributeValueMemberS); ok {
		currentLease, _ = time.Parse(time.RFC3339Nano, s.Value)
	}

	// Since we've aggressively stolen a shard then it's pretty much guaranteed
	// that the client we're stealing from is still processing. What we do is we
	// wait a grace period calculated by how long since the previous checkpoint
	// and then reacquire the sequence.
	//
	// This allows the victim client to update the checkpoint with the final
	// sequence as it yields the shard.
	if len(fromClientID) > 0 && time.Since(currentLease) < k.leaseDuration {
		// Wait for the estimated next checkpoint time plus a grace period of
		// one second.
		waitFor := k.leaseDuration - time.Since(currentLease) + time.Second
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return "", ctx.Err()
		}

		cp, err := k.getCheckpoint(ctx, streamID, shardID)
		if err != nil {
			return "", err
		}
		startingSequence = cp.SequenceNumber
	}

	return startingSequence, nil
}

// Checkpoint attempts to set a sequence number for a stream shard. Returns a
// boolean indicating whether this shard is still owned by the client.
//
// If the shard has been claimed by a new client the sequence will still be set
// so that the new client can begin with the latest sequence.
//
// If final is true the client ID is removed from the checkpoint, indicating
// that this client is finished with the shard.
func (k *awsKinesisCheckpointer) Checkpoint(ctx context.Context, streamID, shardID, sequenceNumber string, final bool) (bool, error) {
	item := map[string]types.AttributeValue{
		"StreamID": &types.AttributeValueMemberS{
			Value: streamID,
		},
		"ShardID": &types.AttributeValueMemberS{
			Value: shardID,
		},
	}

	if len(sequenceNumber) > 0 {
		item["SequenceNumber"] = &types.AttributeValueMemberS{
			Value: sequenceNumber,
		}
	}

	if !final {
		item["ClientID"] = &types.AttributeValueMemberS{
			Value: k.clientID,
		}
		item["LeaseTimeout"] = &types.AttributeValueMemberS{
			Value: time.Now().Add(k.leaseDuration).Format(time.RFC3339Nano),
		}
	}

	if _, err := k.svc.PutItem(ctx, &dynamodb.PutItemInput{
		ConditionExpression: aws.String("ClientID = :client_id"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":client_id": &types.AttributeValueMemberS{
				Value: k.clientID,
			},
		},
		TableName: aws.String(k.conf.Table),
		Item:      item,
	}); err != nil {
		var aerr *types.ConditionalCheckFailedException
		if errors.As(err, &aerr) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Yield updates an existing checkpoint sequence number and no other fields.
// This should be done after a non-final checkpoint indicates that shard has
// been stolen and allows the thief client to start with the latest sequence
// rather than the sequence at the point of the theft.
//
// This call is entirely optional, but the benefit is a reduction in duplicated
// messages during a rebalance of shards.
func (k *awsKinesisCheckpointer) Yield(ctx context.Context, streamID, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		// Nothing to present to the thief
		return nil
	}

	_, err := k.svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]types.AttributeValue{
			"StreamID": &types.AttributeValueMemberS{
				Value: streamID,
			},
			"ShardID": &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":new_sequence_number": &types.AttributeValueMemberS{
				Value: sequenceNumber,
			},
		},
		UpdateExpression: aws.String("SET SequenceNumber = :new_sequence_number"),
	})
	return err
}

// Delete attempts to delete a checkpoint, this should be called when a shard is
// emptied.
func (k *awsKinesisCheckpointer) Delete(ctx context.Context, streamID, shardID string) error {
	_, err := k.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]types.AttributeValue{
			"StreamID": &types.AttributeValueMemberS{
				Value: streamID,
			},
			"ShardID": &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})
	return err
}
