package aws

// Inspired by Patrick Robinson https://github.com/patrobinson/gokini

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/benthosdev/benthos/v4/internal/component/input"
)

// Common errors that might occur throughout checkpointing.
var (
	ErrLeaseNotAcquired = errors.New("the shard could not be leased due to a collision")
)

// awsKinesisCheckpointer manages the shard checkpointing for a given client
// identifier.
type awsKinesisCheckpointer struct {
	conf input.DynamoDBCheckpointConfig

	clientID      string
	leaseDuration time.Duration
	commitPeriod  time.Duration
	svc           dynamodbiface.DynamoDBAPI
}

// newAWSKinesisCheckpointer creates a new DynamoDB checkpointer from an AWS
// session and a configuration struct.
func newAWSKinesisCheckpointer(
	session *session.Session,
	clientID string,
	conf input.DynamoDBCheckpointConfig,
	leaseDuration time.Duration,
	commitPeriod time.Duration,
) (*awsKinesisCheckpointer, error) {
	c := &awsKinesisCheckpointer{
		conf:          conf,
		leaseDuration: leaseDuration,
		commitPeriod:  commitPeriod,
		svc:           dynamodb.New(session),
		clientID:      clientID,
	}

	if err := c.ensureTableExists(); err != nil {
		return nil, err
	}
	return c, nil
}

//------------------------------------------------------------------------------

func (k *awsKinesisCheckpointer) ensureTableExists() error {
	_, err := k.svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(k.conf.Table),
	})
	if err == nil {
		return nil
	}
	if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
		return err
	}
	if !k.conf.Create {
		return fmt.Errorf("target table %v does not exist", k.conf.Table)
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("StreamID"), AttributeType: aws.String("S")},
			{AttributeName: aws.String("ShardID"), AttributeType: aws.String("S")},
		},
		BillingMode: aws.String(k.conf.BillingMode),
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("StreamID"), KeyType: aws.String("HASH")},
			{AttributeName: aws.String("ShardID"), KeyType: aws.String("RANGE")},
		},
		TableName: aws.String(k.conf.Table),
	}
	if k.conf.BillingMode == "PROVISIONED" {
		input.ProvisionedThroughput = &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(k.conf.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(k.conf.WriteCapacityUnits),
		}
	}
	if _, err = k.svc.CreateTable(input); err != nil {
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
	rawItem, err := k.svc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: aws.String(shardID),
			},
			"StreamID": {
				S: aws.String(streamID),
			},
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				return nil, nil
			}
		}
		return nil, err
	}

	c := awsKinesisCheckpoint{}

	if s, ok := rawItem.Item["SequenceNumber"]; ok && s.S != nil {
		c.SequenceNumber = *s.S
	} else {
		return nil, errors.New("sequence ID was not found in checkpoint")
	}

	if s, ok := rawItem.Item["ClientID"]; ok && s.S != nil {
		c.ClientID = s.S
	}

	if s, ok := rawItem.Item["LeaseTimeout"]; ok && s.S != nil {
		timeout, err := time.Parse(time.RFC3339Nano, *s.S)
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

	if err := k.svc.ScanPagesWithContext(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(k.conf.Table),
		FilterExpression: aws.String("StreamID = :stream_id"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":stream_id": {
				S: &streamID,
			},
		},
	}, func(page *dynamodb.ScanOutput, last bool) bool {
		for _, i := range page.Items {
			var clientID string
			if s, ok := i["ClientID"]; ok && s.S != nil {
				clientID = *s.S
			} else {
				continue
			}

			var claim awsKinesisClientClaim
			if s, ok := i["ShardID"]; ok && s.S != nil {
				claim.ShardID = *s.S
			}
			if claim.ShardID == "" {
				scanErr = errors.New("failed to extract shard id from claim")
				return false
			}

			if s, ok := i["LeaseTimeout"]; ok && s.S != nil {
				if claim.LeaseTimeout, scanErr = time.Parse(time.RFC3339Nano, *s.S); scanErr != nil {
					scanErr = fmt.Errorf("failed to parse claim lease: %w", scanErr)
					return false
				}
			}
			if claim.LeaseTimeout.IsZero() {
				scanErr = errors.New("failed to extract lease timeout from claim")
				return false
			}

			clientClaims[clientID] = append(clientClaims[clientID], claim)
		}

		return true
	}); err != nil {
		return nil, err
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
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":new_client_id": {
			S: &k.clientID,
		},
		":new_lease_timeout": {
			S: &newLeaseTimeoutString,
		},
	}

	if len(fromClientID) > 0 {
		conditionalExpression = "ClientID = :old_client_id"
		expressionAttributeValues[":old_client_id"] = &dynamodb.AttributeValue{
			S: &fromClientID,
		}
	} else {
		conditionalExpression = "attribute_not_exists(ClientID)"
	}

	res, err := k.svc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		ReturnValues:              aws.String("ALL_OLD"),
		TableName:                 aws.String(k.conf.Table),
		ConditionExpression:       aws.String(conditionalExpression),
		UpdateExpression:          aws.String("SET ClientID = :new_client_id, LeaseTimeout = :new_lease_timeout"),
		ExpressionAttributeValues: expressionAttributeValues,
		Key: map[string]*dynamodb.AttributeValue{
			"StreamID": {
				S: &streamID,
			},
			"ShardID": {
				S: &shardID,
			},
		},
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return "", ErrLeaseNotAcquired
			}
		}
		return "", err
	}

	var startingSequence string
	if s, ok := res.Attributes["SequenceNumber"]; ok && s.S != nil {
		startingSequence = *s.S
	}

	var currentLease time.Time
	if s, ok := res.Attributes["LeaseTimeout"]; ok && s.S != nil {
		currentLease, _ = time.Parse(time.RFC3339Nano, *s.S)
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
	item := map[string]*dynamodb.AttributeValue{
		"StreamID": {
			S: &streamID,
		},
		"ShardID": {
			S: &shardID,
		},
	}

	if len(sequenceNumber) > 0 {
		item["SequenceNumber"] = &dynamodb.AttributeValue{
			S: &sequenceNumber,
		}
	}

	if !final {
		item["ClientID"] = &dynamodb.AttributeValue{
			S: &k.clientID,
		}
		item["LeaseTimeout"] = &dynamodb.AttributeValue{
			S: aws.String(time.Now().Add(k.leaseDuration).Format(time.RFC3339Nano)),
		}
	}

	if _, err := k.svc.PutItem(&dynamodb.PutItemInput{
		ConditionExpression: aws.String("ClientID = :client_id"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":client_id": {
				S: &k.clientID,
			},
		},
		TableName: aws.String(k.conf.Table),
		Item:      item,
	}); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, nil
			}
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

	_, err := k.svc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]*dynamodb.AttributeValue{
			"StreamID": {
				S: &streamID,
			},
			"ShardID": {
				S: &shardID,
			},
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":new_sequence_number": {
				S: &sequenceNumber,
			},
		},
		UpdateExpression: aws.String("SET SequenceNumber = :new_sequence_number"),
	})
	return err
}

// Delete attempts to delete a checkpoint, this should be called when a shard is
// emptied.
func (k *awsKinesisCheckpointer) Delete(ctx context.Context, streamID, shardID string) error {
	_, err := k.svc.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(k.conf.Table),
		Key: map[string]*dynamodb.AttributeValue{
			"StreamID": {
				S: &streamID,
			},
			"ShardID": {
				S: &shardID,
			},
		},
	})
	return err
}
