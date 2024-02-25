package aws

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/public/service"
)

func dynCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Second
	retriesDefaults.MaxInterval = time.Second * 5
	retriesDefaults.MaxElapsedTime = time.Second * 30

	spec := service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Summary(`Stores key/value pairs as a single document in a DynamoDB table. The key is stored as a string value and used as the table hash key. The value is stored as
a binary value using the ` + "`data_key`" + ` field name.`).
		Description(`A prefix can be specified to allow multiple cache types to share a single DynamoDB table. An optional TTL duration (` + "`ttl`" + `) and field
(` + "`ttl_key`" + `) can be specified if the backing table has TTL enabled.

Strong read consistency can be enabled using the ` + "`consistent_read`" + ` configuration field.`).
		Field(service.NewStringField("table").
			Description("The table to store items in.")).
		Field(service.NewStringField("hash_key").
			Description("The key of the table column to store item keys within.")).
		Field(service.NewStringField("data_key").
			Description("The key of the table column to store item values within.")).
		Field(service.NewBoolField("consistent_read").
			Description("Whether to use strongly consistent reads on Get commands.").
			Advanced().
			Default(false)).
		Field(service.NewDurationField("default_ttl").
			Description("An optional default TTL to set for items, calculated from the moment the item is cached. A `ttl_key` must be specified in order to set item TTLs.").
			Optional().
			Advanced()).
		Field(service.NewStringField("ttl_key").
			Description("The column key to place the TTL value within.").
			Optional().
			Advanced()).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced())

	for _, f := range config.SessionFields() {
		spec = spec.Field(f)
	}
	return spec
}

func init() {
	err := service.RegisterCache(
		"aws_dynamodb", dynCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			d, err := newDynamodbCacheFromConfig(conf)
			if err != nil {
				return nil, err
			}
			if err := d.verify(context.Background()); err != nil {
				return nil, err
			}
			return d, nil
		})
	if err != nil {
		panic(err)
	}
}

func newDynamodbCacheFromConfig(conf *service.ParsedConfig) (*dynamodbCache, error) {
	table, err := conf.FieldString("table")
	if err != nil {
		return nil, err
	}
	hashKey, err := conf.FieldString("hash_key")
	if err != nil {
		return nil, err
	}
	dataKey, err := conf.FieldString("data_key")
	if err != nil {
		return nil, err
	}
	consistentRead, err := conf.FieldBool("consistent_read")
	if err != nil {
		return nil, err
	}
	var ttl *time.Duration
	if conf.Contains("default_ttl") {
		ttlTmp, err := conf.FieldDuration("default_ttl")
		if err != nil {
			return nil, err
		}
		ttl = &ttlTmp
	}
	var ttlKey *string
	if conf.Contains("ttl_key") {
		ttlKeyTmp, err := conf.FieldString("ttl_key")
		if err != nil {
			return nil, err
		}
		ttlKey = &ttlKeyTmp
	}
	sess, err := GetSession(context.Background(), conf)
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(sess)

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}
	return newDynamodbCache(client, table, hashKey, dataKey, consistentRead, ttlKey, ttl, backOff), nil
}

//------------------------------------------------------------------------------

type dynamoDBAPIV2 interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type dynamodbCache struct {
	client dynamoDBAPIV2

	table          string
	hashKey        string
	dataKey        string
	consistentRead bool
	ttlKey         *string
	ttl            *time.Duration

	boffPool sync.Pool
}

func newDynamodbCache(
	client dynamoDBAPIV2,
	table, hashKey, dataKey string,
	consistentRead bool,
	ttlKey *string, ttl *time.Duration,
	backOff *backoff.ExponentialBackOff,
) *dynamodbCache {
	return &dynamodbCache{
		client:         client,
		table:          table,
		hashKey:        hashKey,
		dataKey:        dataKey,
		consistentRead: consistentRead,
		ttlKey:         ttlKey,
		ttl:            ttl,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}
}

func (d *dynamodbCache) verify(ctx context.Context) error {
	out, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &d.table,
	})
	if err != nil {
		return err
	}
	if out == nil ||
		out.Table == nil ||
		out.Table.TableStatus != types.TableStatusActive {
		return fmt.Errorf("table '%s' must be active", d.table)
	}
	return nil
}

//------------------------------------------------------------------------------

func (d *dynamodbCache) Get(ctx context.Context, key string) ([]byte, error) {
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	result, err := d.get(ctx, key)
	for err != nil && err != service.ErrKeyNotFound {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, err
		}
		result, err = d.get(ctx, key)
	}

	return result, err
}

func (d *dynamodbCache) get(ctx context.Context, key string) ([]byte, error) {
	res, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		Key: map[string]types.AttributeValue{
			d.hashKey: &types.AttributeValueMemberS{
				Value: key,
			},
		},
		TableName:      &d.table,
		ConsistentRead: aws.Bool(d.consistentRead),
	})
	if err != nil {
		return nil, err
	}

	val, ok := res.Item[d.dataKey].(*types.AttributeValueMemberB)
	if !ok {
		return nil, service.ErrKeyNotFound
	}
	return val.Value, nil
}

func (d *dynamodbCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	_, err := d.client.PutItem(ctx, d.putItemInput(key, value, ttl))
	for err != nil {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		_, err = d.client.PutItem(ctx, d.putItemInput(key, value, ttl))
	}

	return err
}

func (d *dynamodbCache) SetMulti(ctx context.Context, items ...service.CacheItem) error {
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	writeReqs := []types.WriteRequest{}
	for _, kv := range items {
		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: d.putItemInput(kv.Key, kv.Value, kv.TTL).Item,
			},
		})
	}

	var err error
	for len(writeReqs) > 0 {
		wait := boff.NextBackOff()
		var batchResult *dynamodb.BatchWriteItemOutput
		batchResult, err = d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				d.table: writeReqs,
			},
		})
		if err == nil {
			if unproc := batchResult.UnprocessedItems[d.table]; len(unproc) > 0 {
				writeReqs = unproc
				err = fmt.Errorf("failed to set %v items", len(unproc))
			} else {
				writeReqs = nil
			}
		}
		if err != nil {
			if wait == backoff.Stop {
				break
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return err
			}
		}
	}

	return err
}

func (d *dynamodbCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	err := d.add(ctx, key, value, ttl)
	for err != nil && err != service.ErrKeyAlreadyExists {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		err = d.add(ctx, key, value, ttl)
	}

	return err
}

func (d *dynamodbCache) add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	input := d.putItemInput(key, value, ttl)

	expr, err := expression.NewBuilder().
		WithCondition(expression.AttributeNotExists(expression.Name(d.hashKey))).
		Build()
	if err != nil {
		return err
	}
	input.ExpressionAttributeNames = expr.Names()
	input.ConditionExpression = expr.Condition()

	if _, err = d.client.PutItem(ctx, input); err != nil {
		var derr *types.ConditionalCheckFailedException
		if errors.As(err, &derr) {
			return service.ErrKeyAlreadyExists
		}
		return err
	}
	return nil
}

func (d *dynamodbCache) Delete(ctx context.Context, key string) error {
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	err := d.delete(ctx, key)
	for err != nil {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		err = d.delete(ctx, key)
	}
	return err
}

func (d *dynamodbCache) delete(ctx context.Context, key string) error {
	_, err := d.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			d.hashKey: &types.AttributeValueMemberS{
				Value: key,
			},
		},
		TableName: &d.table,
	})
	return err
}

func (d *dynamodbCache) putItemInput(key string, value []byte, ttl *time.Duration) *dynamodb.PutItemInput {
	input := dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			d.hashKey: &types.AttributeValueMemberS{
				Value: key,
			},
			d.dataKey: &types.AttributeValueMemberB{
				Value: value,
			},
		},
		TableName: &d.table,
	}

	if ttl == nil {
		ttl = d.ttl
	}
	if ttl != nil && d.ttlKey != nil {
		input.Item[*d.ttlKey] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(*ttl).Unix(), 10),
		}
	}

	return &input
}

func (d *dynamodbCache) Close(context.Context) error {
	return nil
}
