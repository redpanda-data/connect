package cache

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSDynamoDB] = TypeSpec{
		constructor: NewAWSDynamoDB,
		Version:     "3.36.0",
		Summary: `
Stores key/value pairs as a single document in a DynamoDB table. The key is
stored as a string value and used as the table hash key. The value is stored as
a binary value using the ` + "`data_key`" + ` field name.`,
		Description: `
A prefix can be specified to allow multiple cache types to share a single
DynamoDB table. An optional TTL duration (` + "`ttl`" + `) and field
(` + "`ttl_key`" + `) can be specified if the backing table has TTL enabled.

Strong read consistency can be enabled using the ` + "`consistent_read`" + `
configuration field.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("table", "The table to store items in."),
			docs.FieldCommon("hash_key", "The key of the table column to store item keys within."),
			docs.FieldCommon("data_key", "The key of the table column to store item values within."),
			docs.FieldAdvanced("consistent_read", "Whether to use strongly consistent reads on Get commands."),
			docs.FieldAdvanced("ttl", "An optional TTL to set for items, calculated from the moment the item is cached."),
			docs.FieldAdvanced("ttl_key", "The column key to place the TTL value within."),
		}.Merge(session.FieldSpecs()).Merge(retries.FieldSpecs()),
	}
}

//------------------------------------------------------------------------------

type sessionConfig struct {
	session.Config `json:",inline" yaml:",inline"`
}

// DynamoDBConfig contains config fields for the DynamoDB cache type.
type DynamoDBConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	ConsistentRead bool   `json:"consistent_read" yaml:"consistent_read"`
	DataKey        string `json:"data_key" yaml:"data_key"`
	HashKey        string `json:"hash_key" yaml:"hash_key"`
	Table          string `json:"table" yaml:"table"`
	TTL            string `json:"ttl" yaml:"ttl"`
	TTLKey         string `json:"ttl_key" yaml:"ttl_key"`
	retries.Config `json:",inline" yaml:",inline"`
}

// NewDynamoDBConfig creates a MemoryConfig populated with default values.
func NewDynamoDBConfig() DynamoDBConfig {
	rConf := retries.NewConfig()
	rConf.MaxRetries = 3
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return DynamoDBConfig{
		sessionConfig: sessionConfig{
			Config: session.NewConfig(),
		},
		ConsistentRead: false,
		DataKey:        "",
		HashKey:        "",
		Table:          "",
		TTL:            "",
		TTLKey:         "",
		Config:         rConf,
	}
}

//------------------------------------------------------------------------------

// DynamoDB is a DynamoDB based cache implementation.
type DynamoDB struct {
	client      dynamodbiface.DynamoDBAPI
	conf        DynamoDBConfig
	log         log.Modular
	stats       metrics.Type
	table       *string
	ttl         time.Duration
	backoffCtor func() backoff.BackOff
	boffPool    sync.Pool

	mLatency         metrics.StatTimer
	mGetCount        metrics.StatCounter
	mGetRetry        metrics.StatCounter
	mGetFailed       metrics.StatCounter
	mGetSuccess      metrics.StatCounter
	mGetLatency      metrics.StatTimer
	mGetNotFound     metrics.StatCounter
	mSetCount        metrics.StatCounter
	mSetRetry        metrics.StatCounter
	mSetFailed       metrics.StatCounter
	mSetSuccess      metrics.StatCounter
	mSetLatency      metrics.StatTimer
	mSetMultiCount   metrics.StatCounter
	mSetMultiRetry   metrics.StatCounter
	mSetMultiFailed  metrics.StatCounter
	mSetMultiSuccess metrics.StatCounter
	mSetMultiLatency metrics.StatTimer
	mAddCount        metrics.StatCounter
	mAddDupe         metrics.StatCounter
	mAddRetry        metrics.StatCounter
	mAddFailedDupe   metrics.StatCounter
	mAddFailedErr    metrics.StatCounter
	mAddSuccess      metrics.StatCounter
	mAddLatency      metrics.StatTimer
	mDelCount        metrics.StatCounter
	mDelRetry        metrics.StatCounter
	mDelFailedErr    metrics.StatCounter
	mDelSuccess      metrics.StatCounter
	mDelLatency      metrics.StatTimer
}

// NewAWSDynamoDB creates a new DynamoDB cache type.
func NewAWSDynamoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return newDynamoDB(conf.AWSDynamoDB, mgr, log, stats)
}

// NewDynamoDB creates a new DynamoDB cache type.
func NewDynamoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return newDynamoDB(conf.DynamoDB, mgr, log, stats)
}

func newDynamoDB(conf DynamoDBConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	d := DynamoDB{
		conf:  conf,
		log:   log,
		stats: stats,
		table: aws.String(conf.Table),

		mLatency:         stats.GetTimer("latency"),
		mGetCount:        stats.GetCounter("get.count"),
		mGetRetry:        stats.GetCounter("get.retry"),
		mGetFailed:       stats.GetCounter("get.failed.error"),
		mGetNotFound:     stats.GetCounter("get.failed.not_found"),
		mGetSuccess:      stats.GetCounter("get.success"),
		mGetLatency:      stats.GetTimer("get.latency"),
		mSetCount:        stats.GetCounter("set.count"),
		mSetRetry:        stats.GetCounter("set.retry"),
		mSetFailed:       stats.GetCounter("set.failed.error"),
		mSetSuccess:      stats.GetCounter("set.success"),
		mSetLatency:      stats.GetTimer("set.latency"),
		mSetMultiCount:   stats.GetCounter("set_multi.count"),
		mSetMultiRetry:   stats.GetCounter("set_multi.retry"),
		mSetMultiFailed:  stats.GetCounter("set_multi.failed.error"),
		mSetMultiSuccess: stats.GetCounter("set_multi.success"),
		mSetMultiLatency: stats.GetTimer("set_multi.latency"),
		mAddCount:        stats.GetCounter("add.count"),
		mAddDupe:         stats.GetCounter("add.failed.duplicate"),
		mAddRetry:        stats.GetCounter("add.retry"),
		mAddFailedDupe:   stats.GetCounter("add.failed.duplicate"),
		mAddFailedErr:    stats.GetCounter("add.failed.error"),
		mAddSuccess:      stats.GetCounter("add.success"),
		mAddLatency:      stats.GetTimer("add.latency"),
		mDelCount:        stats.GetCounter("delete.count"),
		mDelRetry:        stats.GetCounter("delete.retry"),
		mDelFailedErr:    stats.GetCounter("delete.failed.error"),
		mDelSuccess:      stats.GetCounter("delete.success"),
		mDelLatency:      stats.GetTimer("delete.latency"),
	}

	if d.conf.TTL != "" {
		ttl, err := time.ParseDuration(d.conf.TTL)
		if err != nil {
			return nil, err
		}
		d.ttl = ttl
	}

	sess, err := d.conf.GetSession()
	if err != nil {
		return nil, err
	}

	d.client = dynamodb.New(sess)
	out, err := d.client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: d.table,
	})
	if err != nil {
		return nil, err
	} else if out == nil ||
		out.Table == nil ||
		out.Table.TableStatus == nil ||
		*out.Table.TableStatus != dynamodb.TableStatusActive {
		return nil, fmt.Errorf("table '%s' must be active", d.conf.Table)
	}

	if d.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	d.boffPool = sync.Pool{
		New: func() interface{} {
			return d.backoffCtor()
		},
	}

	return &d, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (d *DynamoDB) Get(key string) ([]byte, error) {
	d.mGetCount.Incr(1)

	tStarted := time.Now()
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	result, err := d.get(key)
	for err != nil && err != types.ErrKeyNotFound {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		time.Sleep(wait)
		d.mGetRetry.Incr(1)
		result, err = d.get(key)
	}
	if err == nil {
		d.mGetSuccess.Incr(1)
	} else if err == types.ErrKeyNotFound {
		d.mGetNotFound.Incr(1)
	} else {
		d.mGetFailed.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	d.mGetLatency.Timing(latency)
	d.mLatency.Timing(latency)

	return result, err
}

func (d *DynamoDB) get(key string) ([]byte, error) {
	res, err := d.client.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			d.conf.HashKey: {
				S: aws.String(key),
			},
		},
		TableName:      d.table,
		ConsistentRead: aws.Bool(d.conf.ConsistentRead),
	})
	if err != nil {
		return nil, err
	}

	val, ok := res.Item[d.conf.DataKey]
	if !ok || val.B == nil {
		d.log.Debugf("key not found: %s", key)
		return nil, types.ErrKeyNotFound
	}
	return val.B, nil
}

// Set attempts to set the value of a key.
func (d *DynamoDB) Set(key string, value []byte) error {
	d.mSetCount.Incr(1)

	tStarted := time.Now()
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	_, err := d.client.PutItem(d.putItemInput(key, value))
	for err != nil {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		time.Sleep(wait)
		d.mSetRetry.Incr(1)
		_, err = d.client.PutItem(d.putItemInput(key, value))
	}
	if err == nil {
		d.mSetSuccess.Incr(1)
	} else {
		d.mSetFailed.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	d.mSetLatency.Timing(latency)
	d.mLatency.Timing(latency)

	return err
}

// SetMulti attempts to set the value of multiple keys, if any keys fail to be
// set an error is returned.
func (d *DynamoDB) SetMulti(items map[string][]byte) error {
	d.mSetMultiCount.Incr(1)

	tStarted := time.Now()
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	writeReqs := []*dynamodb.WriteRequest{}
	for k, v := range items {
		writeReqs = append(writeReqs, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: d.putItemInput(k, v).Item,
			},
		})
	}

	var err error
	for len(writeReqs) > 0 {
		wait := boff.NextBackOff()
		var batchResult *dynamodb.BatchWriteItemOutput
		batchResult, err = d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				*d.table: writeReqs,
			},
		})
		if err != nil {
			d.log.Errorf("Write multi error: %v\n", err)
		} else if unproc := batchResult.UnprocessedItems[*d.table]; len(unproc) > 0 {
			writeReqs = unproc
			err = fmt.Errorf("failed to set %v items", len(unproc))
		} else {
			writeReqs = nil
		}

		if err != nil {
			if wait == backoff.Stop {
				break
			}
			time.After(wait)
			d.mSetMultiRetry.Incr(1)
		}
	}

	if err == nil {
		d.mSetMultiSuccess.Incr(1)
	} else {
		d.mSetMultiFailed.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	d.mSetMultiLatency.Timing(latency)
	d.mLatency.Timing(latency)

	return err
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (d *DynamoDB) Add(key string, value []byte) error {
	d.mAddCount.Incr(1)

	tStarted := time.Now()
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	err := d.add(key, value)
	for err != nil && err != types.ErrKeyAlreadyExists {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		time.Sleep(wait)
		d.mAddRetry.Incr(1)
		err = d.add(key, value)
	}
	if err == nil {
		d.mAddSuccess.Incr(1)
	} else if err == types.ErrKeyAlreadyExists {
		d.mAddFailedDupe.Incr(1)
	} else {
		d.mAddFailedErr.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	d.mAddLatency.Timing(latency)
	d.mLatency.Timing(latency)

	return err
}

func (d *DynamoDB) add(key string, value []byte) error {
	input := d.putItemInput(key, value)

	expr, err := expression.NewBuilder().
		WithCondition(expression.AttributeNotExists(expression.Name(d.conf.HashKey))).
		Build()
	if err != nil {
		return err
	}
	input.ExpressionAttributeNames = expr.Names()
	input.ConditionExpression = expr.Condition()

	if _, err = d.client.PutItem(input); err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return types.ErrKeyAlreadyExists
			}
		}
		return err
	}
	return nil
}

// Delete attempts to remove a key.
func (d *DynamoDB) Delete(key string) error {
	d.mDelCount.Incr(1)

	tStarted := time.Now()
	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	err := d.delete(key)
	for err != nil {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break
		}
		time.Sleep(wait)
		d.mDelRetry.Incr(1)
		err = d.delete(key)
	}
	if err == nil {
		d.mDelSuccess.Incr(1)
	} else {
		d.mDelFailedErr.Incr(1)
	}

	latency := int64(time.Since(tStarted))
	d.mDelLatency.Timing(latency)
	d.mLatency.Timing(latency)

	return err
}

func (d *DynamoDB) delete(key string) error {
	_, err := d.client.DeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			d.conf.HashKey: {
				S: aws.String(key),
			},
		},
		TableName: d.table,
	})
	return err
}

// putItemInput creates a generic put item input for use in Set and Add operations
func (d *DynamoDB) putItemInput(key string, value []byte) *dynamodb.PutItemInput {
	input := dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			d.conf.HashKey: {
				S: aws.String(key),
			},
			d.conf.DataKey: {
				B: value,
			},
		},
		TableName: d.table,
	}

	if d.ttl != 0 && d.conf.TTLKey != "" {
		input.Item[d.conf.TTLKey] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10)),
		}
	}

	return &input
}

// CloseAsync shuts down the cache.
func (d *DynamoDB) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (d *DynamoDB) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
