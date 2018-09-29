// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cache

import (
	"fmt"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDynamoDB] = TypeSpec{
		constructor: NewDynamoDB,
		description: `
The dynamodb cache stores key/value pairs as a single document in a DynamoDB
table. The key is stored as a string value and used as table hash key. The value
is stored as a binary value using the ` + "`data_key`" + ` field name. A prefix
can be specified to allow multiple cache types to share a single DynamoDB table.
An optional TTL duration (` + "`ttl`" + `) and field (` + "`ttl_key`" + `) can
be specified if the backing table has TTL enabled. Strong read consistency can
be enabled using the ` + "`consistent_read`" + ` configuration field.`,
	}
}

//------------------------------------------------------------------------------

// DynamoDBConfig contains config fields for the DynamoDB cache type.
type DynamoDBConfig struct {
	session.Config `json:",inline" yaml:",inline"`
	ConsistentRead bool   `json:"consistent_read" yaml:"consistent_read"`
	DataKey        string `json:"data_key" yaml:"data_key"`
	HashKey        string `json:"hash_key" yaml:"hash_key"`
	Table          string `json:"table" yaml:"table"`
	TTL            string `json:"ttl" yaml:"ttl"`
	TTLKey         string `json:"ttl_key" yaml:"ttl_key"`
}

// NewDynamoDBConfig creates a MemoryConfig populated with default values.
func NewDynamoDBConfig() DynamoDBConfig {
	return DynamoDBConfig{
		Config:         session.NewConfig(),
		ConsistentRead: false,
		DataKey:        "",
		HashKey:        "",
		Table:          "",
		TTL:            "",
		TTLKey:         "",
	}
}

//------------------------------------------------------------------------------

// DynamoDB is a DynamoDB based cache implementation.
type DynamoDB struct {
	client dynamodbiface.DynamoDBAPI
	conf   DynamoDBConfig
	log    log.Modular
	stats  metrics.Type
	table  *string
	ttl    time.Duration
}

// NewDynamoDB creates a new DynamoDB cache type.
func NewDynamoDB(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	d := DynamoDB{
		conf:  conf.DynamoDB,
		log:   log,
		stats: stats,
		table: aws.String(conf.DynamoDB.Table),
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
	} else if out == nil || out.Table == nil || out.Table.TableStatus == nil || *out.Table.TableStatus != dynamodb.TableStatusActive {
		return nil, fmt.Errorf("dynamodb table '%s' must be active", d.conf.Table)
	}

	return &d, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (d *DynamoDB) Get(key string) ([]byte, error) {
	res, err := d.client.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			d.conf.HashKey: {
				S: aws.String(key),
			},
		},
		TableName: d.table,
	})
	if err != nil {
		return nil, err
	}

	val, ok := res.Item[d.conf.DataKey]
	if !ok || val.B == nil {
		d.log.Warnf("cache key not found: %s", key)
		return nil, types.ErrCacheNotFound
	}
	return val.B, nil
}

// Set attempts to set the value of a key.
func (d *DynamoDB) Set(key string, value []byte) error {
	_, err := d.client.PutItem(d.putItemInput(key, value))
	if err != nil {
		return err
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (d *DynamoDB) Add(key string, value []byte) error {
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
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return types.ErrKeyAlreadyExists
			}
		}
		return err
	}
	return nil
}

// Delete attempts to remove a key.
func (d *DynamoDB) Delete(key string) error {
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
			S: aws.String(time.Now().Add(d.ttl).Format(time.RFC3339Nano)),
		}
	}

	return &input
}

//------------------------------------------------------------------------------
