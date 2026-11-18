// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"

	baws "github.com/redpanda-data/connect/v4/internal/impl/aws"
	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	// DynamoDB Output Fields
	ddboField               = "namespace"
	ddboFieldTable          = "table"
	ddboFieldStringColumns  = "string_columns"
	ddboFieldJSONMapColumns = "json_map_columns"
	ddboFieldTTL            = "ttl"
	ddboFieldTTLKey         = "ttl_key"
	ddboFieldBatching       = "batching"
)

// ttlKeyDescription describes the ttl_key field of the aws_dynamodb output and
// cache.
const ttlKeyDescription = "The column key to place the TTL value within. The value is the expiry time as a Unix timestamp in seconds, stored as a number attribute, which is the format that DynamoDB TTL requires. A TTL value is only written when both this field and a TTL duration are set."

type ddboConfig struct {
	Table          string
	StringColumns  map[string]*service.InterpolatedString
	JSONMapColumns map[string]string
	TTL            string
	TTLKey         string

	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func ddboConfigFromParsed(pConf *service.ParsedConfig) (conf ddboConfig, err error) {
	if conf.Table, err = pConf.FieldString(ddboFieldTable); err != nil {
		return
	}
	if conf.StringColumns, err = pConf.FieldInterpolatedStringMap(ddboFieldStringColumns); err != nil {
		return
	}
	if conf.JSONMapColumns, err = pConf.FieldStringMap(ddboFieldJSONMapColumns); err != nil {
		return
	}
	if conf.TTL, err = pConf.FieldString(ddboFieldTTL); err != nil {
		return
	}
	if conf.TTLKey, err = pConf.FieldString(ddboFieldTTLKey); err != nil {
		return
	}
	if conf.aconf, err = baws.GetSession(context.TODO(), pConf); err != nil {
		return
	}
	if conf.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
		return
	}
	return
}

func ddboOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Inserts items into a DynamoDB table.`).
		Description(`
The field `+"`string_columns`"+` is a map of column names to string values, where the values are xref:configuration:interpolation.adoc#bloblang-queries[function interpolated] per message of a batch. This allows you to populate string columns of an item by extracting fields within the document payload or metadata like follows:

`+"```yml"+`
string_columns:
  id: ${!json("id")}
  title: ${!json("body.title")}
  topic: ${!meta("kafka_topic")}
  full_content: ${!content()}
`+"```"+`

The field `+"`json_map_columns`"+` is a map of column names to json paths, where the xref:configuration:field_paths.adoc[dot path] is extracted from each document and converted into a map value. Both an empty path and the path `+"`.`"+` are interpreted as the root of the document. This allows you to populate map columns of an item like follows:

`+"```yml"+`
json_map_columns:
  user: path.to.user
  whole_document: .
`+"```"+`

A column name can be empty:

`+"```yml"+`
json_map_columns:
  "": .
`+"```"+`

In which case the top level document fields will be written at the root of the item, potentially overwriting previously defined column values. If a path is not found within a document the column will not be populated.

[[credentials-overview]]
== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[AWS credentials].

== Performance

This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field `+"`max_in_flight`"+`.

This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more xref:configuration:batching.adoc[in this doc].
`).
		Fields(
			service.NewStringField(ddboFieldTable).
				Description("The table to store messages in."),
			service.NewInterpolatedStringMapField(ddboFieldStringColumns).
				Description("A map of column keys to string values to store.").
				Default(map[string]any{}).
				Example(map[string]any{
					"id":           "${!json(\"id\")}",
					"title":        "${!json(\"body.title\")}",
					"topic":        "${!meta(\"kafka_topic\")}",
					"full_content": "${!content()}",
				}),
			service.NewStringMapField(ddboFieldJSONMapColumns).
				Description("A map of column keys to xref:configuration:field_paths.adoc[field paths] pointing to value data within messages.").
				ShortDescription("A map of column keys to field paths pointing at value data within messages.").
				Default(map[string]any{}).
				Example(map[string]any{
					"user":           "path.to.user",
					"whole_document": ".",
				}).
				Example(map[string]string{
					"": ".",
				}),
			service.NewStringField(ddboFieldTTL).
				Description("An optional TTL to set for items, calculated from the moment the message is sent.").
				Default("").
				Advanced(),
			service.NewStringField(ddboFieldTTLKey).
				Description(ttlKeyDescription).
				Default("").
				Advanced(),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(ddboFieldBatching),
		).
		Fields(config.SessionFields()...).
		Fields(service.NewRetryBackOffFields(3, &backoff.ExponentialBackOff{
			InitialInterval: time.Second,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  30 * time.Second,
		})...)
}

func init() {
	service.MustRegisterBatchOutput("aws_dynamodb", ddboOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(ddboFieldBatching); err != nil {
				return
			}
			var wConf ddboConfig
			if wConf, err = ddboConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newDynamoDBWriter(wConf, mgr)
			return
		})
}

type dynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
	ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type dynamoDBWriter struct {
	client dynamoDBAPI
	conf   ddboConfig
	log    *service.Logger

	boffPool sync.Pool

	table *string
	ttl   time.Duration
}

func newDynamoDBWriter(conf ddboConfig, mgr *service.Resources) (*dynamoDBWriter, error) {
	db := &dynamoDBWriter{
		conf:  conf,
		log:   mgr.Logger(),
		table: aws.String(conf.Table),
	}
	if len(conf.StringColumns) == 0 && len(conf.JSONMapColumns) == 0 {
		return nil, errors.New("you must provide at least one column")
	}
	for k, v := range conf.JSONMapColumns {
		if v == "." {
			conf.JSONMapColumns[k] = ""
		}
	}
	if conf.TTL != "" {
		ttl, err := time.ParseDuration(conf.TTL)
		if err != nil {
			return nil, fmt.Errorf("parsing TTL: %v", err)
		}
		db.ttl = ttl
	}
	db.boffPool = sync.Pool{
		New: func() any {
			return db.conf.backoffCtor()
		},
	}
	return db, nil
}

// ConnectionTest attempts to test the connection configuration of this output
// without actually sending data. The connection, if successful, is then
// closed.
func (d *dynamoDBWriter) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	client := dynamodb.NewFromConfig(d.conf.aconf)
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: d.table,
	})
	if err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("describing table %s: %w", *d.table, err)).AsList()
	}
	return service.ConnectionTestSucceeded().AsList()
}

func (d *dynamoDBWriter) Connect(ctx context.Context) error {
	if d.client != nil {
		return nil
	}

	client := dynamodb.NewFromConfig(d.conf.aconf)
	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: d.table,
	})
	if err != nil {
		return err
	} else if out == nil || out.Table == nil || out.Table.TableStatus != types.TableStatusActive {
		return fmt.Errorf("dynamodb table '%s' must be active", d.conf.Table)
	}

	d.client = client
	return nil
}

func anyToAttributeValue(root any) types.AttributeValue {
	switch v := root.(type) {
	case map[string]any:
		m := make(map[string]types.AttributeValue, len(v))
		for k, v2 := range v {
			m[k] = anyToAttributeValue(v2)
		}
		return &types.AttributeValueMemberM{
			Value: m,
		}
	case []any:
		l := make([]types.AttributeValue, len(v))
		for i, v2 := range v {
			l[i] = anyToAttributeValue(v2)
		}
		return &types.AttributeValueMemberL{
			Value: l,
		}
	case string:
		return &types.AttributeValueMemberS{
			Value: v,
		}
	case json.Number:
		return &types.AttributeValueMemberS{
			Value: v.String(),
		}
	case float64:
		return &types.AttributeValueMemberN{
			Value: strconv.FormatFloat(v, 'f', -1, 64),
		}
	case int:
		return &types.AttributeValueMemberN{
			Value: strconv.Itoa(v),
		}
	case int64:
		return &types.AttributeValueMemberN{
			Value: strconv.Itoa(int(v)),
		}
	case bool:
		return &types.AttributeValueMemberBOOL{
			Value: v,
		}
	case nil:
		return &types.AttributeValueMemberNULL{
			Value: true,
		}
	}
	return &types.AttributeValueMemberS{
		Value: fmt.Sprintf("%v", root),
	}
}

func jsonToMap(path string, root any) (types.AttributeValue, error) {
	gObj := gabs.Wrap(root)
	if path != "" {
		gObj = gObj.Path(path)
	}
	return anyToAttributeValue(gObj.Data()), nil
}

// buildItem converts a single message of a batch into the DynamoDB item that
// represents it, applying the configured TTL, string columns and JSON map
// columns.
func (d *dynamoDBWriter) buildItem(b service.MessageBatch, i int, p *service.Message) (map[string]types.AttributeValue, error) {
	items := map[string]types.AttributeValue{}
	if d.ttl != 0 && d.conf.TTLKey != "" {
		items[d.conf.TTLKey] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10),
		}
	}
	for k, v := range d.conf.StringColumns {
		s, err := b.TryInterpolatedString(i, v)
		if err != nil {
			return nil, fmt.Errorf("string column %v interpolation error: %w", k, err)
		}
		items[k] = &types.AttributeValueMemberS{
			Value: s,
		}
	}
	if len(d.conf.JSONMapColumns) > 0 {
		jRoot, err := p.AsStructured()
		if err != nil {
			d.log.Errorf("Failed to extract JSON maps from document: %v", err)
			return nil, err
		}
		for k, v := range d.conf.JSONMapColumns {
			attr, err := jsonToMap(v, jRoot)
			if err != nil {
				d.log.Warnf("Unable to extract JSON map path '%v' from document: %v", v, err)
				return nil, err
			}
			if k == "" {
				if mv, ok := attr.(*types.AttributeValueMemberM); ok {
					maps.Copy(items, mv.Value)
					continue
				}
			}
			items[k] = attr
		}
	}
	return items, nil
}

// buildFailure records a message of a batch that could not be converted into
// a DynamoDB item, and therefore was never sent.
type buildFailure struct {
	index int
	err   error
}

// newBatchError returns a batch error for b with the given headline that
// already marks every message that failed to build as failed. Messages that
// could not be built must be marked failed on every error returned from
// WriteBatch, otherwise a batch error that only names the messages that failed
// to write would report them as delivered and they would be acknowledged
// without ever reaching DynamoDB.
func newBatchError(b service.MessageBatch, headline error, buildFailures []buildFailure) *service.BatchError {
	batchErr := service.NewBatchError(b, headline)
	for _, f := range buildFailures {
		batchErr.Failed(f.index, f.err)
	}
	return batchErr
}

func (d *dynamoDBWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	if d.client == nil {
		return service.ErrNotConnected
	}

	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	// A message that cannot be built into an item must not prevent the rest
	// of the batch from being written, and must not be reported as delivered
	// either. Build failures are therefore collected here and merged into
	// whatever error the write phase produces, while the requests that did
	// build are sent as usual. reqIndexes maps each write request back to the
	// index of its message within b.
	var buildFailures []buildFailure
	writeReqs := make([]types.WriteRequest, 0, len(b))
	reqIndexes := make([]int, 0, len(b))
	for i, p := range b {
		item, err := d.buildItem(b, i, p)
		if err != nil {
			buildFailures = append(buildFailures, buildFailure{index: i, err: err})
			continue
		}
		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})
		reqIndexes = append(reqIndexes, i)
	}

	// buildErr is the error to return when every request that was sent
	// succeeded: nil when everything built, otherwise a batch error that
	// marks only the messages that failed to build.
	var buildErr error
	if len(buildFailures) > 0 {
		buildErr = newBatchError(b, buildFailures[0].err, buildFailures)
	}
	if len(writeReqs) == 0 {
		return buildErr
	}

	batchResult, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			*d.table: writeReqs,
		},
	})
	if err != nil {
		headlineErr := err

		// None of the messages were successful, attempt to send individually.
		// Each round reports the messages that still failed to write, on top
		// of those that failed to build. If the retries run out midway the
		// error of the previous round is returned, which covers every request
		// still pending and is therefore safe to use for acknowledgements.
		for {
			batchErr := newBatchError(b, headlineErr, buildFailures)
			putFailures := 0
			for i, req := range writeReqs {
				if req.PutRequest == nil {
					continue
				}
				if _, iErr := d.client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: d.table,
					Item:      req.PutRequest.Item,
				}); iErr != nil {
					d.log.Errorf("Put error: %v\n", iErr)
					wait := boff.NextBackOff()
					if wait == backoff.Stop {
						return err
					}
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						return err
					}
					putFailures++
					batchErr.Failed(reqIndexes[i], iErr)
				} else {
					writeReqs[i].PutRequest = nil
				}
			}
			if putFailures == 0 {
				return buildErr
			}
			err = batchErr
		}
	}

	unproc := batchResult.UnprocessedItems[*d.table]
unprocessedLoop:
	for len(unproc) > 0 {
		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			break unprocessedLoop
		}

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			break unprocessedLoop
		}
		if batchResult, err = d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				*d.table: unproc,
			},
		}); err != nil {
			d.log.Errorf("Write multi error: %v\n", err)
		} else if unproc = batchResult.UnprocessedItems[*d.table]; len(unproc) > 0 {
			err = fmt.Errorf("setting %v items", len(unproc))
		} else {
			unproc = nil
		}
	}

	if len(unproc) > 0 {
		if err == nil {
			err = errors.New("ran out of request retries")
		}
	}
	if err != nil {
		// Some items were never written and cannot be matched back to their
		// messages, so the whole batch is failed; this also covers the
		// messages that failed to build.
		return err
	}
	return buildErr
}

func (*dynamoDBWriter) Close(context.Context) error {
	return nil
}
