package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/public/service"
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
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	if conf.backoffCtor, err = pure.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
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
The field `+"`string_columns`"+` is a map of column names to string values, where the values are [function interpolated](/docs/configuration/interpolation#bloblang-queries) per message of a batch. This allows you to populate string columns of an item by extracting fields within the document payload or metadata like follows:

`+"```yml"+`
string_columns:
  id: ${!json("id")}
  title: ${!json("body.title")}
  topic: ${!meta("kafka_topic")}
  full_content: ${!content()}
`+"```"+`

The field `+"`json_map_columns`"+` is a map of column names to json paths, where the [dot path](/docs/configuration/field_paths) is extracted from each document and converted into a map value. Both an empty path and the path `+"`.`"+` are interpreted as the root of the document. This allows you to populate map columns of an item like follows:

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

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).

## Performance

This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field `+"`max_in_flight`"+`.

This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more [in this doc](/docs/configuration/batching).
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
				Description("A map of column keys to [field paths](/docs/configuration/field_paths) pointing to value data within messages.").
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
				Description("The column key to place the TTL value within.").
				Default("").
				Advanced(),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(ddboFieldBatching),
		).
		Fields(config.SessionFields()...).
		Fields(pure.CommonRetryBackOffFields(3, "1s", "5s", "30s")...)
}

func init() {
	err := service.RegisterBatchOutput("aws_dynamodb", ddboOutputSpec(),
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
	if err != nil {
		panic(err)
	}
}

type dynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
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
			return nil, fmt.Errorf("failed to parse TTL: %v", err)
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
	d.log.Infof("Sending messages to DynamoDB table: %v\n", d.conf.Table)
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
	if len(path) > 0 {
		gObj = gObj.Path(path)
	}
	return anyToAttributeValue(gObj.Data()), nil
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

	writeReqs := []types.WriteRequest{}
	if err := b.WalkWithBatchedErrors(func(i int, p *service.Message) error {
		items := map[string]types.AttributeValue{}
		if d.ttl != 0 && d.conf.TTLKey != "" {
			items[d.conf.TTLKey] = &types.AttributeValueMemberN{
				Value: strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10),
			}
		}
		for k, v := range d.conf.StringColumns {
			s, err := b.TryInterpolatedString(i, v)
			if err != nil {
				return fmt.Errorf("string column %v interpolation error: %w", k, err)
			}
			items[k] = &types.AttributeValueMemberS{
				Value: s,
			}
		}
		if len(d.conf.JSONMapColumns) > 0 {
			jRoot, err := p.AsStructured()
			if err != nil {
				d.log.Errorf("Failed to extract JSON maps from document: %v", err)
				return err
			}
			for k, v := range d.conf.JSONMapColumns {
				if attr, err := jsonToMap(v, jRoot); err == nil {
					if k == "" {
						if mv, ok := attr.(*types.AttributeValueMemberM); ok {
							for ak, av := range mv.Value {
								items[ak] = av
							}
						} else {
							items[k] = attr
						}
					} else {
						items[k] = attr
					}
				} else {
					d.log.Warnf("Unable to extract JSON map path '%v' from document: %v", v, err)
					return err
				}
			}
		}
		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: items,
			},
		})
		return nil
	}); err != nil {
		return err
	}

	batchResult, err := d.client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			*d.table: writeReqs,
		},
	})
	if err != nil {
		headlineErr := err

		// None of the messages were successful, attempt to send individually
	individualRequestsLoop:
		for err != nil {
			batchErr := service.NewBatchError(b, headlineErr)
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
						break individualRequestsLoop
					}
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						break individualRequestsLoop
					}
					batchErr.Failed(i, iErr)
				} else {
					writeReqs[i].PutRequest = nil
				}
			}
			if batchErr.IndexedErrors() == 0 {
				err = nil
			} else {
				err = batchErr
			}
		}
		return err
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
			err = fmt.Errorf("failed to set %v items", len(unproc))
		} else {
			unproc = nil
		}
	}

	if len(unproc) > 0 {
		if err == nil {
			err = errors.New("ran out of request retries")
		}
	}
	return err
}

func (d *dynamoDBWriter) Close(context.Context) error {
	return nil
}
