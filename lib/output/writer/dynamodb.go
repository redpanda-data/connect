package writer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	batchInternal "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/Jeffail/gabs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-cmp/cmp"
)

//------------------------------------------------------------------------------

// DynamoDBConfig contains config fields for the DynamoDB output type.
type DynamoDBConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	Table          string            `json:"table" yaml:"table"`
	StringColumns  map[string]string `json:"string_columns" yaml:"string_columns"`
	JSONMapColumns map[string]string `json:"json_map_columns" yaml:"json_map_columns"`
	TTL            string            `json:"ttl" yaml:"ttl"`
	TTLKey         string            `json:"ttl_key" yaml:"ttl_key"`
	MaxInFlight    int               `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewDynamoDBConfig creates a DynamoDBConfig populated with default values.
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
		Table:          "",
		StringColumns:  map[string]string{},
		JSONMapColumns: map[string]string{},
		TTL:            "",
		TTLKey:         "",
		MaxInFlight:    1,
		Config:         rConf,
		Batching:       batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// DynamoDB is a benthos writer.Type implementation that writes messages to an
// Amazon SQS queue.
type DynamoDB struct {
	client dynamodbiface.DynamoDBAPI
	conf   DynamoDBConfig
	log    log.Modular
	stats  metrics.Type

	backoffCtor func() backoff.BackOff
	boffPool    sync.Pool

	table          *string
	ttl            time.Duration
	strColumns     map[string]*field.Expression
	jsonMapColumns map[string]string
}

// NewDynamoDBV2 creates a new Amazon SQS writer.Type.
func NewDynamoDBV2(
	conf DynamoDBConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*DynamoDB, error) {
	db := &DynamoDB{
		conf:           conf,
		log:            log,
		stats:          stats,
		table:          aws.String(conf.Table),
		strColumns:     map[string]*field.Expression{},
		jsonMapColumns: map[string]string{},
	}
	if len(conf.StringColumns) == 0 && len(conf.JSONMapColumns) == 0 {
		return nil, errors.New("you must provide at least one column")
	}
	var err error
	for k, v := range conf.StringColumns {
		if db.strColumns[k], err = interop.NewBloblangField(mgr, v); err != nil {
			return nil, fmt.Errorf("failed to parse column '%v' expression: %v", k, err)
		}
	}
	for k, v := range conf.JSONMapColumns {
		if v == "." {
			v = ""
		}
		db.jsonMapColumns[k] = v
	}
	if conf.TTL != "" {
		ttl, err := time.ParseDuration(conf.TTL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse TTL: %v", err)
		}
		db.ttl = ttl
	}
	if db.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	db.boffPool = sync.Pool{
		New: func() interface{} {
			return db.backoffCtor()
		},
	}
	return db, nil
}

// Connect attempts to establish a connection to the target SQS queue.
func (d *DynamoDB) Connect() error {
	return d.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a connection to the target DynamoDB
// table.
func (d *DynamoDB) ConnectWithContext(ctx context.Context) error {
	if d.client != nil {
		return nil
	}

	sess, err := d.conf.GetSession()
	if err != nil {
		return err
	}

	client := dynamodb.New(sess)
	out, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: d.table,
	})
	if err != nil {
		return err
	} else if out == nil || out.Table == nil || out.Table.TableStatus == nil || *out.Table.TableStatus != dynamodb.TableStatusActive {
		return fmt.Errorf("dynamodb table '%s' must be active", d.conf.Table)
	}

	d.client = client
	d.log.Infof("Sending messages to DynamoDB table: %v\n", d.conf.Table)
	return nil
}

func walkJSON(root interface{}) *dynamodb.AttributeValue {
	switch v := root.(type) {
	case map[string]interface{}:
		m := make(map[string]*dynamodb.AttributeValue, len(v))
		for k, v2 := range v {
			m[k] = walkJSON(v2)
		}
		return &dynamodb.AttributeValue{
			M: m,
		}
	case []interface{}:
		l := make([]*dynamodb.AttributeValue, len(v))
		for i, v2 := range v {
			l[i] = walkJSON(v2)
		}
		return &dynamodb.AttributeValue{
			L: l,
		}
	case string:
		return &dynamodb.AttributeValue{
			S: aws.String(v),
		}
	case json.Number:
		return &dynamodb.AttributeValue{
			N: aws.String(v.String()),
		}
	case float64:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatFloat(v, 'f', -1, 64)),
		}
	case int:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(v)),
		}
	case int64:
		return &dynamodb.AttributeValue{
			N: aws.String(strconv.Itoa(int(v))),
		}
	case bool:
		return &dynamodb.AttributeValue{
			BOOL: aws.Bool(v),
		}
	case nil:
		return &dynamodb.AttributeValue{
			NULL: aws.Bool(true),
		}
	}
	return &dynamodb.AttributeValue{
		S: aws.String(fmt.Sprintf("%v", root)),
	}
}

func jsonToMap(path string, root interface{}) (*dynamodb.AttributeValue, error) {
	gObj := gabs.Wrap(root)
	if len(path) > 0 {
		gObj = gObj.Path(path)
	}
	return walkJSON(gObj.Data()), nil
}

// Write attempts to write message contents to a target DynamoDB table.
func (d *DynamoDB) Write(msg types.Message) error {
	return d.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target DynamoDB
// table.
func (d *DynamoDB) WriteWithContext(ctx context.Context, msg types.Message) error {
	if d.client == nil {
		return types.ErrNotConnected
	}

	boff := d.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		d.boffPool.Put(boff)
	}()

	writeReqs := []*dynamodb.WriteRequest{}
	msg.Iter(func(i int, p types.Part) error {
		items := map[string]*dynamodb.AttributeValue{}
		if d.ttl != 0 && d.conf.TTLKey != "" {
			items[d.conf.TTLKey] = &dynamodb.AttributeValue{
				N: aws.String(strconv.FormatInt(time.Now().Add(d.ttl).Unix(), 10)),
			}
		}
		for k, v := range d.strColumns {
			s := v.String(i, msg)
			items[k] = &dynamodb.AttributeValue{
				S: &s,
			}
		}
		if len(d.jsonMapColumns) > 0 {
			jRoot, err := p.JSON()
			if err != nil {
				d.log.Errorf("Failed to extract JSON maps from document: %v", err)
			} else {
				for k, v := range d.jsonMapColumns {
					if attr, err := jsonToMap(v, jRoot); err == nil {
						if k == "" {
							for ak, av := range attr.M {
								items[ak] = av
							}
						} else {
							items[k] = attr
						}
					} else {
						d.log.Warnf("Unable to extract JSON map path '%v' from document: %v", v, err)
					}
				}
			}
		}
		writeReqs = append(writeReqs, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: items,
			},
		})
		return nil
	})

	batchResult, err := d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*d.table: writeReqs,
		},
	})
	if err != nil {
		// None of the messages were successful, attempt to send individually
	individualRequestsLoop:
		for err != nil {
			batchErr := batchInternal.NewError(msg, err)
			for i, req := range writeReqs {
				if req == nil {
					continue
				}
				if _, iErr := d.client.PutItem(&dynamodb.PutItemInput{
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
					writeReqs[i] = nil
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
		if batchResult, err = d.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
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

		// Sad, we have unprocessed messages, we need to map the requests back
		// to the origin message index. The DynamoDB API doesn't make this easy.
		batchErr := batchInternal.NewError(msg, err)

	requestsLoop:
		for _, req := range unproc {
			for i, src := range writeReqs {
				if cmp.Equal(req, src) {
					batchErr.Failed(i, errors.New("failed to set item"))
					continue requestsLoop
				}
			}
			// If we're unable to map a single request to the origin message
			// then we return a general error.
			return err
		}

		err = batchErr
	}

	return err
}

// CloseAsync begins cleaning up resources used by this writer asynchronously.
func (d *DynamoDB) CloseAsync() {
}

// WaitForClose will block until either the writer is closed or a specified
// timeout occurs.
func (d *DynamoDB) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
