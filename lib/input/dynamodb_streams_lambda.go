package input

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func init() {
	Constructors[TypeDynamoDBStreamsLambda] = TypeSpec{
		constructor: NewDynamoDBStreamsLambda,
		description: `
Runs in an AWS Lambda and listens for DynamoDB Streams events. Every
Lambda invocation will generate a multipart message containing a record
changed per part.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- dynamodbstreams_event_id
- dynamodbstreams_event_name
- dynamodbstreams_event_source
- dynamodbstreams_event_source_arn
- dynamodbstreams_event_version
- dynamodbstreams_user_identity_type # if applicable
- dynamodbstreams_user_identity_principal_id # if applicable
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

type DynamoDBStreamsLambdaConfig struct {
}

func NewDynamoDBStreamsLambdaConfig() DynamoDBStreamsLambdaConfig {
	return DynamoDBStreamsLambdaConfig{}
}

type DynamoDBStreamsLambda struct {
	running int32

	transactionsChan chan types.Transaction
	closeChan        chan struct{}

	handleMutex sync.Mutex

	logger  log.Modular
	manager types.Manager

	mFiltered    metrics.StatCounter
	mSendSuccess metrics.StatCounter
	mSendError   metrics.StatCounter
	mAckSuccess  metrics.StatCounter
	mLatency     metrics.StatTimer
}

func NewDynamoDBStreamsLambda(
	config Config,
	manager types.Manager,
	logger log.Modular,
	metrics metrics.Type,
) (Type, error) {
	d := &DynamoDBStreamsLambda{
		running:          1,
		transactionsChan: make(chan types.Transaction),

		closeChan: make(chan struct{}),

		logger:  logger,
		manager: manager,

		mSendSuccess: metrics.GetCounter("send.success"),
		mSendError:   metrics.GetCounter("send.error"),
		mAckSuccess:  metrics.GetCounter("ack.success"),
		mLatency:     metrics.GetTimer("latency"),
	}

	go lambda.Start(d.handleRequest)

	return d, nil
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (d *DynamoDBStreamsLambda) TransactionChan() <-chan types.Transaction {
	return d.transactionsChan
}

// CloseAsync shuts down the input and stops processing requests.
func (d *DynamoDBStreamsLambda) CloseAsync() {
	if atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		close(d.closeChan)
	}
}

// WaitForClose blocks until the input has closed down.
func (d *DynamoDBStreamsLambda) WaitForClose(timeout time.Duration) error {
	closedChan := make(chan struct{})
	go func() {
		d.handleMutex.Lock()
		defer d.handleMutex.Unlock()
		close(closedChan)
	}()
	select {
	case <-closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

func (d *DynamoDBStreamsLambda) Connected() bool {
	return atomic.LoadInt32(&d.running) == 1
}

func (d *DynamoDBStreamsLambda) handleRequest(ctx context.Context, e events.DynamoDBEvent) error {
	d.handleMutex.Lock()
	defer d.handleMutex.Unlock()

	if atomic.LoadInt32(&d.running) != 1 {
		return types.ErrChanClosed
	}

	msg := message.New([][]byte{})
	for _, record := range e.Records {
		jsonEncoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		part := message.NewPart(jsonEncoded)
		meta := part.Metadata()
		meta.Set("dynamodbstreams_event_id", record.EventID)
		meta.Set("dynamodbstreams_event_name", record.EventName)
		meta.Set("dynamodbstreams_event_source", record.EventSource)
		meta.Set("dynamodbstreams_event_source_arn", record.EventSourceArn)
		meta.Set("dynamodbstreams_event_version", record.EventVersion)
		if record.UserIdentity != nil {
			meta.Set("dynamodbstreams_user_identity_type", record.UserIdentity.Type)
			meta.Set("dynamodbstreams_user_identity_principal_id", record.UserIdentity.PrincipalID)
		}
		msg.Append(part)
	}

	responsesChan := make(chan types.Response)
	defer close(responsesChan)

	select {
	case d.transactionsChan <- types.NewTransaction(msg, responsesChan):
	case <-d.closeChan:
		return types.ErrChanClosed
	}

	select {
	case res, open := <-responsesChan:
		if !open {
			return types.ErrChanClosed
		}
		if res.Error() != nil {
			d.mSendError.Incr(1)
		} else {
			d.mSendSuccess.Incr(1)
		}
		if res.Error() != nil || !res.SkipAck() {
			return res.Error()
		}
		d.mLatency.Timing(time.Since(msg.CreatedAt()).Nanoseconds())
		d.mAckSuccess.Incr(1)

	case <-d.closeChan:
		return types.ErrChanClosed
	}

	return nil
}
