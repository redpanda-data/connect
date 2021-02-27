package input

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSSQS] = TypeSpec{
		Status: docs.StatusStable,
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newAWSSQS(conf.AWSSQS, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(TypeAWSSQS, false, r, log, stats)
		}),
		Summary: `
Consume messages from an AWS SQS URL.`,
		Description: `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- sqs_message_id
- sqs_receipt_handle
- sqs_approximate_receive_count
- All message attributes
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("url", "The SQS URL to consume from."),
			docs.FieldAdvanced("delete_message", "Whether to delete the consumed message once it is acked. Disabling allows you to handle the deletion using a different mechanism."),
		}, sess.FieldSpecs()...),
		Categories: []Category{
			CategoryServices,
			CategoryAWS,
		},
	}
}

//------------------------------------------------------------------------------

// AWSSQSConfig contains configuration values for the input type.
type AWSSQSConfig struct {
	sess.Config   `json:",inline" yaml:",inline"`
	URL           string `json:"url" yaml:"url"`
	DeleteMessage bool   `json:"delete_message" yaml:"delete_message"`
}

// NewAWSSQSConfig creates a new Config with default values.
func NewAWSSQSConfig() AWSSQSConfig {
	return AWSSQSConfig{
		Config:        sess.NewConfig(),
		URL:           "",
		DeleteMessage: true,
	}
}

//------------------------------------------------------------------------------

type awsSQS struct {
	conf AWSSQSConfig

	session *session.Session
	sqs     *sqs.SQS

	pending     []*sqs.Message
	pendingMut  sync.Mutex
	nextRequest time.Time

	log   log.Modular
	stats metrics.Type
}

func newAWSSQS(conf AWSSQSConfig, log log.Modular, stats metrics.Type) (*awsSQS, error) {
	return &awsSQS{
		conf:  conf,
		log:   log,
		stats: stats,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target SQS
// queue.
func (a *awsSQS) ConnectWithContext(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.sqs = sqs.New(sess)
	a.session = sess

	a.log.Infof("Receiving Amazon SQS messages from URL: %v\n", a.conf.URL)
	return nil
}

func addSQSMetadata(p types.Part, sqsMsg *sqs.Message) {
	meta := p.Metadata()
	meta.Set("sqs_message_id", *sqsMsg.MessageId)
	meta.Set("sqs_receipt_handle", *sqsMsg.ReceiptHandle)
	if rCountStr := sqsMsg.Attributes["ApproximateReceiveCount"]; rCountStr != nil {
		meta.Set("sqs_approximate_receive_count", *rCountStr)
	}
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			meta.Set(k, *v.StringValue)
		}
	}
}

// ReadWithContext attempts to read a new message from the target SQS.
func (a *awsSQS) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	if a.session == nil {
		return nil, nil, types.ErrNotConnected
	}

	var next *sqs.Message

	a.pendingMut.Lock()
	defer a.pendingMut.Unlock()

	if len(a.pending) > 0 {
		next = a.pending[0]
		a.pending = a.pending[1:]
	} else if !a.nextRequest.IsZero() {
		if until := time.Until(a.nextRequest); until > 0 {
			select {
			case <-time.After(until):
			case <-ctx.Done():
				return nil, nil, types.ErrTimeout
			}
		}

		output, err := a.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(a.conf.URL),
			MaxNumberOfMessages:   aws.Int64(10),
			AttributeNames:        []*string{aws.String("All")},
			MessageAttributeNames: []*string{aws.String("All")},
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
				return nil, nil, types.ErrTimeout
			}
			return nil, nil, err
		}

		if len(output.Messages) > 0 {
			next = output.Messages[0]
			a.pending = output.Messages[1:]
		}
	}

	if next == nil {
		a.nextRequest = time.Now().Add(time.Millisecond * 500)
		return nil, nil, types.ErrTimeout
	}
	a.nextRequest = time.Time{}

	msg := message.New(nil)
	if next.Body != nil {
		part := message.NewPart([]byte(*next.Body))
		addSQSMetadata(part, next)
		msg.Append(part)
	}
	if msg.Len() == 0 {
		return nil, nil, types.ErrTimeout
	}

	return msg, func(rctx context.Context, res types.Response) error {
		if res.Error() == nil {
			if !a.conf.DeleteMessage || next.ReceiptHandle == nil {
				return nil
			}
			_, serr := a.sqs.DeleteMessageWithContext(rctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(a.conf.URL),
				ReceiptHandle: aws.String(*next.ReceiptHandle),
			})
			if serr != nil {
				if aerr, ok := serr.(awserr.Error); !ok && aerr.Code() != request.CanceledErrorCode {
					a.log.Errorf("Failed to delete consumed SQS messages: %v\n", serr)
				}
				return serr
			}
			return nil
		}
		if next.ReceiptHandle == nil {
			return nil
		}
		_, serr := a.sqs.ChangeMessageVisibilityWithContext(rctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(a.conf.URL),
			ReceiptHandle:     aws.String(*next.ReceiptHandle),
			VisibilityTimeout: aws.Int64(0),
		})
		if serr != nil {
			if aerr, ok := serr.(awserr.Error); !ok && aerr.Code() != request.CanceledErrorCode {
				a.log.Errorf("Failed to change consumed SQS message visibility: %v\n", serr)
			}
			return serr
		}
		return nil
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *awsSQS) CloseAsync() {
	go func() {
		a.pendingMut.Lock()
		defer a.pendingMut.Unlock()

		for _, next := range a.pending {
			_, serr := a.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(a.conf.URL),
				ReceiptHandle:     aws.String(*next.ReceiptHandle),
				VisibilityTimeout: aws.Int64(0),
			})
			if serr != nil {
				if aerr, ok := serr.(awserr.Error); !ok && aerr.Code() != request.CanceledErrorCode {
					a.log.Errorf("Failed to change consumed SQS message visibility: %v\n", serr)
				}
			}
		}
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *awsSQS) WaitForClose(time.Duration) error {
	return nil
}
