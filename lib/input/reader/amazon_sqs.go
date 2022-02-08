package reader

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
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

// AmazonSQSConfig contains configuration values for the input type.
type AmazonSQSConfig struct {
	sess.Config         `json:",inline" yaml:",inline"`
	URL                 string `json:"url" yaml:"url"`
	Timeout             string `json:"timeout" yaml:"timeout"`
	MaxNumberOfMessages int64  `json:"max_number_of_messages" yaml:"max_number_of_messages"`
	DeleteMessage       bool   `json:"delete_message" yaml:"delete_message"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	return AmazonSQSConfig{
		Config:              sess.NewConfig(),
		URL:                 "",
		Timeout:             "5s",
		MaxNumberOfMessages: 1,
		DeleteMessage:       true,
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos reader.Type implementation that reads messages from an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig

	pendingHandles map[string]string

	session *session.Session
	sqs     *sqs.SQS
	timeout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQS creates a new Amazon SQS reader.Type.
func NewAmazonSQS(
	conf AmazonSQSConfig,
	log log.Modular,
	stats metrics.Type,
) (*AmazonSQS, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}
	return &AmazonSQS{
		conf:           conf,
		log:            log,
		stats:          stats,
		timeout:        timeout,
		pendingHandles: map[string]string{},
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target SQS
// queue.
func (a *AmazonSQS) ConnectWithContext(ctx context.Context) error {
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

func addSQSMetadata(p *message.Part, sqsMsg *sqs.Message) {
	p.MetaSet("sqs_message_id", *sqsMsg.MessageId)
	p.MetaSet("sqs_receipt_handle", *sqsMsg.ReceiptHandle)
	if rCountStr := sqsMsg.Attributes["ApproximateReceiveCount"]; rCountStr != nil {
		p.MetaSet("sqs_approximate_receive_count", *rCountStr)
	}
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			p.MetaSet(k, *v.StringValue)
		}
	}
}

// ReadWithContext attempts to read a new message from the target SQS.
func (a *AmazonSQS) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	if a.session == nil {
		return nil, nil, component.ErrNotConnected
	}

	msg := message.QuickBatch(nil)
	pendingHandles := map[string]string{}

	output, err := a.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(a.conf.URL),
		MaxNumberOfMessages:   aws.Int64(a.conf.MaxNumberOfMessages),
		WaitTimeSeconds:       aws.Int64(int64(a.timeout.Seconds())),
		AttributeNames:        []*string{aws.String("All")},
		MessageAttributeNames: []*string{aws.String("All")},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}
	for _, sqsMsg := range output.Messages {
		if sqsMsg.ReceiptHandle != nil {
			pendingHandles[*sqsMsg.MessageId] = *sqsMsg.ReceiptHandle
		}

		if sqsMsg.Body != nil {
			part := message.NewPart([]byte(*sqsMsg.Body))
			addSQSMetadata(part, sqsMsg)
			msg.Append(part)
		}
	}
	if msg.Len() == 0 {
		return nil, nil, component.ErrTimeout
	}

	return msg, func(rctx context.Context, res types.Response) error {
		// TODO: Replace this with a background process for batching these
		// requests up more.
		if res.AckError() == nil {
			if !a.conf.DeleteMessage {
				return nil
			}
			for len(pendingHandles) > 0 {
				input := sqs.DeleteMessageBatchInput{
					QueueUrl: aws.String(a.conf.URL),
				}

				for k, v := range pendingHandles {
					input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
						Id:            aws.String(k),
						ReceiptHandle: aws.String(v),
					})
					delete(pendingHandles, k)
					if len(input.Entries) == 10 {
						break
					}
				}

				response, serr := a.sqs.DeleteMessageBatchWithContext(rctx, &input)
				if serr != nil {
					a.log.Errorf("Failed to delete consumed SQS messages: %v\n", serr)
					return serr
				}
				for _, fail := range response.Failed {
					a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", *fail.Id, *fail.Code)
				}
			}
		} else {
			for len(pendingHandles) > 0 {
				input := sqs.ChangeMessageVisibilityBatchInput{
					QueueUrl: aws.String(a.conf.URL),
				}

				for k, v := range pendingHandles {
					input.Entries = append(input.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
						Id:                aws.String(k),
						ReceiptHandle:     aws.String(v),
						VisibilityTimeout: aws.Int64(0),
					})
					delete(pendingHandles, k)
					if len(input.Entries) == 10 {
						break
					}
				}

				response, serr := a.sqs.ChangeMessageVisibilityBatchWithContext(rctx, &input)
				if serr != nil {
					a.log.Errorf("Failed to change consumed SQS message visibility: %v\n", serr)
					return serr
				}
				for _, fail := range response.Failed {
					a.log.Errorf("Failed to change consumed SQS message '%v' visibility, response code: %v\n", *fail.Id, *fail.Code)
				}
			}
		}
		return nil
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonSQS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonSQS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
