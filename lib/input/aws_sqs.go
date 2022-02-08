package input

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
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
	"github.com/cenkalti/backoff/v4"
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
[in this document](/docs/guides/cloud/aws).

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
			docs.FieldAdvanced("reset_visibility", "Whether to set the visibility timeout of the consumed message to zero once it is nacked. Disabling honors the preset visibility timeout specified for the queue.").AtVersion("3.58.0"),
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
	sess.Config     `json:",inline" yaml:",inline"`
	URL             string `json:"url" yaml:"url"`
	DeleteMessage   bool   `json:"delete_message" yaml:"delete_message"`
	ResetVisibility bool   `json:"reset_visibility" yaml:"reset_visibility"`
}

// NewAWSSQSConfig creates a new Config with default values.
func NewAWSSQSConfig() AWSSQSConfig {
	return AWSSQSConfig{
		Config:          sess.NewConfig(),
		URL:             "",
		DeleteMessage:   true,
		ResetVisibility: true,
	}
}

//------------------------------------------------------------------------------

type awsSQS struct {
	conf AWSSQSConfig

	session *session.Session
	sqs     *sqs.SQS

	messagesChan     chan *sqs.Message
	ackMessagesChan  chan sqsMessageHandle
	nackMessagesChan chan sqsMessageHandle
	closeSignal      *shutdown.Signaller

	log   log.Modular
	stats metrics.Type
}

func newAWSSQS(conf AWSSQSConfig, log log.Modular, stats metrics.Type) (*awsSQS, error) {
	return &awsSQS{
		conf:             conf,
		log:              log,
		stats:            stats,
		messagesChan:     make(chan *sqs.Message),
		ackMessagesChan:  make(chan sqsMessageHandle),
		nackMessagesChan: make(chan sqsMessageHandle),
		closeSignal:      shutdown.NewSignaller(),
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

	var wg sync.WaitGroup
	wg.Add(2)
	go a.readLoop(&wg)
	go a.ackLoop(&wg)
	go func() {
		wg.Wait()
		a.closeSignal.ShutdownComplete()
	}()

	a.log.Infof("Receiving Amazon SQS messages from URL: %v\n", a.conf.URL)
	return nil
}

func (a *awsSQS) ackLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	var pendingAcks []sqsMessageHandle
	var pendingNacks []sqsMessageHandle

	flushAcks := func() {
		tmpAcks := pendingAcks
		pendingAcks = nil
		if len(tmpAcks) == 0 {
			return
		}

		ctx, done := a.closeSignal.CloseNowCtx(context.Background())
		defer done()
		if err := a.deleteMessages(ctx, tmpAcks...); err != nil {
			a.log.Errorf("Failed to delete messages: %v", err)
		}
	}

	flushNacks := func() {
		tmpNacks := pendingNacks
		pendingNacks = nil
		if len(tmpNacks) == 0 {
			return
		}

		ctx, done := a.closeSignal.CloseNowCtx(context.Background())
		defer done()
		if err := a.resetMessages(ctx, tmpNacks...); err != nil {
			a.log.Errorf("Failed to reset the visibility timeout of messages: %v", err)
		}
	}

	flushTimer := time.NewTicker(time.Second)
	defer flushTimer.Stop()

ackLoop:
	for {
		select {
		case h := <-a.ackMessagesChan:
			pendingAcks = append(pendingAcks, h)
			if len(pendingAcks) >= 10 {
				flushAcks()
			}
		case h := <-a.nackMessagesChan:
			pendingNacks = append(pendingNacks, h)
			if len(pendingNacks) >= 10 {
				flushNacks()
			}
		case <-flushTimer.C:
			flushAcks()
			flushNacks()
		case <-a.closeSignal.CloseAtLeisureChan():
			break ackLoop
		}
	}

	flushAcks()
	flushNacks()
}

func (a *awsSQS) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	var pendingMsgs []*sqs.Message
	defer func() {
		if len(pendingMsgs) > 0 {
			tmpNacks := make([]sqsMessageHandle, 0, len(pendingMsgs))
			for _, m := range pendingMsgs {
				if m.MessageId == nil || m.ReceiptHandle == nil {
					continue
				}
				tmpNacks = append(tmpNacks, sqsMessageHandle{
					id:            *m.MessageId,
					receiptHandle: *m.ReceiptHandle,
				})
			}
			ctx, done := a.closeSignal.CloseNowCtx(context.Background())
			defer done()
			if err := a.resetMessages(ctx, tmpNacks...); err != nil {
				a.log.Errorf("Failed to reset visibility timeout for pending messages: %v", err)
			}
		}
	}()

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = time.Millisecond
	backoff.MaxInterval = time.Second

	getMsgs := func() {
		ctx, done := a.closeSignal.CloseAtLeisureCtx(context.Background())
		defer done()
		res, err := a.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(a.conf.URL),
			MaxNumberOfMessages:   aws.Int64(10),
			AttributeNames:        []*string{aws.String("All")},
			MessageAttributeNames: []*string{aws.String("All")},
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != request.CanceledErrorCode {
				a.log.Errorf("Failed to pull new SQS messages: %v", aerr)
			}
			return
		}
		if len(res.Messages) > 0 {
			pendingMsgs = append(pendingMsgs, res.Messages...)
			backoff.Reset()
		}
	}

	for {
		if len(pendingMsgs) == 0 {
			getMsgs()
			if len(pendingMsgs) == 0 {
				select {
				case <-time.After(backoff.NextBackOff()):
				case <-a.closeSignal.CloseAtLeisureChan():
					return
				}
				continue
			}
		}
		select {
		case a.messagesChan <- pendingMsgs[0]:
			pendingMsgs = pendingMsgs[1:]
		case <-a.closeSignal.CloseAtLeisureChan():
			return
		}
	}
}

type sqsMessageHandle struct {
	id, receiptHandle string
}

func (a *awsSQS) deleteMessages(ctx context.Context, msgs ...sqsMessageHandle) error {
	for len(msgs) > 0 {
		input := sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []*sqs.DeleteMessageBatchRequestEntry{},
		}

		for _, msg := range msgs {
			input.Entries = append(input.Entries, &sqs.DeleteMessageBatchRequestEntry{
				Id:            aws.String(msg.id),
				ReceiptHandle: aws.String(msg.receiptHandle),
			})
			if len(input.Entries) == 10 {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		response, err := a.sqs.DeleteMessageBatchWithContext(ctx, &input)
		if err != nil {
			return err
		}
		for _, fail := range response.Failed {
			a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", *fail.Id, *fail.Code)
		}
	}
	return nil
}

func (a *awsSQS) resetMessages(ctx context.Context, msgs ...sqsMessageHandle) error {
	for len(msgs) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []*sqs.ChangeMessageVisibilityBatchRequestEntry{},
		}

		for _, msg := range msgs {
			input.Entries = append(input.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
				Id:                aws.String(msg.id),
				ReceiptHandle:     aws.String(msg.receiptHandle),
				VisibilityTimeout: aws.Int64(0),
			})
			if len(input.Entries) == 10 {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		response, err := a.sqs.ChangeMessageVisibilityBatchWithContext(ctx, &input)
		if err != nil {
			return err
		}
		for _, fail := range response.Failed {
			a.log.Errorf("Failed to delete consumed SQS message '%v', response code: %v\n", *fail.Id, *fail.Code)
		}
	}
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
func (a *awsSQS) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	if a.session == nil {
		return nil, nil, component.ErrNotConnected
	}

	var next *sqs.Message
	var open bool
	select {
	case next, open = <-a.messagesChan:
		if !open {
			return nil, nil, component.ErrTypeClosed
		}
	case <-a.closeSignal.CloseAtLeisureChan():
		return nil, nil, component.ErrTypeClosed
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	msg := message.QuickBatch(nil)
	if next.Body != nil {
		part := message.NewPart([]byte(*next.Body))
		addSQSMetadata(part, next)
		msg.Append(part)
	}
	if msg.Len() == 0 {
		return nil, nil, component.ErrTimeout
	}

	mHandle := sqsMessageHandle{
		id: *next.MessageId,
	}
	if next.ReceiptHandle != nil {
		mHandle.receiptHandle = *next.ReceiptHandle
	}
	return msg, func(rctx context.Context, res types.Response) error {
		if mHandle.receiptHandle == "" {
			return nil
		}

		if res.Error() == nil {
			if !a.conf.DeleteMessage {
				return nil
			}
			select {
			case <-rctx.Done():
				return rctx.Err()
			case <-a.closeSignal.CloseAtLeisureChan():
				return a.deleteMessages(rctx, mHandle)
			case a.ackMessagesChan <- mHandle:
			}
			return nil
		}

		if !a.conf.ResetVisibility {
			return nil
		}
		select {
		case <-rctx.Done():
			return rctx.Err()
		case <-a.closeSignal.CloseAtLeisureChan():
			return a.resetMessages(rctx, mHandle)
		case a.nackMessagesChan <- mHandle:
		}
		return nil
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *awsSQS) CloseAsync() {
	a.closeSignal.CloseAtLeisure()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *awsSQS) WaitForClose(tout time.Duration) error {
	go func() {
		closeNowAt := tout - time.Second
		if closeNowAt < time.Second {
			closeNowAt = time.Second
		}
		<-time.After(closeNowAt)
		a.closeSignal.CloseNow()
	}()
	select {
	case <-time.After(tout):
		return component.ErrTimeout
	case <-a.closeSignal.HasClosedChan():
		return nil
	}
}
