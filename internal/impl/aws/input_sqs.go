package aws

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		r, err := newAWSSQSReader(conf.AWSSQS, nm.Logger())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("aws_sqs", r, nm)
	}), docs.ComponentSpec{
		Name:   "aws_sqs",
		Status: docs.StatusStable,
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
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "The SQS URL to consume from."),
			docs.FieldBool("delete_message", "Whether to delete the consumed message once it is acked. Disabling allows you to handle the deletion using a different mechanism.").Advanced(),
			docs.FieldBool("reset_visibility", "Whether to set the visibility timeout of the consumed message to zero once it is nacked. Disabling honors the preset visibility timeout specified for the queue.").AtVersion("3.58.0").Advanced(),
			docs.FieldInt("max_number_of_messages", "The maximum number of messages to return on one poll. Valid values: 1 to 10.").Advanced(),
			docs.FieldInt("wait_time_seconds", "Whether to set the wait time. Enabling this activates long-polling. Valid values: 0 to 20.").Advanced(),
		).WithChildren(sess.FieldSpecs()...).ChildDefaultAndTypesFromStruct(input.NewAWSSQSConfig()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type awsSQSReader struct {
	conf input.AWSSQSConfig

	session *session.Session
	sqs     *sqs.SQS

	messagesChan     chan *sqs.Message
	ackMessagesChan  chan sqsMessageHandle
	nackMessagesChan chan sqsMessageHandle
	closeSignal      *shutdown.Signaller

	log log.Modular
}

func newAWSSQSReader(conf input.AWSSQSConfig, log log.Modular) (*awsSQSReader, error) {
	return &awsSQSReader{
		conf:             conf,
		log:              log,
		messagesChan:     make(chan *sqs.Message),
		ackMessagesChan:  make(chan sqsMessageHandle),
		nackMessagesChan: make(chan sqsMessageHandle),
		closeSignal:      shutdown.NewSignaller(),
	}, nil
}

// Connect attempts to establish a connection to the target SQS
// queue.
func (a *awsSQSReader) Connect(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.Config)
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

func (a *awsSQSReader) ackLoop(wg *sync.WaitGroup) {
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
			if len(pendingAcks) >= a.conf.MaxNumberOfMessages {
				flushAcks()
			}
		case h := <-a.nackMessagesChan:
			pendingNacks = append(pendingNacks, h)
			if len(pendingNacks) >= a.conf.MaxNumberOfMessages {
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

func (a *awsSQSReader) readLoop(wg *sync.WaitGroup) {
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
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 5 * time.Minute
	backoff.MaxElapsedTime = 0

	getMsgs := func() {
		ctx, done := a.closeSignal.CloseAtLeisureCtx(context.Background())
		defer done()
		res, err := a.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(a.conf.URL),
			MaxNumberOfMessages:   aws.Int64(int64(a.conf.MaxNumberOfMessages)),
			WaitTimeSeconds:       aws.Int64(int64(a.conf.WaitTimeSeconds)),
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

func (a *awsSQSReader) deleteMessages(ctx context.Context, msgs ...sqsMessageHandle) error {
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
			if len(input.Entries) == a.conf.MaxNumberOfMessages {
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

func (a *awsSQSReader) resetMessages(ctx context.Context, msgs ...sqsMessageHandle) error {
	if !a.conf.ResetVisibility {
		return nil
	}

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
			if len(input.Entries) == a.conf.MaxNumberOfMessages {
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
	p.MetaSetMut("sqs_message_id", *sqsMsg.MessageId)
	p.MetaSetMut("sqs_receipt_handle", *sqsMsg.ReceiptHandle)
	if rCountStr := sqsMsg.Attributes["ApproximateReceiveCount"]; rCountStr != nil {
		p.MetaSetMut("sqs_approximate_receive_count", *rCountStr)
	}
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			p.MetaSetMut(k, *v.StringValue)
		}
	}
}

// ReadBatch attempts to read a new message from the target SQS.
func (a *awsSQSReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
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
		msg = append(msg, part)
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
	return msg, func(rctx context.Context, res error) error {
		if mHandle.receiptHandle == "" {
			return nil
		}

		if res == nil {
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

func (a *awsSQSReader) Close(ctx context.Context) error {
	a.closeSignal.CloseAtLeisure()

	var closeNowAt time.Duration
	if dline, ok := ctx.Deadline(); ok {
		if closeNowAt = time.Until(dline) - time.Second; closeNowAt <= 0 {
			a.closeSignal.CloseNow()
		}
	}
	if closeNowAt > 0 {
		select {
		case <-time.After(closeNowAt):
			a.closeSignal.CloseNow()
		case <-ctx.Done():
			return ctx.Err()
		case <-a.closeSignal.HasClosedChan():
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closeSignal.HasClosedChan():
	}
	return nil
}
