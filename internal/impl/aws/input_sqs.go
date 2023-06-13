package aws

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/cenkalti/backoff/v4"
)

const (
	// SQS Input Fields
	sqsiFieldURL                 = "url"
	sqsiFieldWaitTimeSeconds     = "wait_time_seconds"
	sqsiFieldDeleteMessage       = "delete_message"
	sqsiFieldResetVisibility     = "reset_visibility"
	sqsiFieldMaxNumberOfMessages = "max_number_of_messages"

	sqsiAttributeNameVisibilityTimeout = "VisibilityTimeout"
)

type sqsiConfig struct {
	URL                 string
	WaitTimeSeconds     int
	DeleteMessage       bool
	ResetVisibility     bool
	MaxNumberOfMessages int
}

func sqsiConfigFromParsed(pConf *service.ParsedConfig) (conf sqsiConfig, err error) {
	if conf.URL, err = pConf.FieldString(sqsiFieldURL); err != nil {
		return
	}
	if conf.WaitTimeSeconds, err = pConf.FieldInt(sqsiFieldWaitTimeSeconds); err != nil {
		return
	}
	if conf.DeleteMessage, err = pConf.FieldBool(sqsiFieldDeleteMessage); err != nil {
		return
	}
	if conf.ResetVisibility, err = pConf.FieldBool(sqsiFieldResetVisibility); err != nil {
		return
	}
	if conf.MaxNumberOfMessages, err = pConf.FieldInt(sqsiFieldMaxNumberOfMessages); err != nil {
		return
	}
	return
}

func sqsInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "AWS").
		Summary(`Consume messages from an AWS SQS URL.`).
		Description(`
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- sqs_message_id
- sqs_receipt_handle
- sqs_approximate_receive_count
- All message attributes
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewURLField(sqsiFieldURL).
				Description("The SQS URL to consume from."),
			service.NewBoolField(sqsiFieldDeleteMessage).
				Description("Whether to delete the consumed message once it is acked. Disabling allows you to handle the deletion using a different mechanism.").
				Default(true).
				Advanced(),
			service.NewBoolField(sqsiFieldResetVisibility).
				Description("Whether to set the visibility timeout of the consumed message to zero once it is nacked. Disabling honors the preset visibility timeout specified for the queue.").
				Version("3.58.0").
				Default(true).
				Advanced(),
			service.NewIntField(sqsiFieldMaxNumberOfMessages).
				Description("The maximum number of messages to return on one poll. Valid values: 1 to 10.").
				Default(10).
				Advanced(),
			service.NewIntField("wait_time_seconds").
				Description("Whether to set the wait time. Enabling this activates long-polling. Valid values: 0 to 20.").
				Default(0).
				Advanced(),
		).
		Fields(config.SessionFields()...)
}

func init() {
	err := service.RegisterInput("aws_sqs", sqsInputSpec(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			sess, err := GetSession(pConf)
			if err != nil {
				return nil, err
			}

			conf, err := sqsiConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			return newAWSSQSReader(conf, sess, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type awsSQSReader struct {
	conf sqsiConfig

	session *session.Session
	sqs     sqsiface.SQSAPI

	messagesChan     chan *sqs.Message
	ackMessagesChan  chan sqsMessageHandle
	nackMessagesChan chan sqsMessageHandle
	addInflightChan  chan sqsMessageHandle
	delInflightChan  chan sqsMessageHandle
	closeSignal      *shutdown.Signaller

	log *service.Logger
}

func newAWSSQSReader(conf sqsiConfig, sess *session.Session, log *service.Logger) (*awsSQSReader, error) {
	return &awsSQSReader{
		conf:             conf,
		session:          sess,
		log:              log,
		messagesChan:     make(chan *sqs.Message),
		ackMessagesChan:  make(chan sqsMessageHandle),
		nackMessagesChan: make(chan sqsMessageHandle),
		addInflightChan:  make(chan sqsMessageHandle),
		delInflightChan:  make(chan sqsMessageHandle),
		closeSignal:      shutdown.NewSignaller(),
	}, nil
}

// Connect attempts to establish a connection to the target SQS
// queue.
func (a *awsSQSReader) Connect(ctx context.Context) error {
	if a.sqs == nil {
		a.sqs = sqs.New(a.session)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go a.readLoop(&wg)
	go a.ackLoop(&wg)
	go a.updateVisibilityLoop(&wg)
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
			a.delInflightChan <- h
			pendingAcks = append(pendingAcks, h)
			if len(pendingAcks) >= a.conf.MaxNumberOfMessages {
				flushAcks()
			}
		case h := <-a.nackMessagesChan:
			a.delInflightChan <- h
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
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = time.Minute
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
			for _, msg := range res.Messages {
				a.addInflightChan <- sqsMessageHandle{
					id:            *msg.MessageId,
					receiptHandle: *msg.ReceiptHandle,
				}
			}
		}
		if len(res.Messages) > 0 || a.conf.WaitTimeSeconds > 0 {
			// When long polling we want to reset our back off even if we didn't
			// receive messages. However, with long polling disabled we back off
			// each time we get an empty response.
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

func (a *awsSQSReader) updateVisibilityLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	var inflightMsgs []sqsMessageHandle

	updateMsgs := func() {
		if len(inflightMsgs) == 0 {
			return
		}
		ctx, done := a.closeSignal.CloseNowCtx(context.Background())
		defer done()

		attributes, err := a.sqs.GetQueueAttributes(&sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(a.conf.URL),
			AttributeNames: []*string{aws.String(sqsiAttributeNameVisibilityTimeout)},
		})
		if err != nil {
			a.log.Errorf("Failed to retrieve queue visibility timeout: %v", err)
			return
		}

		timeoutSeconds, err := strconv.Atoi(*attributes.Attributes[sqsiAttributeNameVisibilityTimeout])
		if err != nil {
			a.log.Errorf("Failed to parse queue visibility timeout: %v", err)
			return
		}

		if err := a.updateVisibilityMessages(ctx, timeoutSeconds, inflightMsgs...); err != nil {
			a.log.Errorf("Failed to update messages visibility timeout: %v", err)
		}
	}

	updateTimer := time.NewTicker(time.Second)
	defer updateTimer.Stop()

	for {
		select {
		case h := <-a.addInflightChan:
			inflightMsgs = append(inflightMsgs, h)
		case h := <-a.delInflightChan:
			for i, msg := range inflightMsgs {
				if h == msg {
					inflightMsgs = append(inflightMsgs[:i], inflightMsgs[i+1:]...)
					break
				}
			}
		case <-updateTimer.C:
			updateMsgs()
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

	return a.updateVisibilityMessages(ctx, 0, msgs...)
}

func (a *awsSQSReader) updateVisibilityMessages(ctx context.Context, timeout int, msgs ...sqsMessageHandle) error {
	for len(msgs) > 0 {
		input := sqs.ChangeMessageVisibilityBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []*sqs.ChangeMessageVisibilityBatchRequestEntry{},
		}

		for _, msg := range msgs {
			input.Entries = append(input.Entries, &sqs.ChangeMessageVisibilityBatchRequestEntry{
				Id:                aws.String(msg.id),
				ReceiptHandle:     aws.String(msg.receiptHandle),
				VisibilityTimeout: aws.Int64(int64(timeout)),
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

func addSQSMetadata(p *service.Message, sqsMsg *sqs.Message) {
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
func (a *awsSQSReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if a.session == nil {
		return nil, nil, service.ErrNotConnected
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

	if next.Body == nil {
		return nil, nil, component.ErrTimeout
	}

	msg := service.NewMessage([]byte(*next.Body))
	addSQSMetadata(msg, next)

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
