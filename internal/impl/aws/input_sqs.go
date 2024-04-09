package aws

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
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
			sess, err := GetSession(context.TODO(), pConf)
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

type sqsAPI interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibilityBatch(context.Context, *sqs.ChangeMessageVisibilityBatchInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
}

type awsSQSReader struct {
	conf sqsiConfig

	aconf aws.Config
	sqs   sqsAPI

	messagesChan     chan types.Message
	ackMessagesChan  chan sqsMessageHandle
	nackMessagesChan chan sqsMessageHandle
	closeSignal      *shutdown.Signaller

	log *service.Logger
}

func newAWSSQSReader(conf sqsiConfig, aconf aws.Config, log *service.Logger) (*awsSQSReader, error) {
	return &awsSQSReader{
		conf:             conf,
		aconf:            aconf,
		log:              log,
		messagesChan:     make(chan types.Message),
		ackMessagesChan:  make(chan sqsMessageHandle),
		nackMessagesChan: make(chan sqsMessageHandle),
		closeSignal:      shutdown.NewSignaller(),
	}, nil
}

// Connect attempts to establish a connection to the target SQS
// queue.
func (a *awsSQSReader) Connect(ctx context.Context) error {
	if a.sqs == nil {
		a.sqs = sqs.NewFromConfig(a.aconf)
	}

	ift := &sqsInFlightTracker{
		handles: map[string]sqsInFlightHandle{},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go a.readLoop(&wg, ift)
	go a.ackLoop(&wg, ift)
	go func() {
		wg.Wait()
		a.closeSignal.ShutdownComplete()
	}()
	return nil
}

type sqsInFlightHandle struct {
	receiptHandle  string
	timeoutSeconds int
	addedAt        time.Time
}

type sqsInFlightTracker struct {
	handles map[string]sqsInFlightHandle
	m       sync.Mutex
}

func (t *sqsInFlightTracker) PullToRefresh() (handles []sqsMessageHandle, timeoutSeconds int) {
	t.m.Lock()
	defer t.m.Unlock()

	handles = make([]sqsMessageHandle, 0, len(t.handles))
	for k, v := range t.handles {
		if time.Since(v.addedAt) < time.Second {
			continue
		}
		handles = append(handles, sqsMessageHandle{
			id:            k,
			receiptHandle: v.receiptHandle,
		})
		if v.timeoutSeconds > timeoutSeconds {
			timeoutSeconds = v.timeoutSeconds
		}
	}
	return
}

func (t *sqsInFlightTracker) Remove(id string) {
	t.m.Lock()
	defer t.m.Unlock()
	delete(t.handles, id)
}

func (t *sqsInFlightTracker) AddNew(messages ...types.Message) {
	t.m.Lock()
	defer t.m.Unlock()

	for _, m := range messages {
		if m.MessageId == nil || m.ReceiptHandle == nil {
			continue
		}

		handle := sqsInFlightHandle{
			timeoutSeconds: 30,
			receiptHandle:  *m.ReceiptHandle,
			addedAt:        time.Now(),
		}
		if timeoutStr, exists := m.Attributes[sqsiAttributeNameVisibilityTimeout]; exists {
			// Might as well keep the queue timeout setting refreshed as we
			// consume new data.
			if tmpTimeoutSeconds, err := strconv.Atoi(timeoutStr); err == nil {
				handle.timeoutSeconds = tmpTimeoutSeconds
			}
		}
		t.handles[*m.MessageId] = handle
	}
}

func flushMapToHandles(m map[string]string) (s []sqsMessageHandle) {
	s = make([]sqsMessageHandle, 0, len(m))
	for k, v := range m {
		s = append(s, sqsMessageHandle{id: k, receiptHandle: v})
		delete(m, k)
	}
	return
}

func (a *awsSQSReader) ackLoop(wg *sync.WaitGroup, inFlightTracker *sqsInFlightTracker) {
	defer wg.Done()

	closeNowCtx, done := a.closeSignal.CloseNowCtx(context.Background())
	defer done()

	flushFinishedHandles := func(m map[string]string, erase bool) {
		handles := flushMapToHandles(m)
		if len(handles) == 0 {
			return
		}
		if erase {
			if err := a.deleteMessages(closeNowCtx, handles...); err != nil {
				a.log.Errorf("Failed to delete messages: %v", err)
			}
		} else {
			if err := a.resetMessages(closeNowCtx, handles...); err != nil {
				a.log.Errorf("Failed to reset the visibility timeout of messages: %v", err)
			}
		}
	}

	refreshCurrentHandles := func() {
		currentHandles, timeoutSeconds := inFlightTracker.PullToRefresh()
		if len(currentHandles) == 0 {
			return
		}
		if err := a.updateVisibilityMessages(closeNowCtx, timeoutSeconds, currentHandles...); err != nil {
			a.log.Debugf("Failed to update messages visibility timeout: %v", err)
		}
	}

	flushTimer := time.NewTicker(time.Second)
	defer flushTimer.Stop()

	// Both maps are of the message ID to the receipt handle
	pendingAcks := map[string]string{}
	pendingNacks := map[string]string{}

ackLoop:
	for {
		select {
		case h := <-a.ackMessagesChan:
			pendingAcks[h.id] = h.receiptHandle
			inFlightTracker.Remove(h.id)
			if len(pendingAcks) >= a.conf.MaxNumberOfMessages {
				flushFinishedHandles(pendingAcks, true)
			}
		case h := <-a.nackMessagesChan:
			pendingNacks[h.id] = h.receiptHandle
			inFlightTracker.Remove(h.id)
			if len(pendingNacks) >= a.conf.MaxNumberOfMessages {
				flushFinishedHandles(pendingNacks, false)
			}
		case <-flushTimer.C:
			flushFinishedHandles(pendingAcks, true)
			flushFinishedHandles(pendingNacks, false)
			refreshCurrentHandles()
		case <-a.closeSignal.CloseAtLeisureChan():
			break ackLoop
		}
	}

	flushFinishedHandles(pendingAcks, true)
	flushFinishedHandles(pendingNacks, false)
}

func (a *awsSQSReader) readLoop(wg *sync.WaitGroup, inFlightTracker *sqsInFlightTracker) {
	defer wg.Done()

	var pendingMsgs []types.Message
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

	closeAtLeisureCtx, done := a.closeSignal.CloseAtLeisureCtx(context.Background())
	defer done()

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = time.Minute
	backoff.MaxElapsedTime = 0

	getMsgs := func() {
		res, err := a.sqs.ReceiveMessage(closeAtLeisureCtx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(a.conf.URL),
			MaxNumberOfMessages:   int32(a.conf.MaxNumberOfMessages),
			WaitTimeSeconds:       int32(a.conf.WaitTimeSeconds),
			AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if !awsErrIsTimeout(err) {
				a.log.Errorf("Failed to pull new SQS messages: %v", err)
			}
			return
		}
		if len(res.Messages) > 0 {
			inFlightTracker.AddNew(res.Messages...)
			pendingMsgs = append(pendingMsgs, res.Messages...)
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

type sqsMessageHandle struct {
	id, receiptHandle string
}

func (a *awsSQSReader) deleteMessages(ctx context.Context, msgs ...sqsMessageHandle) error {
	for len(msgs) > 0 {
		input := sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(a.conf.URL),
			Entries:  []types.DeleteMessageBatchRequestEntry{},
		}

		for _, msg := range msgs {
			msg := msg
			input.Entries = append(input.Entries, types.DeleteMessageBatchRequestEntry{
				Id:            &msg.id,
				ReceiptHandle: &msg.receiptHandle,
			})
			if len(input.Entries) == a.conf.MaxNumberOfMessages {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		response, err := a.sqs.DeleteMessageBatch(ctx, &input)
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
			Entries:  []types.ChangeMessageVisibilityBatchRequestEntry{},
		}

		for _, msg := range msgs {
			msg := msg
			input.Entries = append(input.Entries, types.ChangeMessageVisibilityBatchRequestEntry{
				Id:                &msg.id,
				ReceiptHandle:     &msg.receiptHandle,
				VisibilityTimeout: int32(timeout),
			})
			if len(input.Entries) == a.conf.MaxNumberOfMessages {
				break
			}
		}

		msgs = msgs[len(input.Entries):]
		response, err := a.sqs.ChangeMessageVisibilityBatch(ctx, &input)
		if err != nil {
			return err
		}
		for _, fail := range response.Failed {
			a.log.Debugf("Failed to update consumed SQS message '%v' visibility, response code: %v\n", *fail.Id, *fail.Code)
		}
	}
	return nil
}

func addSQSMetadata(p *service.Message, sqsMsg types.Message) {
	p.MetaSetMut("sqs_message_id", *sqsMsg.MessageId)
	p.MetaSetMut("sqs_receipt_handle", *sqsMsg.ReceiptHandle)
	if rCountStr, exists := sqsMsg.Attributes["ApproximateReceiveCount"]; exists {
		p.MetaSetMut("sqs_approximate_receive_count", rCountStr)
	}
	for k, v := range sqsMsg.MessageAttributes {
		if v.StringValue != nil {
			p.MetaSetMut(k, *v.StringValue)
		}
	}
}

// ReadBatch attempts to read a new message from the target SQS.
func (a *awsSQSReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if a.sqs == nil {
		return nil, nil, service.ErrNotConnected
	}

	var next types.Message
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
