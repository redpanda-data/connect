package aws

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/require"
)

type mockSqsInput struct {
	sqsiface.SQSAPI

	mtx          chan struct{}
	queueTimeout int
	messages     []*sqs.Message
	mesTimeouts  map[string]int
}

func (m *mockSqsInput) do(fn func()) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()
	fn()
}

func (m *mockSqsInput) TimeoutLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			<-m.mtx

			for mesID, timeout := range m.mesTimeouts {
				timeout = timeout - 1
				if timeout > 0 {
					m.mesTimeouts[mesID] = timeout
				} else {
					m.mesTimeouts[mesID] = 0
				}
			}

			m.mtx <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}

func (m *mockSqsInput) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	messages := make([]*sqs.Message, 0, len(m.messages))

	for _, message := range m.messages {
		if timeout, found := m.mesTimeouts[*message.MessageId]; !found || timeout == 0 {
			messages = append(messages, message)
			m.mesTimeouts[*message.MessageId] = m.queueTimeout
		}
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

func (m *mockSqsInput) GetQueueAttributes(input *sqs.GetQueueAttributesInput) (*sqs.GetQueueAttributesOutput, error) {
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]*string{sqsiAttributeNameVisibilityTimeout: aws.String(strconv.Itoa(m.queueTimeout))}}, nil
}

func (m *mockSqsInput) ChangeMessageVisibilityBatchWithContext(ctx aws.Context, input *sqs.ChangeMessageVisibilityBatchInput, opts ...request.Option) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	for _, entry := range input.Entries {
		if _, found := m.mesTimeouts[*entry.Id]; found {
			m.mesTimeouts[*entry.Id] = int(*entry.VisibilityTimeout)
		}
	}

	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
}

func (m *mockSqsInput) DeleteMessageBatchWithContext(ctx aws.Context, input *sqs.DeleteMessageBatchInput, opts ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
	<-m.mtx
	defer func() { m.mtx <- struct{}{} }()

	for _, entry := range input.Entries {
		delete(m.mesTimeouts, *entry.Id)
		for i, message := range m.messages {
			if *entry.Id == *message.MessageId {
				m.messages = append(m.messages[:i], m.messages[i+1:]...)
			}
		}
	}

	return &sqs.DeleteMessageBatchOutput{}, nil
}

func TestSQSInput(t *testing.T) {
	tCtx := context.Background()
	defer tCtx.Done()

	messages := []*sqs.Message{
		{
			Body:          aws.String("message-1"),
			MessageId:     aws.String("message-1"),
			ReceiptHandle: aws.String("message-1"),
		},
		{
			Body:          aws.String("message-2"),
			MessageId:     aws.String("message-2"),
			ReceiptHandle: aws.String("message-2"),
		},
		{
			Body:          aws.String("message-3"),
			MessageId:     aws.String("message-3"),
			ReceiptHandle: aws.String("message-3"),
		},
	}
	expectedMessages := len(messages)

	r, err := newAWSSQSReader(
		sqsiConfig{
			URL:                 "http://foo.example.com",
			WaitTimeSeconds:     0,
			DeleteMessage:       true,
			ResetVisibility:     true,
			MaxNumberOfMessages: 10,
		},
		session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		nil,
	)
	require.NoError(t, err)

	mockInput := &mockSqsInput{
		mtx:          make(chan struct{}, 1),
		queueTimeout: 10,
		messages:     messages,
		mesTimeouts:  make(map[string]int, expectedMessages),
	}
	mockInput.mtx <- struct{}{}
	r.sqs = mockInput
	go mockInput.TimeoutLoop(tCtx)

	defer r.closeSignal.CloseNow()
	err = r.Connect(tCtx)
	require.NoError(t, err)

	receivedMessages := make([]sqs.Message, 0, expectedMessages)

	// Check that all messages are received from the reader
	require.Eventually(t, func() bool {
	out:
		for {
			select {
			case mes := <-r.messagesChan:
				receivedMessages = append(receivedMessages, *mes)
			default:
				break out
			}
		}
		return len(receivedMessages) == expectedMessages
	}, 30*time.Second, time.Second)

	// Wait over the defined queue timeout and check that messages have not been received again
	time.Sleep(time.Duration(mockInput.queueTimeout+5) * time.Second)
	select {
	case <-r.messagesChan:
		require.Fail(t, "messages have been received again due to timeouts")
	default:
	}
	// Check that even if they are not visible, messages haven't been deleted from the queue
	mockInput.do(func() {
		require.Len(t, mockInput.messages, expectedMessages)
		require.Len(t, mockInput.mesTimeouts, expectedMessages)
	})

	// Ack all messages and ensure that they are deleted from SQS
	for _, message := range receivedMessages {
		r.ackMessagesChan <- sqsMessageHandle{id: *message.MessageId, receiptHandle: *message.ReceiptHandle}
	}

	require.Eventually(t, func() bool {
		msgsLen := 0
		mockInput.do(func() {
			msgsLen = len(mockInput.messages)
		})
		return msgsLen == 0
	}, 5*time.Second, time.Second)
}
