// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockSqsInput struct {
	sqsAPI

	mtx          sync.Mutex
	queueTimeout int32
	messages     []types.Message
	mesTimeouts  map[string]int32
}

func (m *mockSqsInput) do(fn func()) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	fn()
}

func (m *mockSqsInput) TimeoutLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.mtx.Lock()

			for mesID, timeout := range m.mesTimeouts {
				timeout = timeout - 1
				m.mesTimeouts[mesID] = max(timeout, 0)
			}

			m.mtx.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (m *mockSqsInput) ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	messages := make([]types.Message, 0, len(m.messages))

	for _, message := range m.messages {
		if timeout, found := m.mesTimeouts[*message.MessageId]; !found || timeout == 0 {
			messages = append(messages, message)
			m.mesTimeouts[*message.MessageId] = m.queueTimeout
		}
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

func (m *mockSqsInput) ChangeMessageVisibilityBatch(_ context.Context, input *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, entry := range input.Entries {
		if _, found := m.mesTimeouts[*entry.Id]; found {
			m.mesTimeouts[*entry.Id] = entry.VisibilityTimeout
		} else {
			panic("nope")
		}
	}

	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
}

func (m *mockSqsInput) DeleteMessageBatch(_ context.Context, input *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, entry := range input.Entries {
		delete(m.mesTimeouts, *entry.Id)
		m.messages = slices.DeleteFunc(m.messages, func(msg types.Message) bool {
			return *entry.Id == *msg.MessageId
		})
	}

	return &sqs.DeleteMessageBatchOutput{}, nil
}

func TestSQSInput(t *testing.T) {
	tCtx := t.Context()
	defer tCtx.Done()

	messages := []types.Message{
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

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	r, err := newAWSSQSReader(
		sqsiConfig{
			URL:                 "http://foo.example.com",
			WaitTimeSeconds:     0,
			DeleteMessage:       true,
			ResetVisibility:     true,
			MaxNumberOfMessages: 10,
			MaxOutstanding:      100,
			MessageTimeout:      10 * time.Second,
		},
		conf,
		nil,
	)
	require.NoError(t, err)

	mockInput := &mockSqsInput{
		queueTimeout: 10,
		messages:     messages,
		mesTimeouts:  make(map[string]int32, expectedMessages),
	}
	r.sqs = mockInput
	go mockInput.TimeoutLoop(tCtx)

	defer r.closeSignal.TriggerHardStop()
	err = r.Connect(tCtx)
	require.NoError(t, err)

	receivedMessages := make([]sqsMessage, 0, expectedMessages)

	// Check that all messages are received from the reader
	require.Eventually(t, func() bool {
	out:
		for {
			select {
			case mes := <-r.messagesChan:
				receivedMessages = append(receivedMessages, mes)
			default:
				break out
			}
		}
		return len(receivedMessages) == expectedMessages
	}, 30*time.Second, 100*time.Millisecond)

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
		if message.handle != nil {
			r.ackMessagesChan <- message.handle
		}
	}

	require.Eventually(t, func() bool {
		msgsLen := 0
		mockInput.do(func() {
			msgsLen = len(mockInput.messages)
		})
		return msgsLen == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestSQSInputBatchAck(t *testing.T) {
	tCtx := t.Context()
	defer tCtx.Done()

	messages := []types.Message{}
	for i := range 101 {
		messages = append(messages, types.Message{
			Body:          aws.String(fmt.Sprintf("message-%v", i)),
			MessageId:     aws.String(fmt.Sprintf("id-%v", i)),
			ReceiptHandle: aws.String(fmt.Sprintf("h-%v", i)),
		})
	}
	expectedMessages := len(messages)

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	r, err := newAWSSQSReader(
		sqsiConfig{
			URL:                 "http://foo.example.com",
			WaitTimeSeconds:     0,
			DeleteMessage:       true,
			ResetVisibility:     true,
			MaxNumberOfMessages: 10,
			MaxOutstanding:      100,
			MessageTimeout:      10 * time.Second,
		},
		conf,
		nil,
	)
	require.NoError(t, err)

	mockInput := &mockSqsInput{
		queueTimeout: 10,
		messages:     messages,
		mesTimeouts:  make(map[string]int32, expectedMessages),
	}
	r.sqs = mockInput
	go mockInput.TimeoutLoop(tCtx)

	defer r.closeSignal.TriggerHardStop()
	err = r.Connect(tCtx)
	require.NoError(t, err)

	receivedMessageAcks := map[string]service.AckFunc{}

	for _, eMsg := range messages {
		m, aFn, err := r.Read(tCtx)
		require.NoError(t, err)

		mBytes, err := m.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, *eMsg.Body, string(mBytes))
		receivedMessageAcks[string(mBytes)] = aFn
	}

	// Check that messages haven't been deleted from the queue
	mockInput.do(func() {
		require.Len(t, mockInput.messages, expectedMessages)
		require.Len(t, mockInput.mesTimeouts, expectedMessages)
	})

	// Ack all messages as a batch
	for _, aFn := range receivedMessageAcks {
		require.NoError(t, aFn(tCtx, err))
	}

	require.Eventually(t, func() bool {
		msgsLen := 0
		mockInput.do(func() {
			msgsLen = len(mockInput.messages)
		})
		return msgsLen == 0
	}, 5*time.Second, time.Second)
}
