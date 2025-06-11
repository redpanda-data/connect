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

package gcp

import (
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestPubSubOutput(t *testing.T) {
	ctx := t.Context()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test_${! content().string().split("_").index(0) }
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("Stop").Return().Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(true, nil).Once()
	barTopic.On("Stop").Return().Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()
	client.On("Close").Return(nil).Once()

	fooMsgA := service.NewMessage([]byte("foo_a"))
	fooResA := &mockPublishResult{}
	fooResA.On("Get").Return("foo_a", nil).Once()
	fooTopic.On("Publish", "foo_a", mock.Anything).Return(fooResA).Once()

	fooMsgB := service.NewMessage([]byte("foo_b"))
	fooResB := &mockPublishResult{}
	fooResB.On("Get").Return("foo_b", nil).Once()
	fooTopic.On("Publish", "foo_b", mock.Anything).Return(fooResB).Once()

	barMsg := service.NewMessage([]byte("bar"))
	barRes := &mockPublishResult{}
	barRes.On("Get").Return("bar", nil).Once()
	barTopic.On("Publish", "bar", mock.Anything).Return(barRes).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic, barTopic,
			fooResA, fooResB, barRes,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	err = out.WriteBatch(ctx, service.MessageBatch{fooMsgA, fooMsgB, barMsg})
	require.NoError(t, err, "publish failed")
}

func TestPubSubOutput_MessageAttr(t *testing.T) {
	ctx := t.Context()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test
    ordering_key: '${! content().string() }_${! counter() }'
    metadata:
      exclude_prefixes:
        - drop_
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("EnableOrdering").Return().Once()
	fooTopic.On("Stop").Return().Once()

	fooMsgA := &mockPublishResult{}
	fooMsgA.On("Get").Return("foo", nil).Once()
	fooTopic.On("Publish", "foo", mock.AnythingOfType("*pubsub.Message")).Return(fooMsgA).Once()

	client.On("Topic", "test").Return(fooTopic).Once()
	client.On("Close").Return(nil).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic,
			fooMsgA,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	msg := service.NewMessage([]byte("foo"))
	msg.MetaSet("keep_a", "good stuff")
	msg.MetaSet("drop_b", "oh well")

	err = out.WriteBatch(ctx, service.MessageBatch{msg})
	require.NoError(t, err, "publish failed")

	require.Len(t, fooTopic.Calls, 3)
	require.Equal(t, "Publish", fooTopic.Calls[2].Method)
	require.Len(t, fooTopic.Calls[2].Arguments, 2)
	psmsg := fooTopic.Calls[2].Arguments[1].(*pubsub.Message)
	require.Equal(t, map[string]string{"keep_a": "good stuff"}, psmsg.Attributes)
	require.Equal(t, "foo_1", psmsg.OrderingKey)
}

func TestPubSubOutput_MissingTopic(t *testing.T) {
	ctx := t.Context()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: 'test_${! content().string() }'
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(false, nil).Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(false, errors.New("simulated error")).Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()
	client.On("Close").Return(nil).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(t, client, fooTopic, barTopic)
	})

	var bErr *service.BatchError
	errs := []error{}

	batch := service.MessageBatch{service.NewMessage([]byte("foo"))}
	index := batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, `topic 'test_foo' does not exist`)
	bErr.WalkMessagesIndexedBy(index, func(_ int, _ *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "topic 'test_foo' does not exist")

	bErr = nil
	errs = []error{}

	batch = service.MessageBatch{service.NewMessage([]byte("bar"))}
	index = batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, "failed to validate topic 'test_bar': simulated error")
	bErr.WalkMessagesIndexedBy(index, func(_ int, _ *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "failed to validate topic 'test_bar': simulated error")
}

func TestPubSubOutput_PublishErrors(t *testing.T) {
	ctx := t.Context()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test_${! content().string().split("_").index(0) }
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("Stop").Return().Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(true, nil).Once()
	barTopic.On("Stop").Return().Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()
	client.On("Close").Return(nil).Once()

	fooMsgA := service.NewMessage([]byte("foo_a"))
	fooResA := &mockPublishResult{}
	fooResA.On("Get").Return("", errors.New("simulated foo error")).Once()
	fooTopic.On("Publish", "foo_a", mock.Anything).Return(fooResA).Once()

	fooMsgB := service.NewMessage([]byte("foo_b"))
	fooResB := &mockPublishResult{}
	fooResB.On("Get").Return("foo_b", nil).Once()
	fooTopic.On("Publish", "foo_b", mock.Anything).Return(fooResB).Once()

	barMsg := service.NewMessage([]byte("bar"))
	barRes := &mockPublishResult{}
	barRes.On("Get").Return("", errors.New("simulated bar error")).Once()
	barTopic.On("Publish", "bar", mock.Anything).Return(barRes).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic, barTopic,
			fooResA, fooResB, barRes,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	batch := service.MessageBatch{fooMsgA, fooMsgB, barMsg}
	index := batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.Error(t, err, "did not get expected publish error")

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr, "error is not a batch error")
	require.Equal(t, 2, batchErr.IndexedErrors(), "did not receive expected number of batch errors")

	var errs []string
	batchErr.WalkMessagesIndexedBy(index, func(_ int, _ *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err.Error())
		}
		return true
	})
	require.ElementsMatch(t, []string{"simulated foo error", "simulated bar error"}, errs)
}

func TestPubSubOutput_ValidateTopic(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name            string
		validateTopic   bool
		topicExists     bool
		expectError     bool
		expectPublish   bool
		expectedError   string
		multipleBatches bool // Test if getTopic caches correctly
	}{
		{
			name:          "validate_topic=true, topic exists",
			validateTopic: true,
			topicExists:   true,
			expectError:   false,
			expectPublish: true,
		},
		{
			name:          "validate_topic=true, topic does not exist",
			validateTopic: true,
			topicExists:   false,
			expectError:   true,
			expectPublish: false,
			expectedError: "topic 'test_topic' does not exist",
		},
		{
			name:          "validate_topic=false, topic exists",
			validateTopic: false,
			topicExists:   true, // Should still publish if topic happens to exist
			expectError:   false,
			expectPublish: true,
		},
		{
			name:          "validate_topic=false, topic does not exist",
			validateTopic: false,
			topicExists:   false, // Exists() should not be called
			expectError:   false, // No error, but messages might be lost
			expectPublish: true,  // Publish will be attempted
		},
		{
			name:            "validate_topic=true, topic exists, multiple batches",
			validateTopic:   true,
			topicExists:     true,
			expectError:     false,
			expectPublish:   true,
			multipleBatches: true,
		},
		{
			name:            "validate_topic=false, topic does not exist, multiple batches",
			validateTopic:   false,
			topicExists:     false,
			expectError:     false,
			expectPublish:   true,
			multipleBatches: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configYAML := `
project: sample-project
topic: test_topic
validate_topic: %v
`
			conf, err := newPubSubOutputConfig().ParseYAML(
				fmt.Sprintf(configYAML, tt.validateTopic),
				nil,
			)
			require.NoError(t, err, "bad output config")

			client := &mockPubSubClient{}
			topic := &mockTopic{}

			if tt.validateTopic {
				topic.On("Exists").Return(tt.topicExists, nil).Once()
			}

			if tt.expectPublish {
				if tt.topicExists || !tt.validateTopic { // Publish is called if topic exists OR validation is off
					msgRes := &mockPublishResult{}
					msgRes.On("Get").Return("id", nil) // Don't care about return val for this test
					// Expect Publish to be called once per batch
					timesToCallPublish := 1
					if tt.multipleBatches {
						timesToCallPublish = 2
					}
					topic.On("Publish", mock.Anything, mock.Anything).Return(msgRes).Times(timesToCallPublish)
					topic.On("Stop").Return()
				}
			}

			client.On("Topic", "test_topic").Return(topic).Once()
			// If multiple batches and topic is cached, Topic() is called only once.
			client.On("Close").Return(nil).Once()

			out, err := newPubSubOutput(conf)
			require.NoError(t, err, "failed to create output")
			out.client = client
			defer func() {
				err = out.Close(ctx)
				require.NoError(t, err, "closing output failed")
				// Stop is only called if a topic was successfully obtained and used
				// For multiple batches, Stop is still only called once at Close
				if tt.expectPublish && ((tt.validateTopic && tt.topicExists) || !tt.validateTopic) {
					topic.AssertCalled(t, "Stop")
				}
				mock.AssertExpectationsForObjects(t, client, topic)
			}()

			err = out.Connect(ctx)
			require.NoError(t, err, "connect failed")

			msgBatch := service.MessageBatch{service.NewMessage([]byte("test message"))}

			err = out.WriteBatch(ctx, msgBatch)
			if tt.expectError {
				require.Error(t, err, "expected an error during WriteBatch")
				if tt.expectedError != "" {
					require.ErrorContains(t, err, tt.expectedError)
				}
			} else {
				require.NoError(t, err, "did not expect an error during WriteBatch")
			}

			if tt.multipleBatches {
				// Second batch to test caching of topic
				err = out.WriteBatch(ctx, msgBatch)
				if tt.expectError {
					// If an error was expected, it should happen on the first batch
					// and the topic wouldn't be cached for a second attempt in error cases.
					// However, our test setup for error cases (topic not existing with validate_topic=true)
					// means getTopic itself errors, so subsequent calls to WriteBatch would re-trigger that.
					require.Error(t, err, "expected an error during second WriteBatch")
					if tt.expectedError != "" {
						require.ErrorContains(t, err, tt.expectedError)
					}
				} else {
					require.NoError(t, err, "did not expect an error during second WriteBatch")
				}
			}

			// Assertions for mock calls are handled in Cleanup
		})
	}
}
