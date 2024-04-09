package gcp

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestPubSubOutput(t *testing.T) {
	ctx := context.Background()

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
	ctx := context.Background()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test
    ordering_key: '${! content().string() }_${! count(content().string()) }'
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
	ctx := context.Background()

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
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
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
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "failed to validate topic 'test_bar': simulated error")
}

func TestPubSubOutput_PublishErrors(t *testing.T) {
	ctx := context.Background()

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
	batchErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err.Error())
		}
		return true
	})
	require.ElementsMatch(t, []string{"simulated foo error", "simulated bar error"}, errs)
}
