package gcp

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/mock"
)

type mockPubSubClient struct {
	mock.Mock
}

func (c *mockPubSubClient) Topic(id string, _ *pubsub.PublishSettings) pubsubTopic {
	args := c.Called(id)

	return args.Get(0).(pubsubTopic)
}

type mockTopic struct {
	mock.Mock
}

func (mt *mockTopic) Exists(context.Context) (bool, error) {
	args := mt.Called()
	return args.Bool(0), args.Error(1)
}

func (mt *mockTopic) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	args := mt.Called(string(msg.Data), msg)

	return args.Get(0).(publishResult)
}

func (mt *mockTopic) Stop() {
	mt.Called()
}

type mockPublishResult struct {
	mock.Mock
}

func (m *mockPublishResult) Get(ctx context.Context) (string, error) {
	args := m.Called()

	return args.String(0), args.Error(1)
}
