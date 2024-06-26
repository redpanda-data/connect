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
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/mock"
)

type mockPubSubClient struct {
	mock.Mock
}

var _ pubsubClient = &mockPubSubClient{}

func (c *mockPubSubClient) Topic(id string, settings *pubsub.PublishSettings) pubsubTopic {
	args := c.Called(id)

	return args.Get(0).(pubsubTopic)
}

type mockTopic struct {
	mock.Mock
}

var _ pubsubTopic = &mockTopic{}

func (mt *mockTopic) Exists(context.Context) (bool, error) {
	args := mt.Called()
	return args.Bool(0), args.Error(1)
}

func (mt *mockTopic) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	args := mt.Called(string(msg.Data), msg)

	return args.Get(0).(publishResult)
}

func (mt *mockTopic) EnableOrdering() {
	mt.Called()
}

func (mt *mockTopic) Stop() {
	mt.Called()
}

type mockPublishResult struct {
	mock.Mock
}

var _ publishResult = &mockPublishResult{}

func (m *mockPublishResult) Get(ctx context.Context) (string, error) {
	args := m.Called()

	return args.String(0), args.Error(1)
}
