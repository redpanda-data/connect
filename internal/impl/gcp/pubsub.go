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

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ pubsubClient = (*airGappedPubsubClient)(nil)

type pubsubClient interface {
	Topic(id string, settings *pubsub.PublishSettings) pubsubTopic
	Close() error
}

type pubsubTopic interface {
	Exists(ctx context.Context) (bool, error)
	Publish(ctx context.Context, msg *pubsub.Message) publishResult
	EnableOrdering()
	Stop()
}

type publishResult interface {
	Get(ctx context.Context) (serverID string, err error)
}

type airGappedPubsubClient struct {
	c *pubsub.Client
}

func (ac *airGappedPubsubClient) Close() error {
	return ac.c.Close()
}

func (ac *airGappedPubsubClient) Topic(id string, settings *pubsub.PublishSettings) pubsubTopic {
	p := ac.c.Publisher(id)
	p.PublishSettings = *settings

	return &airGappedTopic{publisher: p, client: ac.c}
}

type airGappedTopic struct {
	publisher *pubsub.Publisher
	client    *pubsub.Client
}

func (at *airGappedTopic) Exists(ctx context.Context) (bool, error) {
	_, err := at.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: at.publisher.String()})
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (at *airGappedTopic) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return at.publisher.Publish(ctx, msg)
}

func (at *airGappedTopic) EnableOrdering() {
	at.publisher.EnableMessageOrdering = true
}

func (at *airGappedTopic) Stop() {
	at.publisher.Stop()
}
