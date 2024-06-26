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
)

type pubsubClient interface {
	Topic(id string, settings *pubsub.PublishSettings) pubsubTopic
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

func (ac *airGappedPubsubClient) Topic(id string, settings *pubsub.PublishSettings) pubsubTopic {
	t := ac.c.Topic(id)
	t.PublishSettings = *settings

	return &airGappedTopic{t: t}
}

type airGappedTopic struct {
	t *pubsub.Topic
}

func (at *airGappedTopic) Exists(ctx context.Context) (bool, error) {
	return at.t.Exists(ctx)
}

func (at *airGappedTopic) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return at.t.Publish(ctx, msg)
}

func (at *airGappedTopic) EnableOrdering() {
	at.t.EnableMessageOrdering = true
}

func (at *airGappedTopic) Stop() {
	at.t.Stop()
}
