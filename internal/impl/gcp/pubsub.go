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
