package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	ibatch "github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newRedisPubSubOutput), docs.ComponentSpec{
		Name: "redis_pubsub",
		Summary: `
Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.`,
		Description: output.Description(true, true, `
This output will interpolate functions within the channel field, you
can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).`),
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("channel", "The channel to publish messages to.").IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewRedisPubSubConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisPubSubOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newRedisPubSubWriter(conf.RedisPubSub, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("redis_pubsub", conf.RedisPubSub.MaxInFlight, w, mgr)
	if err != nil {
		return nil, err
	}
	return batcher.NewFromConfig(conf.RedisPubSub.Batching, a, mgr)
}

type redisPubSubWriter struct {
	mgr bundle.NewManagement
	log log.Modular

	conf       output.RedisPubSubConfig
	channelStr *field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

func newRedisPubSubWriter(conf output.RedisPubSubConfig, mgr bundle.NewManagement) (*redisPubSubWriter, error) {
	r := &redisPubSubWriter{
		mgr:  mgr,
		log:  mgr.Logger(),
		conf: conf,
	}
	var err error
	if r.channelStr, err = mgr.BloblEnvironment().NewField(conf.Channel); err != nil {
		return nil, fmt.Errorf("failed to parse channel expression: %v", err)
	}
	if _, err = clientFromConfig(mgr.FS(), conf.Config); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisPubSubWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := clientFromConfig(r.mgr.FS(), r.conf.Config)
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}

	r.log.Infof("Pushing messages to Redis channel: %v\n", r.conf.Channel)

	r.client = client
	return nil
}

func (r *redisPubSubWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return component.ErrNotConnected
	}

	if msg.Len() == 1 {
		channel, err := r.channelStr.String(0, msg)
		if err != nil {
			return fmt.Errorf("channel interpolation error: %w", err)
		}
		if err := client.Publish(ctx, channel, msg.Get(0).AsBytes()).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return component.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()
	if err := msg.Iter(func(i int, p *message.Part) error {
		channel, err := r.channelStr.String(i, msg)
		if err != nil {
			return fmt.Errorf("channel interpolation error: %w", err)
		}
		_ = pipe.Publish(ctx, channel, p.AsBytes())
		return nil
	}); err != nil {
		return err
	}
	cmders, err := pipe.Exec(ctx)
	if err != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return component.ErrNotConnected
	}

	var batchErr *ibatch.Error
	for i, res := range cmders {
		if res.Err() != nil {
			if batchErr == nil {
				batchErr = ibatch.NewError(msg, res.Err())
			}
			batchErr.Failed(i, res.Err())
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (r *redisPubSubWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisPubSubWriter) Close(context.Context) error {
	return r.disconnect()
}
