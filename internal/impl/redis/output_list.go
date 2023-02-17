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
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newRedisListOutput), docs.ComponentSpec{
		Name: "redis_list",
		Summary: `
Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.`,
		Description: output.Description(true, true, `
The field `+"`key`"+` supports
[interpolation functions](/docs/configuration/interpolation#bloblang-queries), allowing
you to create a unique key for each message.`),
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString(
				"key", "The key for each message, function interpolations can be optionally used to create a unique key per message.",
				"benthos_list", "${!meta(\"kafka_key\")}", "${!json(\"doc.id\")}", "${!count(\"msgs\")}",
			).IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewRedisListConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisListOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newRedisListWriter(conf.RedisList, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("redis_list", conf.RedisList.MaxInFlight, w, mgr)
	if err != nil {
		return nil, err
	}
	return batcher.NewFromConfig(conf.RedisList.Batching, a, mgr)
}

type redisListWriter struct {
	mgr bundle.NewManagement
	log log.Modular

	conf output.RedisListConfig

	keyStr *field.Expression

	client  redis.UniversalClient
	connMut sync.RWMutex
}

func newRedisListWriter(conf output.RedisListConfig, mgr bundle.NewManagement) (*redisListWriter, error) {
	r := &redisListWriter{
		mgr:  mgr,
		log:  mgr.Logger(),
		conf: conf,
	}

	var err error
	if r.keyStr, err = mgr.BloblEnvironment().NewField(conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	if _, err := clientFromConfig(mgr.FS(), conf.Config); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *redisListWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := clientFromConfig(r.mgr.FS(), r.conf.Config)
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}

	r.client = client
	return nil
}

func (r *redisListWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return component.ErrNotConnected
	}

	if msg.Len() == 1 {
		key, err := r.keyStr.String(0, msg)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		if err := client.RPush(ctx, key, msg.Get(0).AsBytes()).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return component.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()
	if err := msg.Iter(func(i int, p *message.Part) error {
		key, err := r.keyStr.String(0, msg)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		_ = pipe.RPush(ctx, key, p.AsBytes())
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

func (r *redisListWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisListWriter) Close(context.Context) error {
	return r.disconnect()
}
