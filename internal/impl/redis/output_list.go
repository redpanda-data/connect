package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	loFieldKey      = "key"
	loFieldBatching = "batching"
)

type redisPushCommand string

const (
	rPush redisPushCommand = "rpush"
	lPush redisPushCommand = "lpush"
)

func redisListOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Pushes messages onto the end of a Redis list (which is created if it doesn't already exist) using the RPUSH command.`).
		Description(`The field `+"`key`"+` supports xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions], allowing you to create a unique key for each message.`+service.OutputPerformanceDocs(true, true)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(loFieldKey).
				Description("The key for each message, function interpolations can be optionally used to create a unique key per message.").
				Examples("some_list", "${! @.kafka_key )}", "${! this.doc.id }", "${! count(\"msgs\") }"),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(loFieldBatching),
			service.NewStringEnumField("command", string(rPush), string(lPush)).
				Description("The command used to push elements to the Redis list").
				Default(string(rPush)).
				Advanced().
				Version("4.22.0"),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"redis_list", redisListOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(loFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisListWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type redisListWriter struct {
	log *service.Logger

	key *service.InterpolatedString

	clientCtor   func() (redis.UniversalClient, error)
	client       redis.UniversalClient
	connMut      sync.RWMutex
	clientPush   func(client redis.UniversalClient, ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	pipelinePush func(pipe redis.Pipeliner, ctx context.Context, key string, values ...interface{}) *redis.IntCmd
}

func newRedisListWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisListWriter, err error) {
	r = &redisListWriter{
		log: mgr.Logger(),
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
	}

	if r.key, err = conf.FieldInterpolatedString(loFieldKey); err != nil {
		return
	}

	if _, err := getClient(conf); err != nil {
		return nil, err
	}

	pushCommand, err := conf.FieldString("command")
	if err != nil {
		return nil, err
	}

	switch redisPushCommand(pushCommand) {
	case rPush:
		r.clientPush = func(client redis.UniversalClient, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
			return client.RPush(ctx, key, values)
		}
		r.pipelinePush = func(pipe redis.Pipeliner, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
			return pipe.RPush(ctx, key, values)
		}

	case lPush:
		r.clientPush = func(client redis.UniversalClient, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
			return client.LPush(ctx, key, values)
		}
		r.pipelinePush = func(pipe redis.Pipeliner, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
			return pipe.LPush(ctx, key, values)
		}

	default:
		return nil, fmt.Errorf("invalid redis command: %s", pushCommand)
	}

	return r, nil
}

func (r *redisListWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.clientCtor()
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}

	r.client = client
	return nil
}

func (r *redisListWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	if len(batch) == 1 {
		key, err := r.key.TryString(batch[0])
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}

		mBytes, err := batch[0].AsBytes()
		if err != nil {
			return err
		}

		if err := r.clientPush(client, ctx, key, mBytes).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return service.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()

	for i := 0; i < len(batch); i++ {
		key, err := batch.TryInterpolatedString(i, r.key)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}

		mBytes, err := batch[i].AsBytes()
		if err != nil {
			return err
		}

		_ = r.pipelinePush(pipe, ctx, key, mBytes)
	}

	cmders, err := pipe.Exec(ctx)
	if err != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return service.ErrNotConnected
	}

	var batchErr *service.BatchError
	for i, res := range cmders {
		if res.Err() != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, res.Err())
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
