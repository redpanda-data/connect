package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/go-redis/redis/v7"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newRedisListInput), docs.ComponentSpec{
		Name: "redis_list",
		Summary: `
Pops messages from the beginning of a Redis list using the BLPop command.`,
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("key", "The key of a list to read from."),
			docs.FieldString("timeout", "The length of time to poll for new messages before reattempting.").Advanced(),
			docs.FieldInt("checkpoint_limit", "Sets a limit on the number of messages that can be in either a prefetched or in-processing state. Default value implies no limit. Notice that that there are caveats to imposing this limit. If you have a batch policy at the output level, then messages won't be acked until the batch is flushed. If you don't allow the input to consume enough messages to trigger the batch, then it will stall.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewRedisListConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisListInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	r, err := newRedisListReader(conf.RedisList, mgr.Logger())
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("redis_list", true, input.NewAsyncPreserver(r), mgr)
}

type redisListReader struct {
	client redis.UniversalClient
	cMut   sync.Mutex

	conf            input.RedisListConfig
	timeout         time.Duration
	checkpointLimit int

	ackSema *semaphore.Weighted

	log log.Modular
}

func newRedisListReader(conf input.RedisListConfig, log log.Modular) (*redisListReader, error) {
	r := &redisListReader{
		conf: conf,
		log:  log,
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if r.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	r.checkpointLimit = conf.CheckpointLimit
	r.ackSema = semaphore.NewWeighted(int64(r.checkpointLimit))

	if _, err := clientFromConfig(conf.Config); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisListReader) Connect(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := clientFromConfig(r.conf.Config)
	if err == nil {
		_, err = client.Ping().Result()
	}
	if err != nil {
		return err
	}

	r.log.Infof("Receiving messages from Redis list: %v\n", r.conf.Key)

	r.client = client
	return nil
}

func (r *redisListReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	var client redis.UniversalClient

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return nil, nil, component.ErrNotConnected
	}

	ackFn := func(ctx context.Context, err error) error { return nil }
	if r.checkpointLimit > 0 {
		if err := r.ackSema.Acquire(ctx, 1); err != nil {
			return nil, nil, fmt.Errorf("could not acquire ack semaphore: %v", err)
		}
		ackFn = func(ctx context.Context, err error) error { r.ackSema.Release(1); return nil }
	}

	res, err := client.BLPop(r.timeout, r.conf.Key).Result()

	if err != nil && err != redis.Nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return nil, nil, component.ErrNotConnected
	}

	if len(res) < 2 {
		return nil, nil, component.ErrTimeout
	}

	return message.QuickBatch([][]byte{[]byte(res[1])}), ackFn, nil
}

func (r *redisListReader) disconnect() error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

func (r *redisListReader) Close(ctx context.Context) (err error) {
	err = r.disconnect()
	return
}
