package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newRedisListInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name: "redis_list",
		Summary: `
Pops messages from the beginning of a Redis list using the BLPop command.`,
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("key", "The key of a list to read from."),
			docs.FieldString("timeout", "The length of time to poll for new messages before reattempting.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewRedisListConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisListInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	r, err := newRedisListReader(conf.RedisList, log)
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("redis_list", true, input.NewAsyncPreserver(r), log, stats)
}

type redisListReader struct {
	client redis.UniversalClient
	cMut   sync.Mutex

	conf    input.RedisListConfig
	timeout time.Duration

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

	if _, err := clientFromConfig(conf.Config); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisListReader) ConnectWithContext(ctx context.Context) error {
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

func (r *redisListReader) ReadWithContext(ctx context.Context) (*message.Batch, input.AsyncAckFn, error) {
	var client redis.UniversalClient

	r.cMut.Lock()
	client = r.client
	r.cMut.Unlock()

	if client == nil {
		return nil, nil, component.ErrNotConnected
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

	return message.QuickBatch([][]byte{[]byte(res[1])}), func(ctx context.Context, err error) error {
		return nil
	}, nil
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

func (r *redisListReader) CloseAsync() {
	_ = r.disconnect()
}

func (r *redisListReader) WaitForClose(timeout time.Duration) error {
	return nil
}
