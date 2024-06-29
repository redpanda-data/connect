package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/cenkalti/backoff/v4"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdCache struct {
	cli    clientv3.Client
	prefix string

	boffPool sync.Pool
}

func etcdCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Summary(`Use etcd as a cache.`)

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}
	spec = spec.
		Field(service.NewStringField("prefix").
			Description("An optional string to prefix item keys with in order to prevent collisions with similar services.").
			Optional().
			Example("prefix-").
			Advanced())

	return spec
}
func newEtcdCache(prefix string, cli clientv3.Client, backOff *backoff.ExponentialBackOff) (*etcdCache, error) {
	return &etcdCache{
		cli:    cli,
		prefix: prefix,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}, nil
}
func newEtcdCacheFromConfig(conf *service.ParsedConfig) (*etcdCache, error) {
	cli, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	var prefix string
	if conf.Contains("prefix") {
		if prefix, err = conf.FieldString("prefix"); err != nil {
			return nil, err
		}
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}
	return newEtcdCache(prefix, *cli, backOff)
}
func init() {
	err := service.RegisterCache(
		"etcd", etcdCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newEtcdCacheFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func (e *etcdCache) Delete(ctx context.Context, key string) error {
	boff := e.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		e.boffPool.Put(boff)
	}()

	if e.prefix != "" {
		key = e.prefix + key
	}

	for {
		_, err := e.cli.KV.Delete(ctx, key)
		if err == nil {
			return nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
	}
}
func (e *etcdCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := e.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		e.boffPool.Put(boff)
	}()

	if e.prefix != "" {
		key = e.prefix + key
	}

	read, _ := e.Get(ctx, key)
	if len(read) != 0 {
		return service.ErrKeyAlreadyExists
	}
	return e.Set(ctx, key, value, nil)
}
func (e *etcdCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := e.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		e.boffPool.Put(boff)
	}()

	if e.prefix != "" {
		key = e.prefix + key
	}

	for {
		_, err := e.cli.KV.Put(ctx, key, string(value))
		if err == nil {
			return nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
	}
}

func (e *etcdCache) Get(ctx context.Context, key string) ([]byte, error) {
	boff := e.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		e.boffPool.Put(boff)
	}()

	if e.prefix != "" {
		key = e.prefix + key
	}

	for {
		res, err := e.cli.KV.Get(ctx, key)

		if len(res.Kvs) == 0 {
			return nil, service.ErrKeyNotFound
		}
		if res.More {
			return nil, errors.New("Multiple keys were found.")
		}
		if errors.Is(err, rpctypes.ErrGRPCKeyNotFound) {
			return nil, service.ErrKeyNotFound
		}
		if err == nil {
			return res.Kvs[0].Value, nil
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return nil, err
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, err
		}
	}
}

func (e *etcdCache) Close(ctx context.Context) error {
	return e.cli.Close()
}
