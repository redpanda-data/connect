package memcached

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/public/service"
)

func memcachedConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Second
	retriesDefaults.MaxInterval = time.Second * 5
	retriesDefaults.MaxElapsedTime = time.Second * 30

	spec := service.NewConfigSpec().
		Stable().
		Summary(`Connects to a cluster of memcached services, a prefix can be specified to allow multiple cache types to share a memcached cluster under different namespaces.`).
		Field(service.NewStringListField("addresses").
			Description("A list of addresses of memcached servers to use.")).
		Field(service.NewStringField("prefix").
			Description("An optional string to prefix item keys with in order to prevent collisions with similar services.").
			Optional()).
		Field(service.NewDurationField("default_ttl").
			Description("A default TTL to set for items, calculated from the moment the item is cached.").
			Default("300s")).
		Field(service.NewBackOffField("retries", false, retriesDefaults).
			Advanced())

	return spec
}

func init() {
	err := service.RegisterCache(
		"memcached", memcachedConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newMemcachedFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
}

func newMemcachedFromConfig(conf *service.ParsedConfig) (*memcachedCache, error) {
	addresses, err := conf.FieldStringList("addresses")
	if err != nil {
		return nil, err
	}

	var prefix string
	if conf.Contains("prefix") {
		if prefix, err = conf.FieldString("prefix"); err != nil {
			return nil, err
		}
	}

	ttl, err := conf.FieldDuration("default_ttl")
	if err != nil {
		return nil, err
	}

	backOff, err := conf.FieldBackOff("retries")
	if err != nil {
		return nil, err
	}
	return newMemcachedCache(addresses, prefix, ttl, backOff)
}

//------------------------------------------------------------------------------

type memcachedCache struct {
	prefix     string
	defaultTTL time.Duration

	mc       *memcache.Client
	boffPool sync.Pool
}

func newMemcachedCache(
	inAddresses []string,
	prefix string,
	defaultTTL time.Duration,
	backOff *backoff.ExponentialBackOff,
) (*memcachedCache, error) {
	addresses := []string{}
	for _, addr := range inAddresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				addresses = append(addresses, splitAddr)
			}
		}
	}
	return &memcachedCache{
		mc:         memcache.New(addresses...),
		prefix:     prefix,
		defaultTTL: defaultTTL,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}, nil
}

func (m *memcachedCache) getItemFor(key string, value []byte, ttl *time.Duration) *memcache.Item {
	var expiration int32
	if ttl != nil {
		expiration = int32(ttl.Milliseconds() / 1000)
	} else {
		expiration = int32(m.defaultTTL.Milliseconds() / 1000)
	}
	return &memcache.Item{
		Key:        m.prefix + key,
		Value:      value,
		Expiration: expiration,
	}
}

func (m *memcachedCache) Get(ctx context.Context, key string) ([]byte, error) {
	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	for {
		item, err := m.mc.Get(m.prefix + key)
		if err == nil {
			return item.Value, nil
		}
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, service.ErrKeyNotFound
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

func (m *memcachedCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	for {
		err := m.mc.Set(m.getItemFor(key, value, ttl))
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

// AddWithTTL attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists or if the operation fails.
func (m *memcachedCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	for {
		err := m.mc.Add(m.getItemFor(key, value, ttl))
		if err == nil {
			return nil
		}
		if errors.Is(err, memcache.ErrNotStored) {
			return service.ErrKeyAlreadyExists
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

// Delete attempts to remove a key.
func (m *memcachedCache) Delete(ctx context.Context, key string) error {
	boff := m.boffPool.Get().(backoff.BackOff)
	defer func() {
		boff.Reset()
		m.boffPool.Put(boff)
	}()

	for {
		err := m.mc.Delete(m.prefix + key)
		if errors.Is(err, memcache.ErrCacheMiss) {
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

func (m *memcachedCache) Close(ctx context.Context) error {
	return nil
}
