package service_test

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/public/x/service"
)

// LossyCache is a terrible cache example and silently drops items when the
// capacity is reached. It also doesn't respect TTLs.
type LossyCache struct {
	Capacity int `yaml:"capacity"`

	mDropped *service.MetricCounter
	items    map[string][]byte
}

func (l *LossyCache) Get(ctx context.Context, key string) ([]byte, error) {
	if b, ok := l.items[key]; ok {
		return b, nil
	}
	return nil, service.ErrKeyNotFound
}

func (l *LossyCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if len(l.items) >= l.Capacity {
		// Dropped, whoopsie!
		l.mDropped.Incr(1)
		return nil
	}
	l.items[key] = value
	return nil
}

func (l *LossyCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if _, exists := l.items[key]; exists {
		return service.ErrKeyAlreadyExists
	}
	if len(l.items) >= l.Capacity {
		// Dropped, whoopsie!
		l.mDropped.Incr(1)
		return nil
	}
	l.items[key] = value
	return nil
}

func (l *LossyCache) Delete(ctx context.Context, key string) error {
	delete(l.items, key)
	return nil
}

func (l *LossyCache) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create a cache plugin, where the
// implementation of the cache (type `LossyCache`) also contains fields that
// should be parsed within the Benthos config.
func Example_cachePlugin() {
	configSpec, err := service.NewStructConfigSpec(func() interface{} {
		return &LossyCache{
			Capacity: 100,
			items:    map[string][]byte{},
		}
	})
	if err != nil {
		panic(err)
	}

	err = service.RegisterCache("lossy", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			c := conf.AsStruct().(*LossyCache)
			c.mDropped = mgr.Metrics().NewCounter("dropped.just.cus")
			return c, nil
		})
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI(context.Background())
}
