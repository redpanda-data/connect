package service_test

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

// LossyCache is a terrible cache example and silently drops items when the
// capacity is reached. It also doesn't respect TTLs.
type LossyCache struct {
	capacity int
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
	if len(l.items) >= l.capacity {
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
	if len(l.items) >= l.capacity {
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
	configSpec := service.NewConfigSpec().
		Summary("Creates a terrible cache with a fixed capacity.").
		Field(service.NewIntField("capacity").Default(100))

	err := service.RegisterCache("lossy", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			capacity, err := conf.FieldInt("capacity")
			if err != nil {
				return nil, err
			}
			return &LossyCache{
				capacity: capacity,
				mDropped: mgr.Metrics().NewCounter("dropped_just_cus"),
				items:    make(map[string][]byte, capacity),
			}, nil
		})
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI(context.Background())
}
