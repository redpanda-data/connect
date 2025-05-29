// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dgraph

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dgraph-io/ristretto/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func ristrettoCacheConfig() *service.ConfigSpec {
	retriesDefaults := backoff.NewExponentialBackOff()
	retriesDefaults.InitialInterval = time.Second
	retriesDefaults.MaxInterval = time.Second * 5
	retriesDefaults.MaxElapsedTime = time.Second * 30

	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores key/value pairs in a map held in the memory-bound https://github.com/dgraph-io/ristretto[Ristretto cache^].`).
		Description(`This cache is more efficient and appropriate for high-volume use cases than the standard memory cache. However, the add command is non-atomic, and therefore this cache is not suitable for deduplication.`).
		Field(service.NewDurationField("default_ttl").
			Description("A default TTL to set for items, calculated from the moment the item is cached. Set to an empty string or zero duration to disable TTLs.").
			Default("").
			Example("5m").
			Example("60s")).
		Field(service.NewBackOffToggledField("get_retries", false, retriesDefaults).
			Description("Determines how and whether get attempts should be retried if the key is not found. Ristretto is a concurrent cache that does not immediately reflect writes, and so it can sometimes be useful to enable retries at the cost of speed in cases where the key is expected to exist.").
			Advanced())

	return spec
}

func init() {
	service.MustRegisterCache(
		"ristretto", ristrettoCacheConfig(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.Cache, error) {
			return newRistrettoCacheFromConfig(conf)
		})
}

func newRistrettoCacheFromConfig(conf *service.ParsedConfig) (*ristrettoCache, error) {
	backOff, backOffEnabled, err := conf.FieldBackOffToggled("get_retries")
	if err != nil {
		return nil, err
	}

	var defaultTTL time.Duration
	if testStr, _ := conf.FieldString("default_ttl"); testStr != "" {
		if defaultTTL, err = conf.FieldDuration("default_ttl"); err != nil {
			return nil, err
		}
	}

	return newRistrettoCache(defaultTTL, backOffEnabled, backOff)
}

//------------------------------------------------------------------------------

type ristrettoCache struct {
	defaultTTL time.Duration
	cache      *ristretto.Cache[string, []byte]

	retriesEnabled bool
	boffPool       sync.Pool
	closeOnce      sync.Once
}

func newRistrettoCache(defaultTTL time.Duration, retriesEnabled bool, backOff *backoff.ExponentialBackOff) (*ristrettoCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}
	r := &ristrettoCache{
		defaultTTL:     defaultTTL,
		cache:          cache,
		retriesEnabled: retriesEnabled,
		boffPool: sync.Pool{
			New: func() any {
				bo := *backOff
				bo.Reset()
				return &bo
			},
		},
	}

	return r, nil
}

func (r *ristrettoCache) Get(ctx context.Context, key string) ([]byte, error) {
	var boff backoff.BackOff

	for {
		res, ok := r.cache.Get(key)
		if ok {
			return res, nil
		}

		if r.retriesEnabled {
			if boff == nil {
				boff = r.boffPool.Get().(backoff.BackOff)
				defer func() { //nolint:gocritic
					boff.Reset()
					r.boffPool.Put(boff)
				}()
			}
		} else {
			return nil, service.ErrKeyNotFound
		}

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return nil, service.ErrKeyNotFound
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, service.ErrKeyNotFound
		}
	}
}

func (r *ristrettoCache) Set(_ context.Context, key string, value []byte, ttl *time.Duration) error {
	var t time.Duration
	if ttl != nil {
		t = *ttl
	} else {
		t = r.defaultTTL
	}
	if !r.cache.SetWithTTL(key, value, 1, t) {
		return errors.New("set operation was dropped")
	}
	return nil
}

func (r *ristrettoCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return r.Set(ctx, key, value, ttl)
}

func (r *ristrettoCache) Delete(_ context.Context, key string) error {
	r.cache.Del(key)
	return nil
}

func (r *ristrettoCache) Close(_ context.Context) error {
	r.closeOnce.Do(func() {
		r.cache.Close()
	})
	return nil
}
