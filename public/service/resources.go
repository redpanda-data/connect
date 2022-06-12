package service

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

// Resources provides access to service-wide resources.
type Resources struct {
	mgr bundle.NewManagement
}

func newResourcesFromManager(nm bundle.NewManagement) *Resources {
	return &Resources{mgr: nm}
}

// MockResources returns an instantiation of a resources struct that provides
// valid but ineffective methods and observability components. It is possible to
// instantiate this with mocked (in-memory) cache and rate limit types for
// testing purposed.
func MockResources(opts ...MockResourcesOptFn) *Resources {
	m := mock.NewManager()
	for _, o := range opts {
		o(m)
	}
	return newResourcesFromManager(m)
}

// MockResourcesOptFn provides a func based optional argument to MockResources.
type MockResourcesOptFn func(*mock.Manager)

// MockResourcesOptAddCache instantiates the resources type with a mock cache
// with a given name. Cached items are held in memory.
func MockResourcesOptAddCache(name string) MockResourcesOptFn {
	return func(m *mock.Manager) {
		m.Caches[name] = map[string]mock.CacheItem{}
	}
}

// MockResourcesOptAddRateLimit instantiates the resources type with a mock rate
// limit with a given name, the provided closure will be called for each
// invocation of the rate limit.
func MockResourcesOptAddRateLimit(name string, fn func(context.Context) (time.Duration, error)) MockResourcesOptFn {
	return func(m *mock.Manager) {
		m.RateLimits[name] = mock.RateLimit(fn)
	}
}

// Label returns a label that identifies the component instantiation. This could
// be an explicit label set in config, or is otherwise a generated label based
// on the position of the component within a config.
func (r *Resources) Label() string {
	return r.mgr.Label()
}

// Logger returns a logger preset with context about the component the resources
// were provided to.
func (r *Resources) Logger() *Logger {
	return newReverseAirGapLogger(r.mgr.Logger())
}

// Metrics returns a mechanism for creating custom metrics.
func (r *Resources) Metrics() *Metrics {
	return newReverseAirGapMetrics(r.mgr.Metrics())
}

// AccessCache attempts to access a cache resource by name. This action can
// block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessCache(ctx context.Context, name string, fn func(c Cache)) error {
	return r.mgr.AccessCache(ctx, name, func(c cache.V1) {
		fn(newReverseAirGapCache(c))
	})
}

// HasCache confirms whether a cache with a given name has been registered as a
// resource. This method is useful during component initialisation as it is
// defensive against ordering.
func (r *Resources) HasCache(name string) bool {
	return r.mgr.ProbeCache(name)
}

// AccessRateLimit attempts to access a rate limit resource by name. This action
// can block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessRateLimit(ctx context.Context, name string, fn func(r RateLimit)) error {
	return r.mgr.AccessRateLimit(ctx, name, func(r ratelimit.V1) {
		fn(newReverseAirGapRateLimit(r))
	})
}

// HasRateLimit confirms whether a rate limit with a given name has been
// registered as a resource. This method is useful during component
// initialisation as it is defensive against ordering.
func (r *Resources) HasRateLimit(name string) bool {
	return r.mgr.ProbeRateLimit(name)
}
