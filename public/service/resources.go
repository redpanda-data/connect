package service

import (
	"context"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/bundle/mock"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
)

// Resources provides access to service-wide resources.
type Resources struct {
	mgr bundle.NewManagement
}

func newResourcesFromManager(nm bundle.NewManagement) *Resources {
	return &Resources{mgr: nm}
}

// MockResources returns an instantiation of a resources struct that provides
// valid but ineffective methods and observability components. This is useful
// for testing components that interact with a resources type but do not
// explicitly need it for testing purposes.
func MockResources() *Resources {
	// This is quite naughty, if we encounter a case where an empty resource
	// config like this could actually return an error then we'd need to change
	// this.
	return newResourcesFromManager(mock.NewManager())
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

// AccessRateLimit attempts to access a rate limit resource by name. This action
// can block if CRUD operations are being actively performed on the resource.
func (r *Resources) AccessRateLimit(ctx context.Context, name string, fn func(r RateLimit)) error {
	return r.mgr.AccessRateLimit(ctx, name, func(r ratelimit.V1) {
		fn(newReverseAirGapRateLimit(r))
	})
}
