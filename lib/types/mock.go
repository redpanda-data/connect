package types

import (
	"net/http"

	"github.com/Jeffail/benthos/v3/internal/component"
)

type dudMgr struct{}

func (f dudMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

func (f dudMgr) GetCache(name string) (Cache, error) {
	return nil, component.ErrCacheNotFound
}

func (f dudMgr) GetRateLimit(name string) (RateLimit, error) {
	return nil, component.ErrRateLimitNotFound
}

func (f dudMgr) GetPipe(name string) (<-chan Transaction, error) {
	return nil, component.ErrPipeNotFound
}

func (f dudMgr) SetPipe(name string, t <-chan Transaction) {}

func (f dudMgr) UnsetPipe(name string, t <-chan Transaction) {}

// NoopMgr creates a manager implementation that does nothing.
func NoopMgr() Manager {
	return dudMgr{}
}
