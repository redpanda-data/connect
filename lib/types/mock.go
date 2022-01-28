package types

import (
	"net/http"
)

type dudMgr struct{}

func (f dudMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

func (f dudMgr) GetCache(name string) (Cache, error) {
	return nil, ErrCacheNotFound
}

func (f dudMgr) GetRateLimit(name string) (RateLimit, error) {
	return nil, ErrRateLimitNotFound
}

func (f dudMgr) GetPlugin(name string) (interface{}, error) {
	return nil, ErrPluginNotFound
}

func (f dudMgr) GetPipe(name string) (<-chan Transaction, error) {
	return nil, ErrPipeNotFound
}

func (f dudMgr) SetPipe(name string, t <-chan Transaction) {}

func (f dudMgr) UnsetPipe(name string, t <-chan Transaction) {}

// NoopMgr creates a manager implementation that does nothing.
func NoopMgr() Manager {
	return dudMgr{}
}
