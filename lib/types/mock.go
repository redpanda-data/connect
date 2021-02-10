package types

import (
	"net/http"
)

// DudMgr is a noop implementation of a types.Manager.
type DudMgr struct {
	ID int
}

// RegisterEndpoint is a noop.
func (f DudMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

// GetCache always returns ErrCacheNotFound.
func (f DudMgr) GetCache(name string) (Cache, error) {
	return nil, ErrCacheNotFound
}

// GetCondition always returns ErrConditionNotFound.
func (f DudMgr) GetCondition(name string) (Condition, error) {
	return nil, ErrConditionNotFound
}

// GetRateLimit always returns ErrRateLimitNotFound.
func (f DudMgr) GetRateLimit(name string) (RateLimit, error) {
	return nil, ErrRateLimitNotFound
}

// GetPlugin always returns ErrPluginNotFound.
func (f DudMgr) GetPlugin(name string) (interface{}, error) {
	return nil, ErrPluginNotFound
}

// GetPipe attempts to find a service wide message producer by its name.
func (f DudMgr) GetPipe(name string) (<-chan Transaction, error) {
	return nil, ErrPipeNotFound
}

// SetPipe registers a message producer under a name.
func (f DudMgr) SetPipe(name string, t <-chan Transaction) {}

// UnsetPipe removes a named pipe.
func (f DudMgr) UnsetPipe(name string, t <-chan Transaction) {}

// NoopMgr returns a Manager implementation that does nothing.
func NoopMgr() Manager {
	return DudMgr{}
}

// CacheMgr is a noop implementation of a types.Manager.
type CacheMgr struct {
	ID int
	Caches map[string]Cache
}

// RegisterEndpoint is a noop.
func (f CacheMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}

// GetCache always returns ErrCacheNotFound.
func (f CacheMgr) GetCache(name string) (Cache, error) {
	if result, ok := f.Caches[name]; !ok {
		return nil, ErrCacheNotFound
	} else {
		return result, nil
	}
}

// GetCondition always returns ErrConditionNotFound.
func (f CacheMgr) GetCondition(name string) (Condition, error) {
	return nil, ErrConditionNotFound
}

// GetRateLimit always returns ErrRateLimitNotFound.
func (f CacheMgr) GetRateLimit(name string) (RateLimit, error) {
	return nil, ErrRateLimitNotFound
}

// GetPlugin always returns ErrPluginNotFound.
func (f CacheMgr) GetPlugin(name string) (interface{}, error) {
	return nil, ErrPluginNotFound
}

// GetPipe attempts to find a service wide message producer by its name.
func (f CacheMgr) GetPipe(name string) (<-chan Transaction, error) {
	return nil, ErrPipeNotFound
}

// SetPipe registers a message producer under a name.
func (f CacheMgr) SetPipe(name string, t <-chan Transaction) {}

// UnsetPipe removes a named pipe.
func (f CacheMgr) UnsetPipe(name string, t <-chan Transaction) {}

// NoopMgr returns a Manager implementation that does nothing.
func NoopCacheMgr(caches map[string]Cache) Manager {
	return CacheMgr{
		Caches: caches,
	}
}