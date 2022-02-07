package mock

import (
	"context"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.Manager = &Manager{}

// Manager provides a mock benthos manager that components can use to test
// interactions with fake resources.
type Manager struct {
	Caches     map[string]map[string]string
	RateLimits map[string]func(context.Context) (time.Duration, error)
	Pipes      map[string]<-chan types.Transaction
}

// NewManager provides a new mock manager.
func NewManager() *Manager {
	return &Manager{
		Caches:     map[string]map[string]string{},
		RateLimits: map[string]func(context.Context) (time.Duration, error){},
		Pipes:      map[string]<-chan types.Transaction{},
	}
}

// RegisterEndpoint registers a server wide HTTP endpoint.
func (m *Manager) RegisterEndpoint(path, desc string, h http.HandlerFunc) {}

// GetCache attempts to find a service wide cache by its name.
func (m *Manager) GetCache(name string) (types.Cache, error) {
	values, ok := m.Caches[name]
	if !ok {
		return nil, types.ErrCacheNotFound
	}
	return &Cache{Values: values}, nil
}

// GetRateLimit attempts to find a service wide rate limit by its name.
func (m *Manager) GetRateLimit(name string) (types.RateLimit, error) {
	fn, ok := m.RateLimits[name]
	if !ok {
		return nil, types.ErrRateLimitNotFound
	}
	return RateLimit(fn), nil
}

// GetPipe attempts to find a service wide transaction chan by its name.
func (m *Manager) GetPipe(name string) (<-chan types.Transaction, error) {
	if p, ok := m.Pipes[name]; ok {
		return p, nil
	}
	return nil, types.ErrPipeNotFound
}

// SetPipe registers a transaction chan under a name.
func (m *Manager) SetPipe(name string, t <-chan types.Transaction) {
	m.Pipes[name] = t
}

// UnsetPipe removes a named transaction chan.
func (m *Manager) UnsetPipe(name string, t <-chan types.Transaction) {
	delete(m.Pipes, name)
}
