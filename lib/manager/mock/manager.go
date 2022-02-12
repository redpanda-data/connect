package mock

import (
	"context"
	"net/http"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// Manager provides a mock benthos manager that components can use to test
// interactions with fake resources.
type Manager struct {
	Inputs     map[string]*Input
	Caches     map[string]map[string]CacheItem
	RateLimits map[string]RateLimit
	Outputs    map[string]OutputWriter
	Processors map[string]Processor
	Pipes      map[string]<-chan message.Transaction
}

// NewManager provides a new mock manager.
func NewManager() *Manager {
	return &Manager{
		Inputs:     map[string]*Input{},
		Caches:     map[string]map[string]CacheItem{},
		RateLimits: map[string]RateLimit{},
		Outputs:    map[string]OutputWriter{},
		Processors: map[string]Processor{},
		Pipes:      map[string]<-chan message.Transaction{},
	}
}

// ForStream returns the same mock manager.
func (m *Manager) ForStream(id string) interop.Manager { return m }

// ForComponent returns the same mock manager.
func (m *Manager) ForComponent(id string) interop.Manager { return m }

// ForChildComponent returns the same mock manager.
func (m *Manager) ForChildComponent(id string) interop.Manager { return m }

// Label always returns empty.
func (m *Manager) Label() string { return "" }

// Metrics returns a no-op metrics.
func (m *Manager) Metrics() metrics.Type { return metrics.Noop() }

// Logger returns a no-op logger.
func (m *Manager) Logger() log.Modular { return log.Noop() }

// RegisterEndpoint registers a server wide HTTP endpoint.
func (m *Manager) RegisterEndpoint(path, desc string, h http.HandlerFunc) {}

// BloblEnvironment always returns the global environment.
func (m *Manager) BloblEnvironment() *bloblang.Environment {
	return bloblang.GlobalEnvironment()
}

// ProbeCache returns true if a cache resource exists under the provided name.
func (m *Manager) ProbeCache(name string) bool {
	_, exists := m.Caches[name]
	return exists
}

// AccessCache executes a closure on a cache resource.
func (m *Manager) AccessCache(ctx context.Context, name string, fn func(cache.V1)) error {
	values, ok := m.Caches[name]
	if !ok {
		return component.ErrCacheNotFound
	}
	fn(&Cache{Values: values})
	return nil
}

// ProbeRateLimit returns true if a rate limit resource exists under the
// provided name.
func (m *Manager) ProbeRateLimit(name string) bool {
	_, exists := m.RateLimits[name]
	return exists
}

// AccessRateLimit executes a closure on a rate limit resource.
func (m *Manager) AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) error {
	r, ok := m.RateLimits[name]
	if !ok {
		return component.ErrRateLimitNotFound
	}
	fn(r)
	return nil
}

// ProbeInput returns true if an input resource exists under the provided name.
func (m *Manager) ProbeInput(name string) bool {
	_, exists := m.Inputs[name]
	return exists
}

// AccessInput executes a closure on an input resource.
func (m *Manager) AccessInput(ctx context.Context, name string, fn func(input.Streamed)) error {
	i, exists := m.Inputs[name]
	if !exists {
		return component.ErrInputNotFound
	}
	fn(i)
	return nil
}

// ProbeProcessor returns true if a processor resource exists under the provided
// name.
func (m *Manager) ProbeProcessor(name string) bool {
	_, exists := m.Processors[name]
	return exists
}

// AccessProcessor executes a closure on a processor resource.
func (m *Manager) AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) error {
	p, ok := m.Processors[name]
	if !ok {
		return component.ErrProcessorNotFound
	}
	fn(p)
	return nil
}

// ProbeOutput returns true if an output resource exists under the provided
// name.
func (m *Manager) ProbeOutput(name string) bool {
	_, exists := m.Outputs[name]
	return exists
}

// AccessOutput executes a closure on an output resource.
func (m *Manager) AccessOutput(ctx context.Context, name string, fn func(output.Sync)) error {
	o, exists := m.Outputs[name]
	if !exists {
		return component.ErrOutputNotFound
	}
	fn(o)
	return nil
}

// GetPipe attempts to find a service wide transaction chan by its name.
func (m *Manager) GetPipe(name string) (<-chan message.Transaction, error) {
	if p, ok := m.Pipes[name]; ok {
		return p, nil
	}
	return nil, component.ErrPipeNotFound
}

// SetPipe registers a transaction chan under a name.
func (m *Manager) SetPipe(name string, t <-chan message.Transaction) {
	m.Pipes[name] = t
}

// UnsetPipe removes a named transaction chan.
func (m *Manager) UnsetPipe(name string, t <-chan message.Transaction) {
	delete(m.Pipes, name)
}
