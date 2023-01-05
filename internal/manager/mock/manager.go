package mock

import (
	"context"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
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
	lock       sync.Mutex

	// OnRegisterEndpoint can be set in order to intercept endpoints registered
	// by components.
	OnRegisterEndpoint func(path string, h http.HandlerFunc)

	CustomFS ifs.FS
	M        metrics.Type
	L        log.Modular
	T        trace.TracerProvider
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
		CustomFS:   ifs.OS(),
		M:          metrics.Noop(),
		L:          log.Noop(),
		T:          trace.NewNoopTracerProvider(),
	}
}

// ForStream returns the same mock manager.
func (m *Manager) ForStream(id string) bundle.NewManagement { return m }

// IntoPath returns the same mock manager.
func (m *Manager) IntoPath(segments ...string) bundle.NewManagement { return m }

// WithAddedMetrics returns the same mock manager.
func (m *Manager) WithAddedMetrics(m2 metrics.Type) bundle.NewManagement { return m }

// NewBuffer always errors on invalid type.
func (m *Manager) NewBuffer(conf buffer.Config) (buffer.Streamed, error) {
	return nil, component.ErrInvalidType("buffer", conf.Type)
}

// NewCache always errors on invalid type.
func (m *Manager) NewCache(conf cache.Config) (cache.V1, error) {
	return bundle.AllCaches.Init(conf, m)
}

// StoreCache always errors on invalid type.
func (m *Manager) StoreCache(ctx context.Context, name string, conf cache.Config) error {
	return component.ErrInvalidType("cache", conf.Type)
}

// NewInput always errors on invalid type.
func (m *Manager) NewInput(conf input.Config) (input.Streamed, error) {
	return bundle.AllInputs.Init(conf, m)
}

// StoreInput always errors on invalid type.
func (m *Manager) StoreInput(ctx context.Context, name string, conf input.Config) error {
	return component.ErrInvalidType("input", conf.Type)
}

// NewProcessor always errors on invalid type.
func (m *Manager) NewProcessor(conf processor.Config) (processor.V1, error) {
	return bundle.AllProcessors.Init(conf, m)
}

// StoreProcessor always errors on invalid type.
func (m *Manager) StoreProcessor(ctx context.Context, name string, conf processor.Config) error {
	return component.ErrInvalidType("processor", conf.Type)
}

// NewOutput always errors on invalid type.
func (m *Manager) NewOutput(conf output.Config, pipelines ...processor.PipelineConstructorFunc) (output.Streamed, error) {
	return bundle.AllOutputs.Init(conf, m, pipelines...)
}

// StoreOutput always errors on invalid type.
func (m *Manager) StoreOutput(ctx context.Context, name string, conf output.Config) error {
	return component.ErrInvalidType("output", conf.Type)
}

// NewRateLimit always errors on invalid type.
func (m *Manager) NewRateLimit(conf ratelimit.Config) (ratelimit.V1, error) {
	return bundle.AllRateLimits.Init(conf, m)
}

// StoreRateLimit always errors on invalid type.
func (m *Manager) StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error {
	return component.ErrInvalidType("rate_limit", conf.Type)
}

// Path always returns empty.
func (m *Manager) Path() []string { return nil }

// Label always returns empty.
func (m *Manager) Label() string { return "" }

// Metrics returns a no-op metrics.
func (m *Manager) Metrics() metrics.Type { return m.M }

// Logger returns a no-op logger.
func (m *Manager) Logger() log.Modular { return m.L }

// Tracer returns a no-op tracer.
func (m *Manager) Tracer() trace.TracerProvider { return m.T }

// RegisterEndpoint registers a server wide HTTP endpoint.
func (m *Manager) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	if m.OnRegisterEndpoint != nil {
		m.OnRegisterEndpoint(path, h)
	}
}

// FS returns CustomFS, which wraps the os package unless overridden.
func (m *Manager) FS() ifs.FS {
	return m.CustomFS
}

// BloblEnvironment always returns the global environment.
func (m *Manager) BloblEnvironment() *bloblang.Environment {
	return bloblang.GlobalEnvironment()
}

// ProbeCache returns true if a cache resource exists under the provided name.
func (m *Manager) ProbeCache(name string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Caches[name]
	return exists
}

// AccessCache executes a closure on a cache resource.
func (m *Manager) AccessCache(ctx context.Context, name string, fn func(cache.V1)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	values, ok := m.Caches[name]
	if !ok {
		return component.ErrCacheNotFound
	}
	fn(&Cache{Values: values})
	return nil
}

// RemoveCache removes a resource.
func (m *Manager) RemoveCache(ctx context.Context, name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Caches[name]
	if !exists {
		return component.ErrCacheNotFound
	}
	delete(m.Caches, name)
	return nil
}

// ProbeRateLimit returns true if a rate limit resource exists under the
// provided name.
func (m *Manager) ProbeRateLimit(name string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.RateLimits[name]
	return exists
}

// AccessRateLimit executes a closure on a rate limit resource.
func (m *Manager) AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	r, ok := m.RateLimits[name]
	if !ok {
		return component.ErrRateLimitNotFound
	}
	fn(r)
	return nil
}

// RemoveRateLimit removes a resource.
func (m *Manager) RemoveRateLimit(ctx context.Context, name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.RateLimits[name]
	if !exists {
		return component.ErrRateLimitNotFound
	}
	delete(m.RateLimits, name)
	return nil
}

// ProbeInput returns true if an input resource exists under the provided name.
func (m *Manager) ProbeInput(name string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Inputs[name]
	return exists
}

// AccessInput executes a closure on an input resource.
func (m *Manager) AccessInput(ctx context.Context, name string, fn func(input.Streamed)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	i, exists := m.Inputs[name]
	if !exists {
		return component.ErrInputNotFound
	}
	fn(i)
	return nil
}

// RemoveInput removes a resource.
func (m *Manager) RemoveInput(ctx context.Context, name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Inputs[name]
	if !exists {
		return component.ErrInputNotFound
	}
	delete(m.Inputs, name)
	return nil
}

// ProbeProcessor returns true if a processor resource exists under the provided
// name.
func (m *Manager) ProbeProcessor(name string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Processors[name]
	return exists
}

// AccessProcessor executes a closure on a processor resource.
func (m *Manager) AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	p, ok := m.Processors[name]
	if !ok {
		return component.ErrProcessorNotFound
	}
	fn(p)
	return nil
}

// RemoveProcessor removes a resource.
func (m *Manager) RemoveProcessor(ctx context.Context, name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Processors[name]
	if !exists {
		return component.ErrProcessorNotFound
	}
	delete(m.Processors, name)
	return nil
}

// ProbeOutput returns true if an output resource exists under the provided
// name.
func (m *Manager) ProbeOutput(name string) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Outputs[name]
	return exists
}

// AccessOutput executes a closure on an output resource.
func (m *Manager) AccessOutput(ctx context.Context, name string, fn func(output.Sync)) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	o, exists := m.Outputs[name]
	if !exists {
		return component.ErrOutputNotFound
	}
	fn(o)
	return nil
}

// RemoveOutput removes an output resource.
func (m *Manager) RemoveOutput(ctx context.Context, name string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	_, exists := m.Outputs[name]
	if !exists {
		return component.ErrOutputNotFound
	}
	delete(m.Outputs, name)
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
