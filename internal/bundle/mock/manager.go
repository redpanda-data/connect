package mock

import (
	"context"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/buffer"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	lbuffer "github.com/Jeffail/benthos/v3/lib/buffer"
	lcache "github.com/Jeffail/benthos/v3/lib/cache"
	linput "github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	loutput "github.com/Jeffail/benthos/v3/lib/output"
	lprocessor "github.com/Jeffail/benthos/v3/lib/processor"
	lratelimit "github.com/Jeffail/benthos/v3/lib/ratelimit"
)

// Manager provides a mock benthos manager that components can use to test
// interactions with fake resources.
type Manager struct {
	*mock.Manager
}

// NewManager provides a new mock manager.
func NewManager() *Manager {
	return &Manager{
		Manager: mock.NewManager(),
	}
}

// NewBuffer always errors on invalid type.
func (m *Manager) NewBuffer(conf lbuffer.Config) (buffer.Streamed, error) {
	return nil, component.ErrInvalidBufferType
}

// NewCache always errors on invalid type.
func (m *Manager) NewCache(conf lcache.Config) (cache.V1, error) {
	return nil, component.ErrInvalidCacheType
}

// StoreCache always errors on invalid type.
func (m *Manager) StoreCache(ctx context.Context, name string, conf lcache.Config) error {
	return component.ErrInvalidCacheType
}

// NewInput always errors on invalid type.
func (m *Manager) NewInput(conf linput.Config, pipelines ...processor.PipelineConstructorFunc) (input.Streamed, error) {
	return nil, component.ErrInvalidInputType
}

// StoreInput always errors on invalid type.
func (m *Manager) StoreInput(ctx context.Context, name string, conf linput.Config) error {
	return component.ErrInvalidInputType
}

// NewProcessor always errors on invalid type.
func (m *Manager) NewProcessor(conf lprocessor.Config) (processor.V1, error) {
	return nil, component.ErrInvalidProcessorType
}

// StoreProcessor always errors on invalid type.
func (m *Manager) StoreProcessor(ctx context.Context, name string, conf lprocessor.Config) error {
	return component.ErrInvalidProcessorType
}

// NewOutput always errors on invalid type.
func (m *Manager) NewOutput(conf loutput.Config, pipelines ...processor.PipelineConstructorFunc) (output.Streamed, error) {
	return nil, component.ErrInvalidOutputType
}

// StoreOutput always errors on invalid type.
func (m *Manager) StoreOutput(ctx context.Context, name string, conf loutput.Config) error {
	return component.ErrInvalidOutputType
}

// NewRateLimit always errors on invalid type.
func (m *Manager) NewRateLimit(conf lratelimit.Config) (ratelimit.V1, error) {
	return nil, component.ErrInvalidRateLimitType
}

// StoreRateLimit always errors on invalid type.
func (m *Manager) StoreRateLimit(ctx context.Context, name string, conf lratelimit.Config) error {
	return component.ErrInvalidRateLimitType
}
