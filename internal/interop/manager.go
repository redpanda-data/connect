package interop

import (
	"context"
	"net/http"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

// Manager is an interface expected by Benthos components that allows them to
// register their service wide behaviours such as HTTP endpoints and event
// listeners, and obtain service wide shared resources such as caches.
type Manager interface {
	ForStream(id string) Manager
	IntoPath(segments ...string) Manager
	WithAddedMetrics(m metrics.Type) Manager

	Path() []string
	Label() string

	Metrics() metrics.Type
	Logger() log.Modular
	BloblEnvironment() *bloblang.Environment

	RegisterEndpoint(path, desc string, h http.HandlerFunc)

	// NewBuffer(conf buffer.Config) (buffer.Streamed, error)
	// NewCache(conf cache.Config) (cache.V1, error)
	// NewInput(conf input.Config, pipelines ...processor.PipelineConstructorFunc) (input.Streamed, error)
	// NewProcessor(conf processor.Config) (processor.V1, error)
	// NewOutput(conf output.Config, pipelines ...processor.PipelineConstructorFunc) (output.Streamed, error)
	// NewRateLimit(conf ratelimit.Config) (ratelimit.V1, error)

	ProbeCache(name string) bool
	AccessCache(ctx context.Context, name string, fn func(cache.V1)) error
	// StoreCache(ctx context.Context, name string, conf cache.Config) error

	ProbeInput(name string) bool
	AccessInput(ctx context.Context, name string, fn func(input.Streamed)) error
	// StoreInput(ctx context.Context, name string, conf input.Config) error

	ProbeProcessor(name string) bool
	AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) error
	// StoreProcessor(ctx context.Context, name string, conf processor.Config) error

	ProbeOutput(name string) bool
	AccessOutput(ctx context.Context, name string, fn func(output.Sync)) error
	// StoreOutput(ctx context.Context, name string, conf output.Config) error

	ProbeRateLimit(name string) bool
	AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) error
	// StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error

	GetPipe(name string) (<-chan message.Transaction, error)
	SetPipe(name string, t <-chan message.Transaction)
	UnsetPipe(name string, t <-chan message.Transaction)
}
