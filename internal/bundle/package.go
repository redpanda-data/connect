// Package bundle contains singletons referenced throughout the Benthos codebase
// that allow imported components to add their constructors and documentation to
// a service.
//
// Each component type has it's own singleton bundle containing all imported
// implementations of the component, and from this bundle more can be derived
// that modify the components that are available.
package bundle

import (
	"context"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	iratelimit "github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// NewManagement defines the latest API for a Benthos manager, which will become
// the only API (internally) in Benthos V4.
type NewManagement interface {
	types.Manager

	ForStream(id string) types.Manager
	ForComponent(id string) types.Manager
	ForChildComponent(id string) types.Manager
	Label() string

	Metrics() metrics.Type
	Logger() log.Modular
	BloblEnvironment() *bloblang.Environment

	NewBuffer(conf buffer.Config) (buffer.Type, error)
	NewCache(conf cache.Config) (icache.V1, error)
	NewInput(conf input.Config, pipelines ...types.PipelineConstructorFunc) (types.Input, error)
	NewProcessor(conf processor.Config) (iprocessor.V1, error)
	NewOutput(conf output.Config, pipelines ...types.PipelineConstructorFunc) (types.Output, error)
	NewRateLimit(conf ratelimit.Config) (iratelimit.V1, error)

	AccessCache(ctx context.Context, name string, fn func(icache.V1)) error
	StoreCache(ctx context.Context, name string, conf cache.Config) error

	AccessInput(ctx context.Context, name string, fn func(types.Input)) error
	StoreInput(ctx context.Context, name string, conf input.Config) error

	AccessProcessor(ctx context.Context, name string, fn func(iprocessor.V1)) error
	StoreProcessor(ctx context.Context, name string, conf processor.Config) error

	AccessOutput(ctx context.Context, name string, fn func(types.OutputWriter)) error
	StoreOutput(ctx context.Context, name string, conf output.Config) error

	AccessRateLimit(ctx context.Context, name string, fn func(iratelimit.V1)) error
	StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error
}
