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

	ibuffer "github.com/Jeffail/benthos/v3/internal/component/buffer"
	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	iinput "github.com/Jeffail/benthos/v3/internal/component/input"
	ioutput "github.com/Jeffail/benthos/v3/internal/component/output"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	iratelimit "github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
)

// NewManagement defines the latest API for a Benthos manager, which will become
// the only API (internally) in Benthos V4.
type NewManagement interface {
	interop.Manager

	NewBuffer(conf buffer.Config) (ibuffer.Streamed, error)
	NewCache(conf cache.Config) (icache.V1, error)
	NewInput(conf input.Config, pipelines ...iprocessor.PipelineConstructorFunc) (iinput.Streamed, error)
	NewProcessor(conf processor.Config) (iprocessor.V1, error)
	NewOutput(conf output.Config, pipelines ...iprocessor.PipelineConstructorFunc) (ioutput.Streamed, error)
	NewRateLimit(conf ratelimit.Config) (iratelimit.V1, error)

	StoreCache(ctx context.Context, name string, conf cache.Config) error
	StoreInput(ctx context.Context, name string, conf input.Config) error
	StoreProcessor(ctx context.Context, name string, conf processor.Config) error
	StoreOutput(ctx context.Context, name string, conf output.Config) error
	StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error
}
