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
	"fmt"
	"regexp"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	iinput "github.com/benthosdev/benthos/v4/internal/component/input"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/output"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
)

var nameRegexpRaw = `^[a-z0-9]+(_[a-z0-9]+)*$`
var nameRegexp = regexp.MustCompile(nameRegexpRaw)

// NewManagement defines the latest API for a Benthos manager, which will become
// the only API (internally) in Benthos V4.
type NewManagement interface {
	interop.Manager

	NewBuffer(conf buffer.Config) (buffer.Streamed, error)
	NewCache(conf cache.Config) (cache.V1, error)
	NewInput(conf input.Config, pipelines ...iprocessor.PipelineConstructorFunc) (iinput.Streamed, error)
	NewProcessor(conf processor.Config) (iprocessor.V1, error)
	NewOutput(conf output.Config, pipelines ...iprocessor.PipelineConstructorFunc) (ioutput.Streamed, error)
	NewRateLimit(conf ratelimit.Config) (ratelimit.V1, error)

	StoreCache(ctx context.Context, name string, conf cache.Config) error
	StoreInput(ctx context.Context, name string, conf input.Config) error
	StoreProcessor(ctx context.Context, name string, conf processor.Config) error
	StoreOutput(ctx context.Context, name string, conf output.Config) error
	StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error
}

func wrapComponentErr(mgr NewManagement, typeStr string, err error) error {
	if err == nil {
		return nil
	}
	annotation := "<no label>"
	if mgr.Label() != "" {
		annotation = "'" + mgr.Label() + "'"
	}
	if p := mgr.Path(); len(p) > 0 {
		annotation += "path root."
		annotation += query.SliceToDotPath(mgr.Path()...)
	}
	return fmt.Errorf("failed to init %v %v: %w", typeStr, annotation, err)
}
