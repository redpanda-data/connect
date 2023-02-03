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
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
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

var (
	nameRegexpRaw = `^[a-z0-9]+(_[a-z0-9]+)*$`
	nameRegexp    = regexp.MustCompile(nameRegexpRaw)
)

// NewManagement defines the latest API for a Benthos manager, which will become
// the only API (internally) in Benthos V4.
type NewManagement interface {
	ForStream(id string) NewManagement
	IntoPath(segments ...string) NewManagement
	WithAddedMetrics(m metrics.Type) NewManagement

	Path() []string
	Label() string

	Metrics() metrics.Type
	Logger() log.Modular
	Tracer() trace.TracerProvider
	FS() ifs.FS
	BloblEnvironment() *bloblang.Environment

	RegisterEndpoint(path, desc string, h http.HandlerFunc)

	NewBuffer(conf buffer.Config) (buffer.Streamed, error)
	NewCache(conf cache.Config) (cache.V1, error)
	NewInput(conf input.Config) (input.Streamed, error)
	NewProcessor(conf processor.Config) (processor.V1, error)
	NewOutput(conf output.Config, pipelines ...processor.PipelineConstructorFunc) (output.Streamed, error)
	NewRateLimit(conf ratelimit.Config) (ratelimit.V1, error)

	ProbeCache(name string) bool
	AccessCache(ctx context.Context, name string, fn func(cache.V1)) error
	StoreCache(ctx context.Context, name string, conf cache.Config) error
	RemoveCache(ctx context.Context, name string) error

	ProbeInput(name string) bool
	AccessInput(ctx context.Context, name string, fn func(input.Streamed)) error
	StoreInput(ctx context.Context, name string, conf input.Config) error
	RemoveInput(ctx context.Context, name string) error

	ProbeProcessor(name string) bool
	AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) error
	StoreProcessor(ctx context.Context, name string, conf processor.Config) error
	RemoveProcessor(ctx context.Context, name string) error

	ProbeOutput(name string) bool
	AccessOutput(ctx context.Context, name string, fn func(output.Sync)) error
	StoreOutput(ctx context.Context, name string, conf output.Config) error
	RemoveOutput(ctx context.Context, name string) error

	ProbeRateLimit(name string) bool
	AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) error
	StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error
	RemoveRateLimit(ctx context.Context, name string) error

	GetPipe(name string) (<-chan message.Transaction, error)
	SetPipe(name string, t <-chan message.Transaction)
	UnsetPipe(name string, t <-chan message.Transaction)
}

type componentErr struct {
	typeStr    string
	annotation string
	err        error
}

func (c *componentErr) Error() string {
	return fmt.Sprintf("failed to init %v %v: %v", c.typeStr, c.annotation, c.err)
}

func (c *componentErr) Unwrap() error {
	return c.err
}

func wrapComponentErr(mgr NewManagement, typeStr string, err error) error {
	if err == nil {
		return nil
	}

	var existing *componentErr
	if errors.As(err, &existing) {
		return err
	}

	annotation := "<no label>"
	if mgr.Label() != "" {
		annotation = "'" + mgr.Label() + "'"
	}
	if p := mgr.Path(); len(p) > 0 {
		annotation += " path root."
		annotation += query.SliceToDotPath(mgr.Path()...)
	}
	return &componentErr{
		typeStr:    typeStr,
		annotation: annotation,
		err:        err,
	}
}
