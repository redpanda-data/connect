package manager

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// ErrResourceNotFound represents an error where a named resource could not be
// accessed because it was not found by the manager.
type ErrResourceNotFound string

// Error implements the standard error interface.
func (e ErrResourceNotFound) Error() string {
	return fmt.Sprintf("unable to locate resource: %v", string(e))
}

//------------------------------------------------------------------------------

// APIReg is an interface representing an API builder.
type APIReg interface {
	RegisterEndpoint(path, desc string, h http.HandlerFunc)
}

//------------------------------------------------------------------------------

// Type is an implementation of types.Manager, which is expected by Benthos
// components that need to register service wide behaviours such as HTTP
// endpoints and event listeners, and obtain service wide shared resources such
// as caches and other resources.
type Type struct {
	// An optional identifier given to a manager that is used by a unique stream
	// and if specified should be used as a path prefix for API endpoints, and
	// added as a label to logs and metrics.
	stream string

	// Opt that determines whether HTTP endpoints registered from within a
	// stream should be prefixed with the stream name.
	namespaceStreamEndpoints bool

	// Keeps track of the full configuration path of the component that holds
	// the manager. This value is used only in observability and therefore it
	// is acceptable that this does not fully represent reality.
	componentPath []string

	// Keeps track of the label of the component holding this manager.
	label string

	apiReg APIReg
	fs     ifs.FS

	inputs     *liveResources[*InputWrapper]
	caches     *liveResources[cache.V1]
	processors *liveResources[processor.V1]
	outputs    *liveResources[*outputWrapper]
	rateLimits *liveResources[ratelimit.V1]

	// Collections of component constructors
	env      *bundle.Environment
	bloblEnv *bloblang.Environment

	logger log.Modular
	stats  *metrics.Namespaced
	tracer trace.TracerProvider

	pipes    map[string]<-chan message.Transaction
	pipeLock *sync.RWMutex
}

// OptFunc is an opt setting for a manager type.
type OptFunc func(*Type)

// OptSetAPIReg sets the multiplexer used by components of this manager for
// registering their own HTTP endpoints.
func OptSetAPIReg(r APIReg) OptFunc {
	return func(t *Type) {
		t.apiReg = r
	}
}

// OptSetStreamHTTPNamespacing determines whether HTTP endpoints registered from
// within a stream should be prefixed with the stream name.
func OptSetStreamHTTPNamespacing(enabled bool) OptFunc {
	return func(t *Type) {
		t.namespaceStreamEndpoints = enabled
	}
}

// OptSetLogger sets the logger from which the manager emits log events for
// components.
func OptSetLogger(logger log.Modular) OptFunc {
	return func(t *Type) {
		t.logger = logger
	}
}

// OptSetMetrics sets the metrics exporter from which the manager creates
// metrics for components.
func OptSetMetrics(stats *metrics.Namespaced) OptFunc {
	return func(t *Type) {
		t.stats = stats
	}
}

// OptSetTracer sets the tracer provider from which the manager creates tracing
// spans.
func OptSetTracer(tracer trace.TracerProvider) OptFunc {
	return func(t *Type) {
		t.tracer = tracer
	}
}

// OptSetEnvironment determines the environment from which the manager
// initializes components and resources. This option is for internal use only.
func OptSetEnvironment(e *bundle.Environment) OptFunc {
	return func(t *Type) {
		t.env = e
	}
}

// OptSetBloblangEnvironment determines the environment from which the manager
// parses bloblang functions and methods. This option is for internal use only.
func OptSetBloblangEnvironment(env *bloblang.Environment) OptFunc {
	return func(t *Type) {
		t.bloblEnv = env
	}
}

// OptSetStreamsMode marks the manager as being created for running streams mode
// resources. This ensures that a label "stream" is added to metrics.
func OptSetStreamsMode(b bool) OptFunc {
	return func(t *Type) {
		if b {
			t.stats = t.stats.WithLabels("stream", "")
		}
	}
}

// OptSetFS determines which ifs.FS implementation to use for its filesystem.
// This can be used to override the default os based filesystem implementation.
func OptSetFS(fs ifs.FS) OptFunc {
	return func(t *Type) {
		t.fs = fs
	}
}

// New returns an instance of manager.Type, which can be shared amongst
// components and logical threads of a Benthos service.
func New(conf ResourceConfig, opts ...OptFunc) (*Type, error) {
	t := &Type{
		apiReg:                   mock.NewManager(),
		namespaceStreamEndpoints: true,

		inputs:     newLiveResources[*InputWrapper](),
		caches:     newLiveResources[cache.V1](),
		processors: newLiveResources[processor.V1](),
		outputs:    newLiveResources[*outputWrapper](),
		rateLimits: newLiveResources[ratelimit.V1](),

		// Environment defaults to global (everything that was imported).
		env:      bundle.GlobalEnvironment,
		bloblEnv: bloblang.GlobalEnvironment(),

		logger: log.Noop(),
		stats:  metrics.Noop(),
		tracer: noop.NewTracerProvider(),

		fs: ifs.OS(),

		pipes:    map[string]<-chan message.Transaction{},
		pipeLock: &sync.RWMutex{},
	}

	for _, opt := range opts {
		opt(t)
	}

	seen := map[string]struct{}{}

	checkLabel := func(typeStr, label string) error {
		if label == "" {
			return fmt.Errorf("%v resource has an empty label", typeStr)
		}
		if _, exists := seen[label]; exists {
			return fmt.Errorf("%v resource label '%v' collides with a previously defined resource", typeStr, label)
		}
		seen[label] = struct{}{}
		return nil
	}

	// Sometimes resources of a type might refer to other resources of the same
	// type. When they are constructed they will check with the manager to
	// ensure the resource they point to is valid, but not keep the reference.
	// Since we cannot guarantee an order of initialisation we create
	// placeholders during construction.
	for _, c := range conf.ResourceInputs {
		if err := checkLabel("input", c.Label); err != nil {
			return nil, err
		}
		t.inputs.Add(c.Label, nil)
	}
	for _, c := range conf.ResourceCaches {
		if err := checkLabel("cache", c.Label); err != nil {
			return nil, err
		}
		t.caches.Add(c.Label, nil)
	}
	for _, c := range conf.ResourceProcessors {
		if err := checkLabel("processor", c.Label); err != nil {
			return nil, err
		}
		t.processors.Add(c.Label, nil)
	}
	for _, c := range conf.ResourceOutputs {
		if err := checkLabel("output", c.Label); err != nil {
			return nil, err
		}
		t.outputs.Add(c.Label, nil)
	}
	for _, c := range conf.ResourceRateLimits {
		if err := checkLabel("rate limit", c.Label); err != nil {
			return nil, err
		}
		t.rateLimits.Add(c.Label, nil)
	}

	// Labels validated, begin construction
	for _, conf := range conf.ResourceRateLimits {
		if err := t.StoreRateLimit(context.Background(), conf.Label, conf); err != nil {
			return nil, err
		}
	}

	for _, conf := range conf.ResourceCaches {
		if err := t.StoreCache(context.Background(), conf.Label, conf); err != nil {
			return nil, err
		}
	}

	// TODO: Prevent recursive processors.
	for _, conf := range conf.ResourceProcessors {
		if err := t.StoreProcessor(context.Background(), conf.Label, conf); err != nil {
			return nil, err
		}
	}

	for _, conf := range conf.ResourceInputs {
		if err := t.StoreInput(context.Background(), conf.Label, conf); err != nil {
			return nil, err
		}
	}

	for _, conf := range conf.ResourceOutputs {
		if err := t.StoreOutput(context.Background(), conf.Label, conf); err != nil {
			return nil, err
		}
	}

	return t, nil
}

//------------------------------------------------------------------------------

// ForStream returns a variant of this manager to be used by a particular stream
// identifier, where APIs registered will be namespaced by that id.
func (t *Type) ForStream(id string) bundle.NewManagement {
	return t.forStream(id)
}

func (t *Type) forStream(id string) *Type {
	newT := *t
	newT.stream = id
	newT.logger = t.logger.WithFields(map[string]string{
		"stream": id,
	})
	newT.stats = t.stats.WithLabels("stream", id)
	return &newT
}

func (t *Type) forLabel(name string) *Type {
	newT := *t
	newT.label = name
	newT.logger = t.logger.WithFields(map[string]string{
		"label": name,
	})
	newT.stats = t.stats.WithLabels("label", name)
	return &newT
}

// IntoPath returns a variant of this manager to be used by a particular
// component path, which is a child of the current component, where
// observability components will be automatically tagged with the new path.
func (t *Type) IntoPath(segments ...string) bundle.NewManagement {
	return t.intoPath(segments...)
}

func (t *Type) intoPath(segments ...string) *Type {
	newT := *t
	newComponentPath := make([]string, 0, len(t.componentPath)+len(segments))
	newComponentPath = append(newComponentPath, t.componentPath...)
	newComponentPath = append(newComponentPath, segments...)
	newT.componentPath = newComponentPath

	pathStr := "root." + query.SliceToDotPath(newComponentPath...)
	newT.logger = t.logger.WithFields(map[string]string{
		"path": pathStr,
	})
	newT.stats = t.stats.WithLabels("path", pathStr)
	return &newT
}

// Path returns the current component path held by a manager.
func (t *Type) Path() []string {
	return t.componentPath
}

// Label returns the current component label held by a manager.
func (t *Type) Label() string {
	return t.label
}

// WithAddedMetrics returns a modified version of the manager where metrics are
// registered to both the current metrics target as well as the provided one.
func (t *Type) WithAddedMetrics(m metrics.Type) bundle.NewManagement {
	newT := *t
	newT.stats = newT.stats.WithStats(metrics.Combine(newT.stats.Child(), m))
	return &newT
}

//------------------------------------------------------------------------------

// RegisterEndpoint registers a server wide HTTP endpoint.
func (t *Type) RegisterEndpoint(apiPath, desc string, h http.HandlerFunc) {
	if t.stream != "" && t.namespaceStreamEndpoints {
		apiPath = path.Join("/", t.stream, apiPath)
	}
	if t.apiReg != nil {
		t.apiReg.RegisterEndpoint(apiPath, desc, h)
	}
}

// FS returns an ifs.FS implementation that provides access to a filesystem. By
// default this simply access the os package, with relative paths resolved from
// the directory that the process is running from.
func (t *Type) FS() ifs.FS {
	return t.fs
}

// SetPipe registers a new transaction chan to a named pipe.
func (t *Type) SetPipe(name string, tran <-chan message.Transaction) {
	t.pipeLock.Lock()
	t.pipes[name] = tran
	t.pipeLock.Unlock()
}

// GetPipe attempts to obtain and return a named output Pipe.
func (t *Type) GetPipe(name string) (<-chan message.Transaction, error) {
	t.pipeLock.RLock()
	pipe, exists := t.pipes[name]
	t.pipeLock.RUnlock()
	if exists {
		return pipe, nil
	}
	return nil, component.ErrPipeNotFound
}

// UnsetPipe removes a named pipe transaction chan.
func (t *Type) UnsetPipe(name string, tran <-chan message.Transaction) {
	t.pipeLock.Lock()
	if otran, exists := t.pipes[name]; exists && otran == tran {
		delete(t.pipes, name)
	}
	t.pipeLock.Unlock()
}

//------------------------------------------------------------------------------

// WithMetricsMapping returns a manager with the stored metrics exporter wrapped
// with a mapping.
func (t *Type) WithMetricsMapping(m *metrics.Mapping) *Type {
	newT := *t
	newT.stats = t.stats.WithMapping(m)
	return &newT
}

// Metrics returns an aggregator preset with the current component context.
func (t *Type) Metrics() metrics.Type {
	return t.stats
}

// Logger returns a logger preset with the current component context.
func (t *Type) Logger() log.Modular {
	return t.logger
}

// Tracer returns a tracer provider with the current component context.
func (t *Type) Tracer() trace.TracerProvider {
	return t.tracer
}

// Environment returns a bundle environment used by the manager. This is for
// internal use only.
func (t *Type) Environment() *bundle.Environment {
	return t.env
}

// BloblEnvironment returns a Bloblang environment used by the manager. This is
// for internal use only.
func (t *Type) BloblEnvironment() *bloblang.Environment {
	return t.bloblEnv
}

//------------------------------------------------------------------------------

// GetDocs returns a documentation spec for an implementation of a component.
func (t *Type) GetDocs(name string, ctype docs.Type) (docs.ComponentSpec, bool) {
	return t.env.GetDocs(name, ctype)
}

//------------------------------------------------------------------------------

// NewBuffer attempts to create a new buffer component from a config.
func (t *Type) NewBuffer(conf buffer.Config) (buffer.Streamed, error) {
	// Buffers currently never have a label
	return t.env.BufferInit(conf, t.forLabel(""))
}

//------------------------------------------------------------------------------

// ProbeCache returns true if a cache resource exists under the provided name.
func (t *Type) ProbeCache(name string) bool {
	return t.caches.Probe(name)
}

// AccessCache attempts to access a cache resource by a unique identifier and
// executes a closure function with the cache as an argument. Returns an error
// if the cache does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessCache(ctx context.Context, name string, fn func(cache.V1)) (err error) {
	if rerr := t.caches.RAccess(name, func(t cache.V1) {
		if t == nil {
			err = ErrResourceNotFound(name)
			return
		}
		fn(t)
	}); rerr != nil {
		err = rerr
	}
	return
}

// NewCache attempts to create a new cache component from a config.
func (t *Type) NewCache(conf cache.Config) (cache.V1, error) {
	return t.env.CacheInit(conf, t.forLabel(conf.Label))
}

// StoreCache attempts to store a new cache resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreCache(ctx context.Context, name string, conf cache.Config) error {
	var initErr error
	if err := t.caches.Access(name, true, func(c *cache.V1, set func(*cache.V1)) {
		if c != nil {
			// If a previous resource exists with the same name then we do NOT allow
			// it to be replaced unless it can be successfully closed. This ensures
			// that we do not leak connections.
			if initErr = (*c).Close(ctx); initErr != nil {
				return
			}
		}

		var newCache cache.V1
		if newCache, initErr = t.intoPath("cache_resources").NewCache(conf); initErr != nil {
			return
		}
		set(&newCache)
	}); err != nil {
		return err
	}
	return initErr
}

// RemoveCache attempts to close and remove an existing cache resource.
func (t *Type) RemoveCache(ctx context.Context, name string) error {
	var closeErr error
	if err := t.caches.Access(name, false, func(c *cache.V1, set func(c *cache.V1)) {
		if c == nil {
			return
		}
		if closeErr = (*c).Close(ctx); closeErr != nil {
			return
		}
		set(nil)
	}); err != nil {
		return err
	}
	return closeErr
}

//------------------------------------------------------------------------------

// ProbeInput returns true if an input resource exists under the provided name.
func (t *Type) ProbeInput(name string) bool {
	return t.inputs.Probe(name)
}

// AccessInput attempts to access an input resource by a unique identifier and
// executes a closure function with the input as an argument. Returns an error
// if the input does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessInput(ctx context.Context, name string, fn func(input.Streamed)) (err error) {
	if rerr := t.inputs.RAccess(name, func(t *InputWrapper) {
		if t == nil {
			err = ErrResourceNotFound(name)
			return
		}
		fn(t)
	}); rerr != nil {
		err = rerr
	}
	return
}

// NewInput attempts to create a new input component from a config.
func (t *Type) NewInput(conf input.Config) (input.Streamed, error) {
	return t.env.InputInit(conf, t.forLabel(conf.Label))
}

// StoreInput attempts to store a new input resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreInput(ctx context.Context, name string, conf input.Config) error {
	var initErr error
	if err := t.inputs.Access(name, true, func(i **InputWrapper, set func(**InputWrapper)) {
		if i != nil {
			// If a previous resource exists with the same name then we do NOT allow
			// it to be replaced unless it can be successfully closed. This ensures
			// that we do not leak connections.
			if initErr = (*i).CloseExistingInput(ctx, true); initErr != nil {
				return
			}
		}

		if conf.Label != "" && conf.Label != name {
			initErr = fmt.Errorf("label '%v' must be empty or match the resource name '%v'", conf.Label, name)
			return
		}

		var newInput input.Streamed
		if newInput, initErr = t.intoPath("input_resources").NewInput(conf); initErr != nil {
			return
		}

		if i != nil {
			(*i).SwapInput(newInput)
		} else {
			ni := WrapInput(newInput)
			set(&ni)
		}
	}); err != nil {
		return err
	}
	return initErr
}

// RemoveInput attempts to close and remove an existing input resource.
func (t *Type) RemoveInput(ctx context.Context, name string) error {
	var closeErr error
	if err := t.inputs.Access(name, false, func(i **InputWrapper, set func(i **InputWrapper)) {
		if i == nil {
			return
		}
		if closeErr = (*i).CloseExistingInput(ctx, false); closeErr != nil {
			return
		}
		set(nil)
	}); err != nil {
		return err
	}
	return closeErr
}

//------------------------------------------------------------------------------

// ProbeProcessor returns true if a processor resource exists under the provided
// name.
func (t *Type) ProbeProcessor(name string) bool {
	return t.processors.Probe(name)
}

// AccessProcessor attempts to access a processor resource by a unique
// identifier and executes a closure function with the processor as an argument.
// Returns an error if the processor does not exist (or is otherwise
// inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) (err error) {
	if rerr := t.processors.RAccess(name, func(t processor.V1) {
		if t == nil {
			err = ErrResourceNotFound(name)
			return
		}
		fn(t)
	}); rerr != nil {
		err = rerr
	}
	return
}

// NewProcessor attempts to create a new processor component from a config.
func (t *Type) NewProcessor(conf processor.Config) (processor.V1, error) {
	return t.env.ProcessorInit(conf, t.forLabel(conf.Label))
}

// StoreProcessor attempts to store a new processor resource. If an existing
// resource has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreProcessor(ctx context.Context, name string, conf processor.Config) error {
	var initErr error
	if err := t.processors.Access(name, true, func(p *processor.V1, set func(*processor.V1)) {
		if p != nil {
			// If a previous resource exists with the same name then we do NOT allow
			// it to be replaced unless it can be successfully closed. This ensures
			// that we do not leak connections.
			if initErr = (*p).Close(ctx); initErr != nil {
				return
			}
		}

		var newProc processor.V1
		if newProc, initErr = t.intoPath("processor_resources").NewProcessor(conf); initErr != nil {
			return
		}
		set(&newProc)
	}); err != nil {
		return err
	}
	return initErr
}

// RemoveProcessor attempts to close and remove an existing processor resource.
func (t *Type) RemoveProcessor(ctx context.Context, name string) error {
	var closeErr error
	if err := t.processors.Access(name, false, func(p *processor.V1, set func(p *processor.V1)) {
		if p == nil {
			return
		}
		if closeErr = (*p).Close(ctx); closeErr != nil {
			return
		}
		set(nil)
	}); err != nil {
		return err
	}
	return closeErr
}

//------------------------------------------------------------------------------

// ProbeOutput returns true if an output resource exists under the provided
// name.
func (t *Type) ProbeOutput(name string) bool {
	return t.outputs.Probe(name)
}

// AccessOutput attempts to access an output resource by a unique identifier and
// executes a closure function with the output as an argument. Returns an error
// if the output does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessOutput(ctx context.Context, name string, fn func(output.Sync)) (err error) {
	if rerr := t.outputs.RAccess(name, func(t *outputWrapper) {
		if t == nil {
			err = ErrResourceNotFound(name)
			return
		}
		fn(t)
	}); rerr != nil {
		err = rerr
	}
	return
}

// NewOutput attempts to create a new output component from a config.
func (t *Type) NewOutput(conf output.Config, pipelines ...processor.PipelineConstructorFunc) (output.Streamed, error) {
	return t.env.OutputInit(conf, t.forLabel(conf.Label), pipelines...)
}

// StoreOutput attempts to store a new output resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreOutput(ctx context.Context, name string, conf output.Config) error {
	var initErr error
	if err := t.outputs.Access(name, true, func(o **outputWrapper, set func(**outputWrapper)) {
		if o != nil {
			// If a previous resource exists with the same name then we do NOT allow
			// it to be replaced unless it can be successfully closed. This ensures
			// that we do not leak connections.
			(*o).TriggerStopConsuming()
			if initErr = (*o).WaitForClose(ctx); initErr != nil {
				return
			}
		}

		if conf.Label != "" && conf.Label != name {
			initErr = fmt.Errorf("label '%v' must be empty or match the resource name '%v'", conf.Label, name)
			return
		}

		var newOutput output.Streamed
		if newOutput, initErr = t.intoPath("output_resources").NewOutput(conf); initErr != nil {
			return
		}

		var wrappedOutput *outputWrapper
		if wrappedOutput, initErr = wrapOutput(newOutput); initErr != nil {
			newOutput.TriggerCloseNow()
			return
		}

		set(&wrappedOutput)
	}); err != nil {
		return err
	}
	return initErr
}

// RemoveOutput attempts to close and remove an existing output resource.
func (t *Type) RemoveOutput(ctx context.Context, name string) error {
	var closeErr error
	if err := t.outputs.Access(name, false, func(o **outputWrapper, set func(o **outputWrapper)) {
		if o == nil {
			return
		}

		(*o).TriggerStopConsuming()
		if closeErr = (*o).WaitForClose(ctx); closeErr != nil {
			return
		}
		set(nil)
	}); err != nil {
		return err
	}
	return closeErr
}

//------------------------------------------------------------------------------

// ProbeRateLimit returns true if a rate limit resource exists under the
// provided name.
func (t *Type) ProbeRateLimit(name string) bool {
	return t.rateLimits.Probe(name)
}

// AccessRateLimit attempts to access a rate limit resource by a unique
// identifier and executes a closure function with the rate limit as an
// argument. Returns an error if the rate limit does not exist (or is otherwise
// inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) (err error) {
	if rerr := t.rateLimits.RAccess(name, func(t ratelimit.V1) {
		if t == nil {
			err = ErrResourceNotFound(name)
			return
		}
		fn(t)
	}); rerr != nil {
		err = rerr
	}
	return
}

// NewRateLimit attempts to create a new rate limit component from a config.
func (t *Type) NewRateLimit(conf ratelimit.Config) (ratelimit.V1, error) {
	return t.env.RateLimitInit(conf, t.forLabel(conf.Label))
}

// StoreRateLimit attempts to store a new rate limit resource. If an existing
// resource has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error {
	var initErr error
	if err := t.rateLimits.Access(name, true, func(r *ratelimit.V1, set func(*ratelimit.V1)) {
		if r != nil {
			// If a previous resource exists with the same name then we do NOT allow
			// it to be replaced unless it can be successfully closed. This ensures
			// that we do not leak connections.
			if initErr = (*r).Close(ctx); initErr != nil {
				return
			}
		}

		var newRL ratelimit.V1
		if newRL, initErr = t.intoPath("rate_limit_resources").NewRateLimit(conf); initErr != nil {
			return
		}
		set(&newRL)
	}); err != nil {
		return err
	}
	return initErr
}

// RemoveRateLimit attempts to close and remove an existing rate limit resource.
func (t *Type) RemoveRateLimit(ctx context.Context, name string) error {
	var closeErr error
	if err := t.rateLimits.Access(name, false, func(r *ratelimit.V1, set func(r *ratelimit.V1)) {
		if r == nil {
			return
		}
		if closeErr = (*r).Close(ctx); closeErr != nil {
			return
		}
		set(nil)
	}); err != nil {
		return err
	}
	return closeErr
}

//------------------------------------------------------------------------------

// NewScanner attempts to create a new scanner component from a config.
func (t *Type) NewScanner(conf scanner.Config) (scanner.Creator, error) {
	return t.env.ScannerInit(conf, t)
}

//------------------------------------------------------------------------------

// CloseObservability attempts to clean up observability (metrics, tracing, etc)
// components owned by the manager. This should only be called when the manager
// itself has finished shutting down and when it is the sole owner of the
// observability components.
func (t *Type) CloseObservability(ctx context.Context) error {
	if t.tracer != nil {
		if shutter, ok := t.tracer.(interface {
			Shutdown(context.Context) error
		}); ok {
			_ = shutter.Shutdown(ctx)
		}
	}
	if t.stats != nil {
		if err := t.stats.Close(); err != nil {
			return err
		}
	}
	return nil
}

// TriggerStopConsuming instructs the manager to stop resource inputs and
// outputs from consuming data. This call does not block.
func (t *Type) TriggerStopConsuming() {
	_ = t.inputs.RWalk(func(name string, i *InputWrapper) error {
		i.TriggerStopConsuming()
		return nil
	})
	_ = t.outputs.RWalk(func(name string, o *outputWrapper) error {
		o.TriggerStopConsuming()
		return nil
	})
}

// TriggerCloseNow triggers the absolute shut down of this component but should
// not block the calling goroutine.
func (t *Type) TriggerCloseNow() {
	_ = t.inputs.RWalk(func(name string, i *InputWrapper) error {
		i.TriggerCloseNow()
		return nil
	})
	_ = t.outputs.RWalk(func(name string, o *outputWrapper) error {
		o.TriggerCloseNow()
		return nil
	})
}

// WaitForClose is a blocking call to wait until the component has finished
// shutting down and cleaning up resources.
func (t *Type) WaitForClose(ctx context.Context) error {
	if err := t.inputs.Walk(func(name string, i **InputWrapper, set func(i **InputWrapper)) error {
		if i == nil {
			return nil
		}
		if err := (*i).WaitForClose(ctx); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", name, err)
		}
		set(nil)
		return nil
	}); err != nil {
		return err
	}

	if err := t.caches.Walk(func(name string, c *cache.V1, set func(c *cache.V1)) error {
		if c == nil {
			return nil
		}
		if err := (*c).Close(ctx); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", name, err)
		}
		set(nil)
		return nil
	}); err != nil {
		return err
	}

	if err := t.processors.Walk(func(name string, p *processor.V1, set func(p *processor.V1)) error {
		if p == nil {
			return nil
		}
		if err := (*p).Close(ctx); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", name, err)
		}
		set(nil)
		return nil
	}); err != nil {
		return err
	}

	if err := t.rateLimits.Walk(func(name string, r *ratelimit.V1, set func(r *ratelimit.V1)) error {
		if r == nil {
			return nil
		}
		if err := (*r).Close(ctx); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", name, err)
		}
		set(nil)
		return nil
	}); err != nil {
		return err
	}

	if err := t.outputs.Walk(func(name string, o **outputWrapper, set func(o **outputWrapper)) error {
		if o == nil {
			return nil
		}
		if err := (*o).WaitForClose(ctx); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", name, err)
		}
		set(nil)
		return nil
	}); err != nil {
		return err
	}
	return nil
}
