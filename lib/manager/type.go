package manager

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	imetrics "github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/docs"
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

	// An optional identifier given to a manager that is used by a component and
	// if specified should be added as a label to logs and metrics.
	component string

	apiReg APIReg

	inputs       map[string]types.Input
	caches       map[string]types.Cache
	processors   map[string]types.Processor
	outputs      map[string]types.OutputWriter
	rateLimits   map[string]types.RateLimit
	plugins      map[string]interface{}
	resourceLock *sync.RWMutex

	// Collections of component constructors
	env      *bundle.Environment
	bloblEnv *bloblang.Environment

	logger log.Modular
	stats  *imetrics.Namespaced

	pipes    map[string]<-chan types.Transaction
	pipeLock *sync.RWMutex

	// TODO: V4 Remove this
	conditions map[string]types.Condition
}

// New returns an instance of manager.Type, which can be shared amongst
// components and logical threads of a Benthos service.
func New(conf Config, apiReg APIReg, log log.Modular, stats metrics.Type) (*Type, error) {
	return NewV2(ResourceConfig{Manager: conf}, apiReg, log, stats)
}

// OptFunc is an opt setting for a manager type.
type OptFunc func(*Type)

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

// NewV2 returns an instance of manager.Type, which can be shared amongst
// components and logical threads of a Benthos service.
func NewV2(conf ResourceConfig, apiReg APIReg, log log.Modular, stats metrics.Type, opts ...OptFunc) (*Type, error) {
	t := &Type{
		apiReg: apiReg,

		inputs:       map[string]types.Input{},
		caches:       map[string]types.Cache{},
		processors:   map[string]types.Processor{},
		outputs:      map[string]types.OutputWriter{},
		rateLimits:   map[string]types.RateLimit{},
		plugins:      map[string]interface{}{},
		resourceLock: &sync.RWMutex{},

		// Environment defaults to global (everything that was imported).
		env:      bundle.GlobalEnvironment,
		bloblEnv: bloblang.GlobalEnvironment(),

		logger: log,
		stats:  imetrics.NewNamespaced(stats),

		pipes:    map[string]<-chan types.Transaction{},
		pipeLock: &sync.RWMutex{},

		conditions: map[string]types.Condition{},
	}

	for _, opt := range opts {
		opt(t)
	}

	conf, err := conf.collapsed()
	if err != nil {
		return nil, err
	}

	// Sometimes resources of a type might refer to other resources of the same
	// type. When they are constructed they will check with the manager to
	// ensure the resource they point to is valid, but not keep the reference.
	// Since we cannot guarantee an order of initialisation we create
	// placeholders during construction.
	for k := range conf.Manager.Inputs {
		t.inputs[k] = nil
	}
	for k := range conf.Manager.Caches {
		t.caches[k] = nil
	}
	for k := range conf.Manager.Processors {
		t.processors[k] = nil
	}
	for k := range conf.Manager.Outputs {
		t.outputs[k] = nil
	}
	for k := range conf.Manager.RateLimits {
		t.rateLimits[k] = nil
	}

	for k, conf := range conf.Manager.RateLimits {
		if err := t.StoreRateLimit(context.Background(), k, conf); err != nil {
			return nil, err
		}
	}

	for k, conf := range conf.Manager.Caches {
		if err := t.StoreCache(context.Background(), k, conf); err != nil {
			return nil, err
		}
	}

	// TODO: Prevent recursive processors.
	for k, conf := range conf.Manager.Processors {
		if err := t.StoreProcessor(context.Background(), k, conf); err != nil {
			return nil, err
		}
	}

	for k, conf := range conf.Manager.Inputs {
		if err := t.StoreInput(context.Background(), k, conf); err != nil {
			return nil, err
		}
	}

	for k, conf := range conf.Manager.Outputs {
		if err := t.StoreOutput(context.Background(), k, conf); err != nil {
			return nil, err
		}
	}

	return t, nil
}

//------------------------------------------------------------------------------

// ForStream returns a variant of this manager to be used by a particular stream
// identifer, where APIs registered will be namespaced by that id.
func (t *Type) ForStream(id string) types.Manager {
	return t.forStream(id)
}

func (t *Type) forStream(id string) *Type {
	newT := *t
	newT.stream = id
	newT.logger = t.logger.WithFields(map[string]string{
		"stream": id,
	})
	newT.stats = t.stats.WithPrefix(id)
	return &newT
}

// ForComponent returns a variant of this manager to be used by a particular
// component identifer, where observability components will be automatically
// tagged with the label.
func (t *Type) ForComponent(id string) types.Manager {
	return t.forComponent(id)
}

func (t *Type) forComponent(id string) *Type {
	newT := *t
	newT.component = id
	newT.logger = t.logger.WithFields(map[string]string{
		"component": id,
	})

	statsPrefix := id
	if len(newT.stream) > 0 {
		statsPrefix = newT.stream + "." + statsPrefix
	}
	newT.stats = t.stats.WithPrefix(statsPrefix)
	return &newT
}

// ForChildComponent returns a variant of this manager to be used by a
// particular component identifer, which is a child of the current component,
// where observability components will be automatically tagged with the label.
func (t *Type) ForChildComponent(id string) types.Manager {
	return t.forChildComponent(id)
}

func (t *Type) forChildComponent(id string) *Type {
	newT := *t
	newT.logger = t.logger.NewModule("." + id)

	if len(newT.component) > 0 {
		id = newT.component + "." + id
	}

	statsPrefix := id
	if len(newT.stream) > 0 {
		statsPrefix = newT.stream + "." + statsPrefix
	}

	newT.stats = t.stats.WithPrefix(statsPrefix)
	newT.component = id
	return &newT
}

// Label returns the current component label held by a manager.
func (t *Type) Label() string {
	return t.component
}

//------------------------------------------------------------------------------

// RegisterEndpoint registers a server wide HTTP endpoint.
func (t *Type) RegisterEndpoint(apiPath, desc string, h http.HandlerFunc) {
	if len(t.stream) > 0 {
		apiPath = path.Join("/", t.stream, apiPath)
	}
	if t.apiReg != nil {
		t.apiReg.RegisterEndpoint(apiPath, desc, h)
	}
}

// SetPipe registers a new transaction chan to a named pipe.
func (t *Type) SetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	t.pipes[name] = tran
	t.pipeLock.Unlock()
}

// GetPipe attempts to obtain and return a named output Pipe
func (t *Type) GetPipe(name string) (<-chan types.Transaction, error) {
	t.pipeLock.RLock()
	pipe, exists := t.pipes[name]
	t.pipeLock.RUnlock()
	if exists {
		return pipe, nil
	}
	return nil, types.ErrPipeNotFound
}

// UnsetPipe removes a named pipe transaction chan.
func (t *Type) UnsetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	if otran, exists := t.pipes[name]; exists && otran == tran {
		delete(t.pipes, name)
	}
	t.pipeLock.Unlock()
}

//------------------------------------------------------------------------------

// WithMetricsMapping returns a manager with the stored metrics exporter wrapped
// with a mapping.
func (t *Type) WithMetricsMapping(m *imetrics.Mapping) *Type {
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

func closeWithContext(ctx context.Context, c types.Closable) error {
	c.CloseAsync()
	waitFor := time.Second
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		waitFor = time.Until(deadline)
	}
	err := c.WaitForClose(waitFor)
	for err != nil && !hasDeadline {
		err = c.WaitForClose(time.Second)
	}
	return err
}

//------------------------------------------------------------------------------

// NewBuffer attempts to create a new buffer component from a config.
func (t *Type) NewBuffer(conf buffer.Config) (buffer.Type, error) {
	return t.env.BufferInit(conf, t)
}

//------------------------------------------------------------------------------

// AccessCache attempts to access a cache resource by a unique identifier and
// executes a closure function with the cache as an argument. Returns an error
// if the cache does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessCache(ctx context.Context, name string, fn func(types.Cache)) error {
	// TODO: Eventually use ctx to cancel blocking on the mutex lock. Needs
	// profiling for heavy use within a busy loop.
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	c, ok := t.caches[name]
	if !ok || c == nil {
		return ErrResourceNotFound(name)
	}
	fn(c)
	return nil
}

// NewCache attempts to create a new cache component from a config.
func (t *Type) NewCache(conf cache.Config) (types.Cache, error) {
	mgr := t
	// A configured label overrides any previously set component label.
	if len(conf.Label) > 0 && t.component != conf.Label {
		mgr = t.forComponent(conf.Label)
	}
	return t.env.CacheInit(conf, mgr)
}

// StoreCache attempts to store a new cache resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreCache(ctx context.Context, name string, conf cache.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	c, ok := t.caches[name]
	if ok && c != nil {
		// If a previous resource exists with the same name then we do NOT allow
		// it to be replaced unless it can be successfully closed. This ensures
		// that we do not leak connections.
		if err := closeWithContext(ctx, c); err != nil {
			return err
		}
	}

	newCache, err := t.forComponent("resource.cache." + name).NewCache(conf)
	if err != nil {
		return fmt.Errorf(
			"failed to create cache resource '%v' of type '%v': %w",
			name, conf.Type, err,
		)
	}

	t.caches[name] = newCache
	return nil
}

//------------------------------------------------------------------------------

// AccessInput attempts to access an input resource by a unique identifier and
// executes a closure function with the input as an argument. Returns an error
// if the input does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessInput(ctx context.Context, name string, fn func(types.Input)) error {
	// TODO: Eventually use ctx to cancel blocking on the mutex lock. Needs
	// profiling for heavy use within a busy loop.
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	i, ok := t.inputs[name]
	if !ok || i == nil {
		return ErrResourceNotFound(name)
	}
	fn(i)
	return nil
}

// NewInput attempts to create a new input component from a config.
//
// TODO: V4 Remove the dumb batch field.
func (t *Type) NewInput(conf input.Config, hasBatchProc bool, pipelines ...types.PipelineConstructorFunc) (types.Input, error) {
	mgr := t
	// A configured label overrides any previously set component label.
	if len(conf.Label) > 0 && t.component != conf.Label {
		mgr = t.forComponent(conf.Label)
	}
	return t.env.InputInit(hasBatchProc, conf, mgr, pipelines...)
}

// StoreInput attempts to store a new input resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreInput(ctx context.Context, name string, conf input.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	i, ok := t.inputs[name]
	if ok && i != nil {
		// If a previous resource exists with the same name then we do NOT allow
		// it to be replaced unless it can be successfully closed. This ensures
		// that we do not leak connections.
		if err := closeWithContext(ctx, i); err != nil {
			return err
		}
	}

	if conf.Label != "" && conf.Label != name {
		return fmt.Errorf("label '%v' must be empty or match the resource name '%v'", conf.Label, name)
	}

	newInput, err := t.forComponent("resource.input."+name).NewInput(conf, false)
	if err != nil {
		return fmt.Errorf(
			"failed to create input resource '%v' of type '%v': %w",
			name, conf.Type, err,
		)
	}

	t.inputs[name] = newInput
	return nil
}

//------------------------------------------------------------------------------

// AccessProcessor attempts to access a processor resource by a unique
// identifier and executes a closure function with the processor as an argument.
// Returns an error if the processor does not exist (or is otherwise
// inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessProcessor(ctx context.Context, name string, fn func(types.Processor)) error {
	// TODO: Eventually use ctx to cancel blocking on the mutex lock. Needs
	// profiling for heavy use within a busy loop.
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	p, ok := t.processors[name]
	if !ok || p == nil {
		return ErrResourceNotFound(name)
	}
	fn(p)
	return nil
}

// NewProcessor attempts to create a new processor component from a config.
func (t *Type) NewProcessor(conf processor.Config) (types.Processor, error) {
	mgr := t
	// A configured label overrides any previously set component label.
	if len(conf.Label) > 0 && t.component != conf.Label {
		mgr = t.forComponent(conf.Label)
	}
	return t.env.ProcessorInit(conf, mgr)
}

// StoreProcessor attempts to store a new processor resource. If an existing
// resource has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreProcessor(ctx context.Context, name string, conf processor.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	p, ok := t.processors[name]
	if ok && p != nil {
		// If a previous resource exists with the same name then we do NOT allow
		// it to be replaced unless it can be successfully closed. This ensures
		// that we do not leak connections.
		if err := closeWithContext(ctx, p); err != nil {
			return err
		}
	}

	if conf.Label != "" && conf.Label != name {
		return fmt.Errorf("label '%v' must be empty or match the resource name '%v'", conf.Label, name)
	}

	newProcessor, err := t.forComponent("resource.processor." + name).NewProcessor(conf)
	if err != nil {
		return fmt.Errorf(
			"failed to create processor resource '%v' of type '%v': %w",
			name, conf.Type, err,
		)
	}

	t.processors[name] = newProcessor
	return nil
}

//------------------------------------------------------------------------------

// AccessOutput attempts to access an output resource by a unique identifier and
// executes a closure function with the output as an argument. Returns an error
// if the output does not exist (or is otherwise inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessOutput(ctx context.Context, name string, fn func(types.OutputWriter)) error {
	// TODO: Eventually use ctx to cancel blocking on the mutex lock. Needs
	// profiling for heavy use within a busy loop.
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	o, ok := t.outputs[name]
	if !ok || o == nil {
		return ErrResourceNotFound(name)
	}
	fn(o)
	return nil
}

// NewOutput attempts to create a new output component from a config.
func (t *Type) NewOutput(conf output.Config, pipelines ...types.PipelineConstructorFunc) (types.Output, error) {
	mgr := t
	// A configured label overrides any previously set component label.
	if len(conf.Label) > 0 && t.component != conf.Label {
		mgr = t.forComponent(conf.Label)
	}
	return t.env.OutputInit(conf, mgr, pipelines...)
}

// StoreOutput attempts to store a new output resource. If an existing resource
// has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreOutput(ctx context.Context, name string, conf output.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	o, ok := t.outputs[name]
	if ok && o != nil {
		// If a previous resource exists with the same name then we do NOT allow
		// it to be replaced unless it can be successfully closed. This ensures
		// that we do not leak connections.
		if err := closeWithContext(ctx, o); err != nil {
			return err
		}
	}

	if conf.Label != "" && conf.Label != name {
		return fmt.Errorf("label '%v' must be empty or match the resource name '%v'", conf.Label, name)
	}

	tmpOutput, err := t.forComponent("resource.output." + name).NewOutput(conf)
	if err == nil {
		if t.outputs[name], err = wrapOutput(tmpOutput); err != nil {
			tmpOutput.CloseAsync()
		}
	}
	if err != nil {
		return fmt.Errorf(
			"failed to create output resource '%v' of type '%v': %w",
			name, conf.Type, err,
		)
	}
	return nil
}

//------------------------------------------------------------------------------

// AccessRateLimit attempts to access a rate limit resource by a unique
// identifier and executes a closure function with the rate limit as an
// argument. Returns an error if the rate limit does not exist (or is otherwise
// inaccessible).
//
// During the execution of the provided closure it is guaranteed that the
// resource will not be closed or removed. However, it is possible for the
// resource to be accessed by any number of components in parallel.
func (t *Type) AccessRateLimit(ctx context.Context, name string, fn func(types.RateLimit)) error {
	// TODO: Eventually use ctx to cancel blocking on the mutex lock. Needs
	// profiling for heavy use within a busy loop.
	t.resourceLock.RLock()
	defer t.resourceLock.RUnlock()
	r, ok := t.rateLimits[name]
	if !ok || r == nil {
		return ErrResourceNotFound(name)
	}
	fn(r)
	return nil
}

// NewRateLimit attempts to create a new rate limit component from a config.
func (t *Type) NewRateLimit(conf ratelimit.Config) (types.RateLimit, error) {
	mgr := t
	// A configured label overrides any previously set component label.
	if len(conf.Label) > 0 && t.component != conf.Label {
		mgr = t.forComponent(conf.Label)
	}
	return t.env.RateLimitInit(conf, mgr)
}

// StoreRateLimit attempts to store a new rate limit resource. If an existing
// resource has the same name it is closed and removed _before_ the new one is
// initialized in order to avoid duplicate connections.
func (t *Type) StoreRateLimit(ctx context.Context, name string, conf ratelimit.Config) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	r, ok := t.rateLimits[name]
	if ok && r != nil {
		// If a previous resource exists with the same name then we do NOT allow
		// it to be replaced unless it can be successfully closed. This ensures
		// that we do not leak connections.
		if err := closeWithContext(ctx, r); err != nil {
			return err
		}
	}

	newRateLimit, err := t.forComponent("resource.rate_limit." + name).NewRateLimit(conf)
	if err != nil {
		return fmt.Errorf(
			"failed to create rate limit resource '%v' of type '%v': %w",
			name, conf.Type, err,
		)
	}

	t.rateLimits[name] = newRateLimit
	return nil
}

//------------------------------------------------------------------------------

// CloseAsync triggers the shut down of all resource types that implement the
// lifetime interface types.Closable.
func (t *Type) CloseAsync() {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	for _, c := range t.inputs {
		c.CloseAsync()
	}
	for _, c := range t.caches {
		c.CloseAsync()
	}
	for _, c := range t.conditions {
		if closer, ok := c.(types.Closable); ok {
			closer.CloseAsync()
		}
	}
	for _, p := range t.processors {
		p.CloseAsync()
	}
	for _, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			closer.CloseAsync()
		}
	}
	for _, c := range t.rateLimits {
		c.CloseAsync()
	}
	for _, c := range t.outputs {
		c.CloseAsync()
	}
}

// WaitForClose blocks until either all closable resource types are shut down or
// a timeout occurs.
func (t *Type) WaitForClose(timeout time.Duration) error {
	t.resourceLock.Lock()
	defer t.resourceLock.Unlock()

	timesOut := time.Now().Add(timeout)
	for k, c := range t.inputs {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.inputs, k)
	}
	for k, c := range t.caches {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.caches, k)
	}
	for k, p := range t.processors {
		if err := p.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.processors, k)
	}
	for k, c := range t.rateLimits {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.rateLimits, k)
	}
	for k, c := range t.outputs {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
		delete(t.outputs, k)
	}
	for k, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			if err := closer.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
			}
		}
		delete(t.plugins, k)
	}
	return nil
}

//------------------------------------------------------------------------------

// DEPRECATED
// TODO: V4 Remove this

// SwapMetrics attempts to swap the underlying metrics implementation of a
// manager. This function does nothing if the manager type is not a *Type.
func SwapMetrics(mgr types.Manager, stats metrics.Type) types.Manager {
	if t, ok := mgr.(*Type); ok {
		newMgr := *t
		newMgr.stats = t.stats.WithStats(stats)
		return &newMgr
	}
	return mgr
}

// GetInput attempts to find a service wide input by its name.
func (t *Type) GetInput(name string) (types.Input, error) {
	if c, exists := t.inputs[name]; exists {
		return c, nil
	}
	return nil, types.ErrInputNotFound
}

// GetCache attempts to find a service wide cache by its name.
func (t *Type) GetCache(name string) (types.Cache, error) {
	if c, exists := t.caches[name]; exists {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}

// GetCondition attempts to find a service wide condition by its name.
func (t *Type) GetCondition(name string) (types.Condition, error) {
	if c, exists := t.conditions[name]; exists {
		return c, nil
	}
	return nil, types.ErrConditionNotFound
}

// GetProcessor attempts to find a service wide processor by its name.
func (t *Type) GetProcessor(name string) (types.Processor, error) {
	if p, exists := t.processors[name]; exists {
		return p, nil
	}
	return nil, types.ErrProcessorNotFound
}

// GetRateLimit attempts to find a service wide rate limit by its name.
func (t *Type) GetRateLimit(name string) (types.RateLimit, error) {
	if rl, exists := t.rateLimits[name]; exists {
		return rl, nil
	}
	return nil, types.ErrRateLimitNotFound
}

// GetOutput attempts to find a service wide output by its name.
func (t *Type) GetOutput(name string) (types.OutputWriter, error) {
	if c, exists := t.outputs[name]; exists {
		return c, nil
	}
	return nil, types.ErrOutputNotFound
}

// GetPlugin attempts to find a service wide resource plugin by its name.
func (t *Type) GetPlugin(name string) (interface{}, error) {
	if pl, exists := t.plugins[name]; exists {
		return pl, nil
	}
	return nil, types.ErrPluginNotFound
}
