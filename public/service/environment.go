package service

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	ibloblang "github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	iprocessors "github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	oprocessors "github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// Environment is a collection of Benthos component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This can be useful for sandboxing, testing, etc, but most plugin
// authors do not need to create an Environment and can simply use the global
// environment.
type Environment struct {
	internal    *bundle.Environment
	bloblangEnv *bloblang.Environment
	fs          ifs.FS
}

var globalEnvironment = &Environment{
	internal:    bundle.GlobalEnvironment,
	bloblangEnv: bloblang.GlobalEnvironment(),
	fs:          ifs.OS(),
}

// GlobalEnvironment returns a reference to the global environment, adding
// plugins to this environment is the equivalent to adding plugins using global
// Functions.
func GlobalEnvironment() *Environment {
	return globalEnvironment
}

// NewEnvironment creates a new environment that inherits all globally defined
// plugins, but can have plugins defined on it that are isolated.
func NewEnvironment() *Environment {
	return globalEnvironment.Clone()
}

// Clone an environment, creating a new environment containing the same plugins
// that can be modified independently of the source.
func (e *Environment) Clone() *Environment {
	return &Environment{
		internal:    e.internal.Clone(),
		bloblangEnv: e.bloblangEnv.WithoutFunctions().WithoutMethods(),
		fs:          e.fs,
	}
}

// UseBloblangEnvironment configures the service environment to restrict
// components constructed with it to a specific Bloblang environment.
func (e *Environment) UseBloblangEnvironment(bEnv *bloblang.Environment) {
	e.bloblangEnv = bEnv
}

// UseFS configures the service environment to use an implementation of ifs.FS
// as its filesystem.
func (e *Environment) UseFS(fs *FS) {
	e.fs = fs
}

// NewStreamBuilder creates a new StreamBuilder upon the defined environment,
// only components known to this environment will be available to the stream
// builder.
func (e *Environment) NewStreamBuilder() *StreamBuilder {
	sb := NewStreamBuilder()
	sb.env = e
	return sb
}

//------------------------------------------------------------------------------

func (e *Environment) getBloblangParserEnv() *ibloblang.Environment {
	if unwrapper, ok := e.bloblangEnv.XUnwrapper().(interface {
		Unwrap() *ibloblang.Environment
	}); ok {
		return unwrapper.Unwrap()
	}
	return ibloblang.GlobalEnvironment()
}

//------------------------------------------------------------------------------

// RegisterBatchBuffer attempts to register a new buffer plugin by providing a
// description of the configuration for the buffer and a constructor for the
// buffer processor. The constructor will be called for each instantiation of
// the component within a config.
//
// Consumed message batches must be created by upstream components (inputs, etc)
// otherwise this buffer will simply receive batches containing single
// messages.
func (e *Environment) RegisterBatchBuffer(name string, spec *ConfigSpec, ctor BatchBufferConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeBuffer
	return e.internal.BufferAdd(func(conf buffer.Config, nm bundle.NewManagement) (buffer.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		b, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return buffer.NewStream(conf.Type, newAirGapBatchBuffer(b), nm), nil
	}, componentSpec)
}

// WalkBuffers executes a provided function argument for every buffer component
// that has been registered to the environment.
func (e *Environment) WalkBuffers(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.BufferDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterCache attempts to register a new cache plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the cache itself. The constructor will be called for each instantiation of
// the component within a config.
func (e *Environment) RegisterCache(name string, spec *ConfigSpec, ctor CacheConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeCache
	return e.internal.CacheAdd(func(conf cache.Config, nm bundle.NewManagement) (cache.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		c, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapCache(c, nm.Metrics()), nil
	}, componentSpec)
}

// WalkCaches executes a provided function argument for every cache component
// that has been registered to the environment.
func (e *Environment) WalkCaches(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.CacheDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterInput attempts to register a new input plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the input itself. The constructor will be called for each instantiation of
// the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacks to get automatic retries.
func (e *Environment) RegisterInput(name string, spec *ConfigSpec, ctor InputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeInput
	return e.internal.InputAdd(iprocessors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		i, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		rdr := newAirGapReader(i)
		return input.NewAsyncReader(conf.Type, rdr, nm)
	}), componentSpec)
}

// RegisterBatchInput attempts to register a new batched input plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the input itself. The constructor will be called for each
// instantiation of the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacksBatched to get automatic
// retries.
func (e *Environment) RegisterBatchInput(name string, spec *ConfigSpec, ctor BatchInputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeInput
	return e.internal.InputAdd(iprocessors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		i, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		if u, ok := i.(interface {
			Unwrap() input.Streamed
		}); ok {
			return u.Unwrap(), nil
		}
		rdr := newAirGapBatchReader(i)
		return input.NewAsyncReader(conf.Type, rdr, nm)
	}), componentSpec)
}

// WalkInputs executes a provided function argument for every input component
// that has been registered to the environment.
func (e *Environment) WalkInputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.InputDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
func (e *Environment) RegisterOutput(name string, spec *ConfigSpec, ctor OutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return e.internal.OutputAdd(oprocessors.WrapConstructor(
		func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
			pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
			if err != nil {
				return nil, err
			}
			op, maxInFlight, err := ctor(pluginConf, newResourcesFromManager(nm))
			if err != nil {
				return nil, err
			}
			if maxInFlight < 1 {
				return nil, fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)
			}
			w := newAirGapWriter(op)
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm)
			if err != nil {
				return nil, err
			}
			return output.OnlySinglePayloads(o), nil
		},
	), componentSpec)
}

// RegisterBatchOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
//
// The constructor of a batch output is able to return a batch policy to be
// applied before calls to write are made, creating batches from the stream of
// messages. However, batches can also be created by upstream components
// (inputs, buffers, etc).
//
// If a batch has been formed upstream it is possible that its size may exceed
// the policy specified in your constructor.
func (e *Environment) RegisterBatchOutput(name string, spec *ConfigSpec, ctor BatchOutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return e.internal.OutputAdd(oprocessors.WrapConstructor(
		func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
			pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
			if err != nil {
				return nil, err
			}
			op, batchPolicy, maxInFlight, err := ctor(pluginConf, newResourcesFromManager(nm))
			if err != nil {
				return nil, err
			}

			if maxInFlight < 1 {
				return nil, fmt.Errorf("invalid maxInFlight parameter: %v", maxInFlight)
			}

			w := newAirGapBatchWriter(op)
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm)
			if err != nil {
				return nil, err
			}
			return batcher.NewFromConfig(batchPolicy.toInternal(), o, nm)
		},
	), componentSpec)
}

// WalkOutputs executes a provided function argument for every output component
// that has been registered to the environment.
func (e *Environment) WalkOutputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.OutputDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterProcessor attempts to register a new processor plugin by providing
// a description of the configuration for the processor and a constructor for
// the processor itself. The constructor will be called for each instantiation
// of the component within a config.
//
// For simple transformations consider implementing a Bloblang plugin method
// instead.
func (e *Environment) RegisterProcessor(name string, spec *ConfigSpec, ctor ProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return e.internal.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapProcessor(conf.Type, r, nm), nil
	}, componentSpec)
}

// RegisterBatchProcessor attempts to register a new processor plugin by
// providing a description of the configuration for the processor and a
// constructor for the processor itself. The constructor will be called for each
// instantiation of the component within a config.
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise this processor will simply receive batches containing single
// messages.
func (e *Environment) RegisterBatchProcessor(name string, spec *ConfigSpec, ctor BatchProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return e.internal.ProcessorAdd(func(conf processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		if u, ok := r.(interface {
			Unwrap() processor.V1
		}); ok {
			return u.Unwrap(), nil
		}
		return newAirGapBatchProcessor(conf.Type, r, nm), nil
	}, componentSpec)
}

// WalkProcessors executes a provided function argument for every processor
// component that has been registered to the environment.
func (e *Environment) WalkProcessors(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.ProcessorDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterRateLimit attempts to register a new rate limit plugin by providing
// a description of the configuration for the plugin as well as a constructor
// for the rate limit itself. The constructor will be called for each
// instantiation of the component within a config.
func (e *Environment) RegisterRateLimit(name string, spec *ConfigSpec, ctor RateLimitConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeRateLimit
	return e.internal.RateLimitAdd(func(conf ratelimit.Config, nm bundle.NewManagement) (ratelimit.V1, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapRateLimit(r, nm.Metrics()), nil
	}, componentSpec)
}

// WalkRateLimits executes a provided function argument for every rate limit
// component that has been registered to the environment.
func (e *Environment) WalkRateLimits(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.RateLimitDocs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterMetricsExporter attempts to register a new metrics exporter plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the metrics exporter itself.
func (e *Environment) RegisterMetricsExporter(name string, spec *ConfigSpec, ctor MetricsExporterConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeMetrics
	return e.internal.MetricsAdd(func(conf metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		m, err := ctor(pluginConf, newReverseAirGapLogger(nm.Logger()))
		if err != nil {
			return nil, err
		}
		return newAirGapMetrics(m), nil
	}, componentSpec)
}

// WalkMetrics executes a provided function argument for every metrics component
// that has been registered to the environment. Note that metrics components
// available to an environment cannot be modified.
func (e *Environment) WalkMetrics(fn func(name string, config *ConfigView)) {
	for _, v := range bundle.AllMetrics.Docs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// RegisterOtelTracerProvider attempts to register a new open telemetry tracer
// provider plugin by providing a description of the configuration for the
// plugin as well as a constructor for the metrics exporter itself. The
// constructor will be called for each instantiation of the component within a
// config.
//
// Experimental: This type signature is experimental and therefore subject to
// change outside of major version releases.
func (e *Environment) RegisterOtelTracerProvider(name string, spec *ConfigSpec, ctor OtelTracerProviderConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeTracer
	return e.internal.TracersAdd(func(conf tracer.Config, nm bundle.NewManagement) (trace.TracerProvider, error) {
		pluginConf, err := extractConfig(nm, spec, name, conf.Plugin, conf)
		if err != nil {
			return nil, err
		}
		t, err := ctor(pluginConf)
		if err != nil {
			return nil, err
		}
		return t, nil
	}, componentSpec)
}

// WalkTracers executes a provided function argument for every tracer component
// that has been registered to the environment. Note that tracer components
// available to an environment cannot be modified.
func (e *Environment) WalkTracers(fn func(name string, config *ConfigView)) {
	for _, v := range bundle.AllTracers.Docs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}

// XFormatConfigJSON returns a byte slice of the Benthos configuration spec
// formatted as a JSON object. The schema of this method is undocumented and is
// not intended for general use.
//
// Experimental: This method is not intended for general use and could have its
// signature and/or behaviour changed outside of major version bumps.
func XFormatConfigJSON() ([]byte, error) {
	return json.Marshal(config.Spec())
}
