package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/public/bloblang"
	"gopkg.in/yaml.v3"
)

// Environment is a collection of Benthos component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This can be useful for sandboxing, testing, etc, but most plugin
// authors do not need to create an Environment and can simply use the global
// environment.
type Environment struct {
	internal    *bundle.Environment
	bloblangEnv *bloblang.Environment
}

var globalEnvironment = &Environment{
	internal: bundle.GlobalEnvironment,
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
		bloblangEnv: e.bloblangEnv,
	}
}

// UseBloblangEnvironment configures the service environment to restrict
// components constructed with it to a specific Bloblang environment.
//
// Experimental: Using custom Bloblang environments throughout a Benthos service
// is still a work in progress. Currently only linting and your plugin
// Interpolation and Bloblang fields are parsed through the environment, but
// native components such as the standard `bloblang` processor will continue to
// use the global environment during construction.
func (e *Environment) UseBloblangEnvironment(bEnv *bloblang.Environment) {
	e.bloblangEnv = bEnv
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

func (e *Environment) getBloblangParserContext() parser.Context {
	if e.bloblangEnv == nil {
		return parser.GlobalContext()
	}
	if unwrapper, ok := e.bloblangEnv.XUnwrapper().(interface {
		Unwrap() parser.Context
	}); ok {
		return unwrapper.Unwrap()
	}
	return parser.GlobalContext()
}

//------------------------------------------------------------------------------

// WalkBuffers executes a provided function argument for every buffer component
// that has been registered to the environment.
func (e *Environment) WalkBuffers(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.Buffers.Docs() {
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
	return e.internal.Caches.Add(func(conf cache.Config, nm bundle.NewManagement) (types.Cache, error) {
		pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
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
	for _, v := range e.internal.Caches.Docs() {
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
	return e.internal.Inputs.Add(bundle.InputConstructorFromSimple(func(conf input.Config, nm bundle.NewManagement) (input.Type, error) {
		pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}
		i, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		rdr := newAirGapReader(i)
		return input.NewAsyncReader(conf.Type, false, rdr, nm.Logger(), nm.Metrics())
	}), componentSpec)
}

// WalkInputs executes a provided function argument for every input component
// that has been registered to the environment.
func (e *Environment) WalkInputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.Inputs.Docs() {
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
	return e.internal.Outputs.Add(bundle.OutputConstructorFromSimple(
		func(conf output.Config, nm bundle.NewManagement) (output.Type, error) {
			pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
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
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm.Logger(), nm.Metrics())
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
func (e *Environment) RegisterBatchOutput(name string, spec *ConfigSpec, ctor BatchOutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return e.internal.Outputs.Add(bundle.OutputConstructorFromSimple(
		func(conf output.Config, nm bundle.NewManagement) (output.Type, error) {
			pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
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
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm.Logger(), nm.Metrics())
			if err != nil {
				return nil, err
			}
			return output.NewBatcherFromConfig(batchPolicy.toInternal(), o, nm, nm.Logger(), nm.Metrics())
		},
	), componentSpec)
}

// WalkOutputs executes a provided function argument for every output component
// that has been registered to the environment.
func (e *Environment) WalkOutputs(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.Outputs.Docs() {
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
	return e.internal.Processors.Add(func(conf processor.Config, nm bundle.NewManagement) (processor.Type, error) {
		pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapProcessor(conf.Type, r, nm.Metrics()), nil
	}, componentSpec)
}

// RegisterBatchProcessor attempts to register a new processor plugin by
// providing a description of the configuration for the processor and a
// constructor for the processor itself. The constructor will be called for each
// instantiation of the component within a config.
func (e *Environment) RegisterBatchProcessor(name string, spec *ConfigSpec, ctor BatchProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return e.internal.Processors.Add(func(conf processor.Config, nm bundle.NewManagement) (processor.Type, error) {
		pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
		if err != nil {
			return nil, err
		}
		r, err := ctor(pluginConf, newResourcesFromManager(nm))
		if err != nil {
			return nil, err
		}
		return newAirGapBatchProcessor(conf.Type, r, nm.Metrics()), nil
	}, componentSpec)
}

// WalkProcessors executes a provided function argument for every processor
// component that has been registered to the environment.
func (e *Environment) WalkProcessors(fn func(name string, config *ConfigView)) {
	for _, v := range e.internal.Processors.Docs() {
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
	return e.internal.RateLimits.Add(func(conf ratelimit.Config, nm bundle.NewManagement) (types.RateLimit, error) {
		pluginConf, err := spec.configFromNode(e, nm, conf.Plugin.(*yaml.Node))
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
	for _, v := range e.internal.RateLimits.Docs() {
		fn(v.Name, &ConfigView{
			component: v,
		})
	}
}
