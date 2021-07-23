package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

// Environment is a collection of Benthos component plugins that can be used in
// order to build and run streaming pipelines with access to different sets of
// plugins. This is useful for sandboxing, testing, etc.
type Environment struct {
	internal *bundle.Environment
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
		internal: e.internal.Clone(),
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
		pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
		pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
			pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
			pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
		pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
		pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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

// RegisterRateLimit attempts to register a new rate limit plugin by providing
// a description of the configuration for the plugin as well as a constructor
// for the rate limit itself. The constructor will be called for each
// instantiation of the component within a config.
func (e *Environment) RegisterRateLimit(name string, spec *ConfigSpec, ctor RateLimitConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeRateLimit
	return e.internal.RateLimits.Add(func(conf ratelimit.Config, nm bundle.NewManagement) (types.RateLimit, error) {
		pluginConf, err := spec.configFromNode(conf.Plugin.(*yaml.Node))
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
