package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

// CacheConstructor is a func that's provided a configuration type and access to
// a service manager and must return an instantiation of a cache based on the
// config, or an error.
type CacheConstructor func(conf *ParsedConfig, mgr *Resources) (Cache, error)

// RegisterCache attempts to register a new cache plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the cache itself. The constructor will be called for each instantiation of
// the component within a config.
func RegisterCache(name string, spec *ConfigSpec, ctor CacheConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeCache
	return bundle.AllCaches.Add(func(conf cache.Config, nm bundle.NewManagement) (types.Cache, error) {
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

// InputConstructor is a func that's provided a configuration type and access to
// a service manager, and must return an instantiation of a reader based on the
// config, or an error.
type InputConstructor func(conf *ParsedConfig, mgr *Resources) (Input, error)

// RegisterInput attempts to register a new input plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the input itself. The constructor will be called for each instantiation of
// the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacks to get automatic retries.
func RegisterInput(name string, spec *ConfigSpec, ctor InputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeInput
	return bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(conf input.Config, nm bundle.NewManagement) (input.Type, error) {
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

// OutputConstructor is a func that's provided a configuration type and access
// to a service manager, and must return an instantiation of a writer based on
// the config and a maximum number of in-flight messages to allow, or an error.
type OutputConstructor func(conf *ParsedConfig, mgr *Resources) (out Output, maxInFlight int, err error)

// RegisterOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
func RegisterOutput(name string, spec *ConfigSpec, ctor OutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(
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

// BatchPolicy describes the mechanisms by which batching should be performed of
// messages destined for a Batch output. This is returned by constructors of
// batch outputs.
type BatchPolicy struct {
	ByteSize int    `yaml:"byte_size"`
	Count    int    `yaml:"count"`
	Check    string `yaml:"check"`
	Period   string `yaml:"period"`
}

// BatchOutputConstructor is a func that's provided a configuration type and
// access to a service manager, and must return an instantiation of a writer
// based on the config, a batching policy, and a maximum number of in-flight
// message batches to allow, or an error.
type BatchOutputConstructor func(conf *ParsedConfig, mgr *Resources) (out BatchOutput, batchPolicy BatchPolicy, maxInFlight int, err error)

// RegisterBatchOutput attempts to register a new output plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the output itself. The constructor will be called for each instantiation of
// the component within a config.
func RegisterBatchOutput(name string, spec *ConfigSpec, ctor BatchOutputConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeOutput
	return bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(
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

			batchConf := batch.NewPolicyConfig()
			batchConf.ByteSize = batchPolicy.ByteSize
			batchConf.Count = batchPolicy.Count
			batchConf.Check = batchPolicy.Check
			batchConf.Period = batchPolicy.Period

			w := newAirGapBatchWriter(op)
			o, err := output.NewAsyncWriter(conf.Type, maxInFlight, w, nm.Logger(), nm.Metrics())
			if err != nil {
				return nil, err
			}
			return output.NewBatcherFromConfig(batchConf, o, nm, nm.Logger(), nm.Metrics())
		},
	), componentSpec)
}

// ProcessorConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a processor
// based on the config, or an error.
type ProcessorConstructor func(conf *ParsedConfig, mgr *Resources) (Processor, error)

// RegisterProcessor attempts to register a new processor plugin by providing
// a description of the configuration for the processor and a constructor for
// the processor itself. The constructor will be called for each instantiation
// of the component within a config.
//
// For simple transformations consider implementing a Bloblang plugin method
// instead.
func RegisterProcessor(name string, spec *ConfigSpec, ctor ProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return bundle.AllProcessors.Add(func(conf processor.Config, nm bundle.NewManagement) (processor.Type, error) {
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

// BatchProcessorConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a processor
// based on the config, or an error.
type BatchProcessorConstructor func(conf *ParsedConfig, mgr *Resources) (BatchProcessor, error)

// RegisterBatchProcessor attempts to register a new processor plugin by
// providing a description of the configuration for the processor and a
// constructor for the processor itself. The constructor will be called for each
// instantiation of the component within a config.
func RegisterBatchProcessor(name string, spec *ConfigSpec, ctor BatchProcessorConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeProcessor
	return bundle.AllProcessors.Add(func(conf processor.Config, nm bundle.NewManagement) (processor.Type, error) {
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

// RateLimitConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a rate limit
// based on the config, or an error.
type RateLimitConstructor func(conf *ParsedConfig, mgr *Resources) (RateLimit, error)

// RegisterRateLimit attempts to register a new rate limit plugin by providing
// a description of the configuration for the plugin as well as a constructor
// for the rate limit itself. The constructor will be called for each
// instantiation of the component within a config.
func RegisterRateLimit(name string, spec *ConfigSpec, ctor RateLimitConstructor) error {
	componentSpec := spec.component
	componentSpec.Name = name
	componentSpec.Type = docs.TypeRateLimit
	return bundle.AllRateLimits.Add(func(conf ratelimit.Config, nm bundle.NewManagement) (types.RateLimit, error) {
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
