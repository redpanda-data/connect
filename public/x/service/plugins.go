package service

// CacheConstructor is a func that's provided a configuration type and access to
// a service manager and must return an instantiation of a cache based on the
// config, or an error.
type CacheConstructor func(conf *ParsedConfig, mgr *Resources) (Cache, error)

// RegisterCache attempts to register a new cache plugin by providing a
// description of the configuration for the plugin as well as a constructor for
// the cache itself. The constructor will be called for each instantiation of
// the component within a config.
func RegisterCache(name string, spec *ConfigSpec, ctor CacheConstructor) error {
	return globalEnvironment.RegisterCache(name, spec, ctor)
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
	return globalEnvironment.RegisterInput(name, spec, ctor)
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
	return globalEnvironment.RegisterOutput(name, spec, ctor)
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
	return globalEnvironment.RegisterBatchOutput(name, spec, ctor)
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
	return globalEnvironment.RegisterProcessor(name, spec, ctor)
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
	return globalEnvironment.RegisterBatchProcessor(name, spec, ctor)
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
	return globalEnvironment.RegisterRateLimit(name, spec, ctor)
}
