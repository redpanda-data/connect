package service

// BatchBufferConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a buffer
// based on the config, or an error.
//
// Consumed message batches must be created by upstream components (inputs, etc)
// otherwise this buffer will simply receive batches containing single messages.
type BatchBufferConstructor func(conf *ParsedConfig, mgr *Resources) (BatchBuffer, error)

// RegisterBatchBuffer attempts to register a new buffer plugin by providing a
// description of the configuration for the buffer and a constructor for the
// buffer processor. The constructor will be called for each instantiation of
// the component within a config.
//
// Consumed message batches must be created by upstream components (inputs, etc)
// otherwise this buffer will simply receive batches containing single
// messages.
func RegisterBatchBuffer(name string, spec *ConfigSpec, ctor BatchBufferConstructor) error {
	return globalEnvironment.RegisterBatchBuffer(name, spec, ctor)
}

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
func RegisterInput(name string, spec *ConfigSpec, ctor InputConstructor, opts ...InputPluginOption) error {
	return globalEnvironment.RegisterInput(name, spec, ctor, opts...)
}

// BatchInputConstructor is a func that's provided a configuration type and
// access to a service manager, and must return an instantiation of a batched
// reader based on the config, or an error.
type BatchInputConstructor func(conf *ParsedConfig, mgr *Resources) (BatchInput, error)

// RegisterBatchInput attempts to register a new batched input plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the input itself. The constructor will be called for each
// instantiation of the component within a config.
//
// If your input implementation doesn't have a specific mechanism for dealing
// with a nack (when the AckFunc provides a non-nil error) then you can instead
// wrap your input implementation with AutoRetryNacksBatched to get automatic
// retries.
func RegisterBatchInput(name string, spec *ConfigSpec, ctor BatchInputConstructor, opts ...InputPluginOption) error {
	return globalEnvironment.RegisterBatchInput(name, spec, ctor, opts...)
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
//
// The constructor of a batch output is able to return a batch policy to be
// applied before calls to write are made, creating batches from the stream of
// messages. However, batches can also be created by upstream components
// (inputs, buffers, etc).
//
// If a batch has been formed upstream it is possible that its size may exceed
// the policy specified in your constructor.
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
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise this processor will simply receive batches containing single
// messages.
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
