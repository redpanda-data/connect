package service

import (
	"go.opentelemetry.io/otel/trace"
)

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
func RegisterInput(name string, spec *ConfigSpec, ctor InputConstructor) error {
	return globalEnvironment.RegisterInput(name, spec, ctor)
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
func RegisterBatchInput(name string, spec *ConfigSpec, ctor BatchInputConstructor) error {
	return globalEnvironment.RegisterBatchInput(name, spec, ctor)
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

// MetricsExporterConstructor is a func that's provided a configuration type and
// access to a service manager and must return an instantiation of a metrics
// exporter based on the config, or an error.
type MetricsExporterConstructor func(conf *ParsedConfig, log *Logger) (MetricsExporter, error)

// RegisterMetricsExporter attempts to register a new metrics exporter plugin by
// providing a description of the configuration for the plugin as well as a
// constructor for the metrics exporter itself. The constructor will be called
// for each instantiation of the component within a config.
func RegisterMetricsExporter(name string, spec *ConfigSpec, ctor MetricsExporterConstructor) error {
	return globalEnvironment.RegisterMetricsExporter(name, spec, ctor)
}

// OtelTracerProviderConstructor is a func that's provided a configuration type
// and access to a service manager and must return an instantiation of an open
// telemetry tracer provider.
//
// Experimental: This type signature is experimental and therefore subject to
// change outside of major version releases.
type OtelTracerProviderConstructor func(conf *ParsedConfig) (trace.TracerProvider, error)

// RegisterOtelTracerProvider attempts to register a new open telemetry tracer
// provider plugin by providing a description of the configuration for the
// plugin as well as a constructor for the metrics exporter itself. The
// constructor will be called for each instantiation of the component within a
// config.
//
// Experimental: This type signature is experimental and therefore subject to
// change outside of major version releases.
func RegisterOtelTracerProvider(name string, spec *ConfigSpec, ctor OtelTracerProviderConstructor) error {
	return globalEnvironment.RegisterOtelTracerProvider(name, spec, ctor)
}

// BatchScannerCreatorConstructor is a func that's provided a configuration type
// and access to a service manager and must return an instantiation of a batch
// scanner creator.
type BatchScannerCreatorConstructor func(conf *ParsedConfig, mgr *Resources) (BatchScannerCreator, error)

// RegisterBatchScannerCreator attempts to register a new batch scanner exporter
// plugin by providing a description of the configuration for the plugin as well
// as a constructor for the scanner itself. The constructor will be called for
// each instantiation of the component within a config.
func RegisterBatchScannerCreator(name string, spec *ConfigSpec, ctor BatchScannerCreatorConstructor) error {
	return globalEnvironment.RegisterBatchScannerCreator(name, spec, ctor)
}

// RegisterTemplateYAML attempts to register a template to the global
// environment, defined as a YAML document, to the environment such that it may
// be used similarly to any other component plugin.
func RegisterTemplateYAML(yamlStr string) error {
	return globalEnvironment.RegisterTemplateYAML(yamlStr)
}
