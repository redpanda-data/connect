package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/config/schema"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/stream"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

// ConfigSchema contains the definitions of all config fields for the overall
// Benthos config as well as all component plugins. A schema can be used in
// order to analyse, export and import the schemas of varying distributions and
// versions of Benthos.
type ConfigSchema struct {
	fields             docs.FieldSpecs
	env                *Environment
	version, dateBuilt string
}

// FullConfigSchema returns a config schema containing all the standard config
// fields and all plugin definitions from the environment.
func (e *Environment) FullConfigSchema(version, dateBuilt string) *ConfigSchema {
	return &ConfigSchema{
		fields:    config.Spec(),
		env:       e,
		version:   version,
		dateBuilt: dateBuilt,
	}
}

// CoreConfigSchema returns a config schema containing only the core Benthos
// pipeline fields (input, buffer, pipeline, output), and all plugin definitions
// from the environment.
func (e *Environment) CoreConfigSchema(version, dateBuilt string) *ConfigSchema {
	return &ConfigSchema{
		fields:    stream.Spec(),
		env:       e,
		version:   version,
		dateBuilt: dateBuilt,
	}
}

// Environment provides access to the environment referenced by this schema.
func (s *ConfigSchema) Environment() *Environment {
	return s.env
}

// ConfigSchemaFromJSONV0 attempts to parse a JSON serialised definition of an
// entire schema. Any plugins defined in the schema will be registered with the
// config schema environment and can be used for config linting and marshalling.
//
// However, the environment cannot be used for instantiating a runnable pipeline
// as the constructors will be disabled. This allows applications to lint
// against plugin definitions that they themselves haven't imported.
func ConfigSchemaFromJSONV0(jBytes []byte) (*ConfigSchema, error) {
	emptyEnvironment := &Environment{
		internal:    bundle.NewEnvironment(),
		bloblangEnv: bloblang.NewEmptyEnvironment().WithDisabledImports(),
		fs:          ifs.OS(), // TODO: Isolate this as well?
	}

	var tmpSchema rawMessageSchema
	if err := json.Unmarshal(jBytes, &tmpSchema); err != nil {
		return nil, err
	}

	if err := expandEnvWithSchema(&tmpSchema, emptyEnvironment); err != nil {
		return nil, err
	}
	if err := expandBloblEnvWithSchema(&tmpSchema, emptyEnvironment.bloblangEnv); err != nil {
		return nil, err
	}
	return &ConfigSchema{
		version:   tmpSchema.Version,
		dateBuilt: tmpSchema.Date,
		fields:    tmpSchema.Config,
		env:       emptyEnvironment,
	}, nil
}

// MarshalJSONV0 attempts to marshal a JSON document containing the entire
// config and plugin ecosystem schema such that other applications can
// potentially execute their own linting and generation tools with it.
func (s *ConfigSchema) MarshalJSONV0() ([]byte, error) {
	bEnv := s.env.getBloblangParserEnv()

	var functionDocs []query.FunctionSpec
	bEnv.WalkFunctions(func(name string, spec query.FunctionSpec) {
		functionDocs = append(functionDocs, spec)
	})

	var methodDocs []query.MethodSpec
	bEnv.WalkMethods(func(name string, spec query.MethodSpec) {
		methodDocs = append(methodDocs, spec)
	})

	iSchema := schema.Full{
		Version:           s.version,
		Date:              s.dateBuilt,
		Config:            s.fields,
		Buffers:           s.env.internal.BufferDocs(),
		Caches:            s.env.internal.CacheDocs(),
		Inputs:            s.env.internal.InputDocs(),
		Outputs:           s.env.internal.OutputDocs(),
		Processors:        s.env.internal.ProcessorDocs(),
		RateLimits:        s.env.internal.RateLimitDocs(),
		Metrics:           s.env.internal.MetricsDocs(),
		Tracers:           s.env.internal.TracersDocs(),
		Scanners:          s.env.internal.ScannerDocs(),
		BloblangFunctions: functionDocs,
		BloblangMethods:   methodDocs,
	}

	return json.Marshal(iSchema)
}

// SetVersion sets the version and date-built stamp associated with the schema.
func (s *ConfigSchema) SetVersion(version, dateBuilt string) *ConfigSchema {
	s.version = version
	s.dateBuilt = dateBuilt
	return s
}

// Field adds a field to the main config of a schema.
func (s *ConfigSchema) Field(f *ConfigField) *ConfigSchema {
	s.fields = append(s.fields, f.field)
	return s
}

// Fields adds multiple fields to the main config of a schema.
func (s *ConfigSchema) Fields(fs ...*ConfigField) *ConfigSchema {
	spec := s
	for _, f := range fs {
		spec = s.Field(f)
	}
	return spec
}

//------------------------------------------------------------------------------

type rawMessageSchema struct {
	Version           string            `json:"version"`
	Date              string            `json:"date"`
	Config            docs.FieldSpecs   `json:"config,omitempty"`
	Buffers           []json.RawMessage `json:"buffers,omitempty"`
	Caches            []json.RawMessage `json:"caches,omitempty"`
	Inputs            []json.RawMessage `json:"inputs,omitempty"`
	Outputs           []json.RawMessage `json:"outputs,omitempty"`
	Processors        []json.RawMessage `json:"processors,omitempty"`
	RateLimits        []json.RawMessage `json:"rate-limits,omitempty"`
	Metrics           []json.RawMessage `json:"metrics,omitempty"`
	Tracers           []json.RawMessage `json:"tracers,omitempty"`
	Scanners          []json.RawMessage `json:"scanners,omitempty"`
	BloblangFunctions []json.RawMessage `json:"bloblang-functions,omitempty"`
	BloblangMethods   []json.RawMessage `json:"bloblang-methods,omitempty"`
}

func nameAndBloblSpec(data []byte) (string, *bloblang.PluginSpec, error) {
	var nameData struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &nameData); err != nil {
		return "", nil, err
	}

	pluginSpec := bloblang.NewPluginSpec()
	if err := pluginSpec.EncodeJSON(data); err != nil {
		return "", nil, err
	}
	return nameData.Name, pluginSpec, nil
}

func expandBloblEnvWithSchema(schema *rawMessageSchema, bEnv *bloblang.Environment) error {
	hasPlug := map[string]struct{}{}
	bEnv.WalkFunctions(func(name string, spec *bloblang.FunctionView) {
		hasPlug[name] = struct{}{}
	})
	for _, spec := range schema.BloblangFunctions {
		name, pluginSpec, err := nameAndBloblSpec(spec)
		if err != nil {
			return err
		}

		if _, exists := hasPlug[name]; exists {
			continue
		}
		_ = bEnv.RegisterFunctionV2(name, pluginSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			return func() (interface{}, error) {
				return nil, fmt.Errorf("function %v not enabled", name)
			}, nil
		})
	}

	hasPlug = map[string]struct{}{}
	for _, spec := range schema.BloblangMethods {
		name, pluginSpec, err := nameAndBloblSpec(spec)
		if err != nil {
			return err
		}

		if _, exists := hasPlug[name]; exists {
			continue
		}
		_ = bEnv.RegisterMethodV2(name, pluginSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v interface{}) (interface{}, error) {
				return nil, fmt.Errorf("method %v not enabled", name)
			}, nil
		})
	}
	return nil
}

var errComponentDisabled = errors.New("component not enabled")

func expandEnvWithSchema(schema *rawMessageSchema, env *Environment) error {
	for _, spec := range schema.Buffers {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeBuffer); exists {
			continue
		}
		_ = env.RegisterBatchBuffer(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (BatchBuffer, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Caches {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeCache); exists {
			continue
		}
		_ = env.RegisterCache(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (Cache, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Inputs {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeInput); exists {
			continue
		}
		_ = env.RegisterInput(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (Input, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Processors {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeProcessor); exists {
			continue
		}
		_ = env.RegisterProcessor(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (Processor, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Outputs {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeOutput); exists {
			continue
		}
		_ = env.RegisterBatchOutput(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (BatchOutput, BatchPolicy, int, error) {
				return nil, BatchPolicy{}, 0, errComponentDisabled
			})
	}

	for _, spec := range schema.RateLimits {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeRateLimit); exists {
			continue
		}
		_ = env.RegisterRateLimit(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (RateLimit, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Metrics {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeMetrics); exists {
			continue
		}
		_ = env.RegisterMetricsExporter(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, log *Logger) (MetricsExporter, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Tracers {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeTracer); exists {
			continue
		}
		_ = env.RegisterOtelTracerProvider(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig) (trace.TracerProvider, error) {
				return nil, errComponentDisabled
			})
	}

	for _, spec := range schema.Scanners {
		pluginSpec := NewConfigSpec()
		if err := pluginSpec.EncodeJSON(spec); err != nil {
			return err
		}
		if _, exists := env.internal.GetDocs(pluginSpec.component.Name, docs.TypeScanner); exists {
			continue
		}
		_ = env.RegisterBatchScannerCreator(
			pluginSpec.component.Name, pluginSpec,
			func(conf *ParsedConfig, mgr *Resources) (BatchScannerCreator, error) {
				return nil, errComponentDisabled
			})
	}
	return nil
}
