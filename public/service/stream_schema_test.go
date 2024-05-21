package service_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestStreamSchemaInteropCore(t *testing.T) {
	bEnv := bloblang.NewEmptyEnvironment()

	require.NoError(t, bEnv.RegisterFunction("cow", func(args ...any) (bloblang.Function, error) {
		return nil, errors.New("nope")
	}))
	require.NoError(t, bEnv.RegisterMethod("sheep", func(args ...any) (bloblang.Method, error) {
		return nil, errors.New("nope")
	}))

	env := service.NewEmptyEnvironment()
	env.UseBloblangEnvironment(bEnv)

	require.NoError(t, env.RegisterInput("testinput", service.NewConfigSpec().Field(service.NewStringField("woof").Example("WOOF")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("testbuffer", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterProcessor("testprocessor", service.NewConfigSpec().Field(service.NewBloblangField("mapfield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("testoutput", service.NewConfigSpec().Field(service.NewStringField("meow").Example("MEOW")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	require.NoError(t, env.RegisterCache("testcache", service.NewConfigSpec().Field(service.NewStringField("cachefield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterRateLimit("testratelimit", service.NewConfigSpec().Field(service.NewStringField("ratelimitfield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec().Field(service.NewStringField("metricsfield")),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOtelTracerProvider("testtracer", service.NewConfigSpec().Field(service.NewStringField("tracerfield")),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			return nil, errors.New("nope")
		}))

	schemaSource := env.FullConfigSchema("aaa", "bbb").Field(service.NewStringField("foo").Default("test default"))

	schemaBytes, err := schemaSource.MarshalJSONV0()
	require.NoError(t, err)

	schemaSink, err := service.ConfigSchemaFromJSONV0(schemaBytes)
	require.NoError(t, err)

	yamlStr, err := schemaSink.NewStreamConfigMarshaller().
		SetHydrateExamples(true).
		AnyToYAML(map[string]any{
			"input":  map[string]any{"type": "testinput"},
			"buffer": map[string]any{"type": "testbuffer"},
			"pipeline": map[string]any{
				"processors": []any{
					map[string]any{
						"testprocessor": map[string]any{
							"mapfield": "root = cow().sheep()",
						},
					},
				},
			},
			"output": map[string]any{"type": "testoutput"},
			"cache_resources": []any{
				map[string]any{"label": "acache", "type": "testcache"},
			},
			"rate_limit_resources": []any{
				map[string]any{"label": "aratelimit", "type": "testratelimit"},
			},
			"metrics": map[string]any{"type": "testmetrics"},
			"tracer":  map[string]any{"type": "testtracer"},
		})
	require.NoError(t, err)

	for _, k := range []string{
		`
input:
  testinput:
    woof: WOOF # No default (required)`,
		`
output:
  testoutput:
    meow: MEOW # No default (required)`,
		`
buffer:
  testbuffer: null # No default (required)`,
		`
    - testprocessor:
        mapfield: root = cow().sheep()`,
		`
cache_resources:
  - label: acache
    testcache:
      cachefield: "" # No default (required)`,
		`
rate_limit_resources:
  - label: aratelimit
    testratelimit:
      ratelimitfield: "" # No default (required)`,
		`
metrics:
  testmetrics:
    metricsfield: "" # No default (required)
`,
		`
tracer:
  testtracer:
    tracerfield: "" # No default (required)`,
		`foo: test default`,
	} {
		assert.Contains(t, yamlStr, k)
	}
}

func TestStreamSchemaInteropLinter(t *testing.T) {
	bEnv := bloblang.NewEmptyEnvironment()

	require.NoError(t, bEnv.RegisterFunction("cow", func(args ...any) (bloblang.Function, error) {
		return nil, errors.New("nope")
	}))
	require.NoError(t, bEnv.RegisterMethod("sheep", func(args ...any) (bloblang.Method, error) {
		return nil, errors.New("nope")
	}))

	env := service.NewEmptyEnvironment()
	env.UseBloblangEnvironment(bEnv)

	require.NoError(t, env.RegisterInput("testinput", service.NewConfigSpec().Field(service.NewStringField("woof").Example("WOOF")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("testbuffer", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterProcessor("testprocessor", service.NewConfigSpec().Field(service.NewBloblangField("mapfield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("testoutput", service.NewConfigSpec().Field(service.NewStringField("meow").Example("MEOW")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	require.NoError(t, env.RegisterCache("testcache", service.NewConfigSpec().Field(service.NewStringField("cachefield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterRateLimit("testratelimit", service.NewConfigSpec().Field(service.NewStringField("ratelimitfield")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterMetricsExporter("testmetrics", service.NewConfigSpec().Field(service.NewStringField("metricsfield")),
		func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOtelTracerProvider("testtracer", service.NewConfigSpec().Field(service.NewStringField("tracerfield")),
		func(conf *service.ParsedConfig) (trace.TracerProvider, error) {
			return nil, errors.New("nope")
		}))

	schemaSource := env.FullConfigSchema("aaa", "bbb").Field(service.NewStringField("foo").Default("test default"))

	schemaBytes, err := schemaSource.MarshalJSONV0()
	require.NoError(t, err)

	schemaSink, err := service.ConfigSchemaFromJSONV0(schemaBytes)
	require.NoError(t, err)

	lints, err := schemaSink.NewStreamConfigLinter().LintYAML([]byte(`
input:
  testinput:
    woof: WOOF

pipeline:
  processors:
    - testprocessor:
        mapfield: root = cow().sheep()

output:
  testoutput:
    meow: MEOW # No default (required)
`))
	require.NoError(t, err)
	assert.Empty(t, lints)
}
