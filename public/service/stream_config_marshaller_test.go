package service_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestStreamMarshallerMinimal(t *testing.T) {
	env := service.NewEmptyEnvironment()

	require.NoError(t, env.RegisterInput("dog", service.NewConfigSpec().Field(service.NewStringField("woof").Example("WOOF")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("none", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("cat", service.NewConfigSpec().Field(service.NewStringField("meow").Example("MEOW")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	yamlStr, err := env.CoreConfigSchema("aaa", "bbb").NewStreamConfigMarshaller().
		AnyToYAML(map[string]any{
			"input": map[string]any{
				"type": "dog",
			},
			"output": map[string]any{
				"type": "cat",
			},
		})
	require.NoError(t, err)

	assert.Equal(t, `input:
  dog: {} # No default (required)
buffer:
  none: {}
pipeline:
  threads: -1
  processors: []
output:
  cat: {} # No default (required)
`, yamlStr)
}

func TestStreamMarshallerExamples(t *testing.T) {
	env := service.NewEmptyEnvironment()

	require.NoError(t, env.RegisterInput("dog", service.NewConfigSpec().Field(service.NewStringField("woof").Example("WOOF")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("none", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("cat", service.NewConfigSpec().Field(service.NewStringField("meow").Example("MEOW")),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	yamlStr, err := env.CoreConfigSchema("aaa", "bbb").NewStreamConfigMarshaller().
		SetHydrateExamples(true).
		AnyToYAML(map[string]any{
			"input": map[string]any{
				"type": "dog",
			},
			"output": map[string]any{
				"type": "cat",
			},
		})
	require.NoError(t, err)

	assert.Equal(t, `input:
  dog:
    woof: WOOF # No default (required)
buffer:
  none: {}
pipeline:
  threads: -1
  processors: []
output:
  cat:
    meow: MEOW # No default (required)
`, yamlStr)
}

func TestStreamMarshallerFieldFilter(t *testing.T) {
	env := service.NewEmptyEnvironment()

	require.NoError(t, env.RegisterInput("dog", service.NewConfigSpec().Fields(
		service.NewStringField("basicfield").Example("WOOF"),
		service.NewStringField("advancedfield").Advanced().Example("WOOF"),
	),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("none", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("cat", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	yamlStr, err := env.CoreConfigSchema("aaa", "bbb").NewStreamConfigMarshaller().
		SetHydrateExamples(true).
		SetFieldFilter(func(view *service.FieldView, value any) bool {
			return !view.IsAdvanced()
		}).
		AnyToYAML(map[string]any{
			"input": map[string]any{
				"type": "dog",
			},
			"output": map[string]any{
				"type": "cat",
			},
		})
	require.NoError(t, err)

	assert.Equal(t, `input:
  dog:
    basicfield: WOOF # No default (required)
buffer:
  none: {}
pipeline:
  threads: -1
  processors: []
output:
  cat: null # No default (required)
`, yamlStr)
}
