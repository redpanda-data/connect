package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestStreamLinter(t *testing.T) {
	env := service.NewEmptyEnvironment()

	require.NoError(t, env.RegisterInput("dog", service.NewConfigSpec().Fields(
		service.NewStringField("woof").Example("WOOF"),
	),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterBatchBuffer("none", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("stdout", service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	schema := env.CoreConfigSchema("", "")

	tests := []struct {
		name         string
		config       string
		lintContains []string
		errContains  string
		linter       *service.StreamConfigLinter
	}{
		{
			name: "basic config no lints",
			config: `
input:
  dog:
    woof: wooooowooof
`,
		},
		{
			name: "unknown field lint",
			config: `
input:
  dog:
    woof: wooooowooof
    huh: whats this?
`,
			lintContains: []string{
				"field huh not recognised",
			},
		},
		{
			name:        "invalid yaml",
			config:      `	this			 !!!! isn't valid: yaml dog`,
			errContains: "found character",
		},
		{
			name: "env var defined",
			config: `
input:
  dog:
    woof: ${WOOF}`,
			linter: schema.NewStreamConfigLinter().
				SetEnvVarLookupFunc(func(ctx context.Context, s string) (string, bool) {
					return "meow", true
				}),
		},
		{
			name: "env var missing with default",
			config: `
input:
  dog:
    woof: ${WOOF:defaultvalue}`,
			linter: schema.NewStreamConfigLinter().
				SetEnvVarLookupFunc(func(ctx context.Context, s string) (string, bool) {
					return "", false
				}),
		},
		{
			name: "env var missing with lint disabled",
			config: `
input:
  dog:
    woof: ${WOOF}`,
			linter: schema.NewStreamConfigLinter().
				SetSkipEnvVarCheck(true).
				SetEnvVarLookupFunc(func(ctx context.Context, s string) (string, bool) {
					return "", false
				}),
		},
		{
			name: "env var missing and linted",
			config: `
input:
  dog:
    woof: ${WOOF}`,
			linter: schema.NewStreamConfigLinter().
				SetEnvVarLookupFunc(func(ctx context.Context, s string) (string, bool) {
					return "", false
				}),
			lintContains: []string{
				"required environment variables were not set",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if test.linter == nil {
				test.linter = schema.NewStreamConfigLinter()
			}

			lints, err := test.linter.LintYAML([]byte(test.config))
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
				return
			}

			require.NoError(t, err)
			require.Len(t, lints, len(test.lintContains))
			for i, lc := range test.lintContains {
				assert.Contains(t, lints[i].Error(), lc)
			}
		})
	}
}
