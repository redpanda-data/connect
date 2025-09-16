// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"
)

func TestComponentLinter(t *testing.T) {
	env := service.NewEmptyEnvironment()

	require.NoError(t, env.RegisterInput("testinput", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterProcessor("testprocessor", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Processor, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterCache("testcache", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Cache, error) {
			return nil, errors.New("nope")
		}))

	require.NoError(t, env.RegisterOutput("testoutput", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (out service.Output, maxInFlight int, err error) {
			err = errors.New("nope")
			return
		}))

	tests := []struct {
		name         string
		typeStr      string
		config       string
		lintContains []string
		errContains  string
	}{
		{
			name:    "basic config no meta",
			typeStr: "input",
			config: `
label: a
testinput: {}
`,
		},
		{
			name:    "meta config no lints",
			typeStr: "input",
			config: `
label: a
testinput: {}
meta:
  tags: [ nah ]
  mcp:
    enabled: true
`,
		},
		{
			name:    "meta config props allowed",
			typeStr: "processor",
			config: `
label: a
testprocessor: {}
meta:
  tags: [ nah ]
  mcp:
    enabled: true
    properties:
      - name: meow
        type: string
`,
		},
		{
			name:    "meta config props not allowed",
			typeStr: "input",
			config: `
label: a
testinput: {}
meta:
  tags: [ nah ]
  mcp:
    enabled: true
    properties:
      - name: meow
        type: string
`,
			lintContains: []string{
				"component type does not support custom properties",
			},
		},
		{
			name:    "meta config props missing type",
			typeStr: "processor",
			config: `
label: a
testprocessor: {}
meta:
  tags: [ nah ]
  mcp:
    enabled: true
    properties:
      - name: meow
`,
			lintContains: []string{
				"field type is required",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			linter := schema.ComponentLinter(env)

			lints, err := linter.LintYAML(test.typeStr, []byte(test.config))
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
