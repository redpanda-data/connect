// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli_test

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/cli"
)

func testSchema(t testing.TB) *service.ConfigSchema {
	t.Helper()
	s := service.NewEmptyEnvironment()
	for _, n := range []string{"a", "b", "c"} {
		require.NoError(t, s.RegisterInput(n, service.NewConfigSpec(), nil))
	}
	return s.CoreConfigSchema("", "")
}

func TestConnectorsList(t *testing.T) {
	for _, testCase := range []struct {
		name                string
		input               string
		expectedMod         bool
		expectedErrContains string
		expectedInputs      []string
	}{
		{
			name:           "no content",
			input:          ``,
			expectedMod:    false,
			expectedInputs: []string{"a", "b", "c"},
		},
		{
			name: "two lists",
			input: `
deny: [ a ]
allow: [ c ]
`,
			expectedErrContains: `must only contain deny or allow items`,
		},
		{
			name:                "not valid yaml",
			input:               `&&!^@&@%$^@#$`,
			expectedErrContains: `failed to parse connector list file`,
		},
		{
			name: "no items listed",
			input: `
allow: []
deny: []
`,
			expectedMod:    false,
			expectedInputs: []string{"a", "b", "c"},
		},
		{
			name:           "basic allow",
			input:          `allow: [ a, c ]`,
			expectedMod:    true,
			expectedInputs: []string{"a", "c"},
		},
		{
			name:           "basic deny",
			input:          `deny: [ a ]`,
			expectedMod:    true,
			expectedInputs: []string{"b", "c"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			inputPath := path.Join(tmpDir, "components_list.yaml")
			require.NoError(t, os.WriteFile(inputPath, []byte(testCase.input), 0o666))

			sch := testSchema(t)
			actMod, err := cli.ApplyConnectorsList(inputPath, sch)
			if testCase.expectedErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testCase.expectedErrContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testCase.expectedMod, actMod)

			var actInputs []string
			sch.Environment().WalkInputs(func(n string, c *service.ConfigView) {
				actInputs = append(actInputs, n)
			})
			assert.Equal(t, testCase.expectedInputs, actInputs)
		})
	}
}
