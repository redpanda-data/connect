// Copyright 2026 Redpanda Data, Inc.
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

package jsonpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratortest"
)

func TestJSONPathBloblangV2(t *testing.T) {
	exec, err := bloblangv2.Parse(`output.all_names = input.json_path("$..name")`)
	require.NoError(t, err)

	res, err := exec.Query(map[string]any{
		"name": "alice",
		"foo":  map[string]any{"name": "bob"},
	})
	require.NoError(t, err)

	got, ok := res.(map[string]any)
	require.True(t, ok)

	names, ok := got["all_names"].([]any)
	require.True(t, ok)
	assert.ElementsMatch(t, []any{"alice", "bob"}, names)
}

func TestJSONPathBloblangV2InvalidExpression(t *testing.T) {
	_, err := bloblangv2.Parse(`output = input.json_path("$..[")`)
	require.Error(t, err)
}

func TestJSONPathEquivalenceV1V2(t *testing.T) {
	migratortest.AssertEquivalentFn(t,
		`root.all_names = this.json_path("$..name")`,
		func() any {
			return map[string]any{
				"name": "alice",
				"foo":  map[string]any{"name": "bob"},
			}
		},
		map[string]any{"all_names": []any{"alice", "bob"}},
	)
}
