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

package changelog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratortest"
)

func TestDiffBloblangV2(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.before.diff(input.after)`)
	require.NoError(t, err)

	got, err := exec.Query(map[string]any{
		"before": map[string]any{"summary": "a"},
		"after":  map[string]any{"summary": "b"},
	})
	require.NoError(t, err)

	assert.Equal(t, []map[string]any{
		{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
	}, got)
}

func TestPatchBloblangV2(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.input.patch(input.changelog)`)
	require.NoError(t, err)

	got, err := exec.Query(map[string]any{
		"input": map[string]any{"summary": "a"},
		"changelog": []map[string]any{
			{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, map[string]any{"summary": "b"}, got)
}

// TestDiffBloblangV2Cases mirrors V1's diff coverage. The V1 implementation
// short-circuits a nil receiver to (nil, nil); V2 enforces null-receiver
// strictness at the runtime layer (the plugin is never invoked) and has no
// public AcceptsNull opt-in yet — that case is therefore omitted here. See
// the V1↔V2 divergence note above.
func TestDiffBloblangV2Cases(t *testing.T) {
	cases := []struct {
		name          string
		before, after any
		expected      []map[string]any
	}{
		{
			name:   "update",
			before: map[string]any{"summary": "a"},
			after:  map[string]any{"summary": "b"},
			expected: []map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
			},
		},
		{
			name:   "remove from object",
			before: map[string]any{"summary": map[string]any{"a": "b", "c": "d"}},
			after:  map[string]any{"summary": map[string]any{"a": "b"}},
			expected: []map[string]any{
				{"Type": "delete", "Path": []string{"summary", "c"}, "From": "d", "To": nil},
			},
		},
		{
			name:   "creation of empty object",
			before: map[string]any{"summary": nil},
			after:  map[string]any{"summary": map[string]any{}},
			expected: []map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": map[string]any{}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := bloblangv2.Parse(`output = input.before.diff(input.after)`)
			require.NoError(t, err)
			got, err := exec.Query(map[string]any{
				"before": tc.before,
				"after":  tc.after,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestPatchBloblangV2Cases(t *testing.T) {
	cases := []struct {
		name      string
		input     map[string]any
		changelog []map[string]any
		expected  map[string]any
	}{
		{
			name:  "patch creation",
			input: map[string]any{},
			changelog: []map[string]any{
				{"Type": "create", "Path": []string{"summary"}, "From": nil, "To": "a"},
			},
			expected: map[string]any{"summary": "a"},
		},
		{
			name:  "patch update",
			input: map[string]any{"summary": "a"},
			changelog: []map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
			},
			expected: map[string]any{"summary": "b"},
		},
		{
			name:  "patch remove",
			input: map[string]any{"summary": "a"},
			changelog: []map[string]any{
				{"Type": "delete", "Path": []string{"summary"}, "From": "a", "To": nil},
			},
			expected: map[string]any{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := bloblangv2.Parse(`output = input.input.patch(input.changelog)`)
			require.NoError(t, err)
			got, err := exec.Query(map[string]any{
				"input":     tc.input,
				"changelog": tc.changelog,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestChangelogEquivalenceV1V2(t *testing.T) {
	t.Run("diff", func(t *testing.T) {
		migratortest.AssertEquivalentFn(t,
			`root = this.before.diff(this.after)`,
			func() any {
				return map[string]any{
					"before": map[string]any{"summary": "a"},
					"after":  map[string]any{"summary": "b"},
				}
			},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
			},
		)
	})
	t.Run("patch", func(t *testing.T) {
		migratortest.AssertEquivalentFn(t,
			`root = this.input.patch(this.changelog)`,
			func() any {
				return map[string]any{
					"input": map[string]any{"summary": "a"},
					"changelog": []map[string]any{
						{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
					},
				}
			},
			map[string]any{"summary": "b"},
		)
	})
}
