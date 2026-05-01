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
