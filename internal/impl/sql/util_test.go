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

package sql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloblValuesToClickHouseValues(t *testing.T) {
	tests := []struct {
		name  string
		input []any
		want  []any
	}{
		{
			name:  "json.Number integer",
			input: []any{json.Number("12345")},
			want:  []any{int64(12345)},
		},
		{
			name:  "json.Number float",
			input: []any{json.Number("3.14")},
			want:  []any{float64(3.14)},
		},
		{
			name:  "json.Number malformed",
			input: []any{json.Number("not-a-number")},
			want:  []any{json.Number("not-a-number")},
		},
		{
			name:  "float64 passthrough",
			input: []any{float64(1.5)},
			want:  []any{float64(1.5)},
		},
		{
			name:  "string passthrough",
			input: []any{"hello"},
			want:  []any{"hello"},
		},
		{
			name:  "nil passthrough",
			input: []any{nil},
			want:  []any{nil},
		},
		{
			name:  "mixed slice",
			input: []any{json.Number("42"), "foo", nil, json.Number("2.71"), float64(9.9)},
			want:  []any{int64(42), "foo", nil, float64(2.71), float64(9.9)},
		},
		{
			name:  "no json.Number returns original slice",
			input: []any{"a", "b"},
			want:  []any{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bloblValuesToClickHouseValues(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBloblValuesToClickHouseValuesNoAlloc(t *testing.T) {
	// When no json.Number is present, the original slice must be returned (no allocation).
	input := []any{"a", "b", "c"}
	got := bloblValuesToClickHouseValues(input)
	assert.Same(t, &input[0], &got[0])
}
