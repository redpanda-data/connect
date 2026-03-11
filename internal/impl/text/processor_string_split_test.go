// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package text

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func newTestStringSplit(t *testing.T, confYAML string) *stringSplitProc {
	t.Helper()
	pConf, err := stringSplitSpec().ParseYAML(confYAML, nil)
	require.NoError(t, err)
	proc, err := newStringSplit(pConf, service.MockResources())
	require.NoError(t, err)
	return proc.(*stringSplitProc)
}

func TestStringSplit(t *testing.T) {
	tests := []struct {
		name     string
		conf     string
		input    []byte
		expected []any
	}{
		{
			name:  "basic newline split",
			conf:  `{}`,
			input: []byte("foo\nbar\nbaz"),
			expected: []any{
				"foo",
				"bar",
				"baz",
			},
		},
		{
			name:  "custom comma delimiter",
			conf:  `delimiter: ","`,
			input: []byte("one,two,three"),
			expected: []any{
				"one",
				"two",
				"three",
			},
		},
		{
			name:  "empty_as_null false leaves empty parts as empty strings",
			conf:  `empty_as_null: false`,
			input: []byte("a\n\nb"),
			expected: []any{
				"a",
				"",
				"b",
			},
		},
		{
			name:  "empty_as_null true converts empty parts to nil",
			conf:  `empty_as_null: true`,
			input: []byte("a\n\nb"),
			expected: []any{
				"a",
				nil,
				"b",
			},
		},
		{
			name:  "no delimiter found returns single element",
			conf:  `{}`,
			input: []byte("no newlines here"),
			expected: []any{
				"no newlines here",
			},
		},
		{
			name:     "empty input produces single empty string",
			conf:     `{}`,
			input:    []byte(""),
			expected: []any{""},
		},
		{
			name:     "empty input with empty_as_null produces single nil",
			conf:     `empty_as_null: true`,
			input:    []byte(""),
			expected: []any{nil},
		},
		{
			name:  "multiple consecutive delimiters",
			conf:  `{}`,
			input: []byte("a\n\n\nb"),
			expected: []any{
				"a",
				"",
				"",
				"b",
			},
		},
		{
			name:  "multiple consecutive delimiters with empty_as_null",
			conf:  `empty_as_null: true`,
			input: []byte("a\n\n\nb"),
			expected: []any{
				"a",
				nil,
				nil,
				"b",
			},
		},
		{
			name:  "emit_bytes returns byte slices",
			conf:  `emit_bytes: true`,
			input: []byte("foo\nbar"),
			expected: []any{
				[]byte("foo"),
				[]byte("bar"),
			},
		},
		{
			name:  "emit_bytes with empty_as_null",
			conf:  "emit_bytes: true\nempty_as_null: true",
			input: []byte("a\n\nb"),
			expected: []any{
				[]byte("a"),
				nil,
				[]byte("b"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := newTestStringSplit(t, tt.conf)
			t.Cleanup(func() { require.NoError(t, proc.Close(context.Background())) })

			msg := service.NewMessage(tt.input)
			batches, err := proc.ProcessBatch(t.Context(), service.MessageBatch{msg})
			require.NoError(t, err)
			require.Len(t, batches, 1)
			require.Len(t, batches[0], 1)

			got, err := batches[0][0].AsStructured()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
