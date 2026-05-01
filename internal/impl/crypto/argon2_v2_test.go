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

package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestBloblangCompareArgon2V2(t *testing.T) {
	// "some-fancy-secret"
	secret2id := "$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"
	secret2i := "$argon2i$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$fyLJGjF+IArVfBnQ6ihK8jQwdNv4sv1aEZGVzBu9oAs"

	exe, err := bloblangv2.Parse(`output = input.user_input.compare_argon2(input.hashed_secret)`)
	require.NoError(t, err)

	testCases := []struct {
		title    string
		input    map[string]any
		expected bool
	}{
		{
			title:    "(argon2id) same values",
			input:    map[string]any{"hashed_secret": secret2id, "user_input": "some-fancy-secret"},
			expected: true,
		},
		{
			title:    "(argon2id) different values",
			input:    map[string]any{"hashed_secret": secret2id, "user_input": "a-blobs-tale"},
			expected: false,
		},
		{
			title:    "(argon2i) same values",
			input:    map[string]any{"hashed_secret": secret2i, "user_input": "some-fancy-secret"},
			expected: true,
		},
		{
			title:    "empty user input",
			input:    map[string]any{"hashed_secret": secret2id, "user_input": ""},
			expected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			res, err := exe.Query(testCase.input)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, res)
		})
	}
}

func TestBloblangCompareArgon2V2InvalidHash(t *testing.T) {
	exe, err := bloblangv2.Parse(`output = input.user_input.compare_argon2(input.hashed_secret)`)
	require.NoError(t, err)

	_, err = exe.Query(map[string]any{"hashed_secret": "", "user_input": "some-fancy-secret"})
	require.Error(t, err)
}
