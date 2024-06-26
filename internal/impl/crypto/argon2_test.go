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

package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestBloblangCompareArgon2(t *testing.T) {
	// "some-fancy-secret"
	secret2id := "$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"
	secret2i := "$argon2i$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$fyLJGjF+IArVfBnQ6ihK8jQwdNv4sv1aEZGVzBu9oAs"

	mapping := `
    root = this.user_input.compare_argon2(this.hashed_secret)
  `
	exe, err := bloblang.Parse(mapping)
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
			title:    "(argon2i) different values",
			input:    map[string]any{"hashed_secret": secret2i, "user_input": "a-blobs-tale"},
			expected: false,
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

func TestBloblangCompareArgon2_EmptySecret(t *testing.T) {
	input := map[string]any{"hashed_secret": "", "user_input": "some-fancy-secret"}

	mapping := `
  root = this.user_input.compare_argon2(this.hashed_secret)
`
	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(input)
	require.ErrorIs(t, err, errInvalidArgon2Hash)
	require.Nil(t, res)
}

func TestBloblangCompareArgon2_Tampered(t *testing.T) {
	testCases := []struct{ title, secret string }{
		{title: "too few parts", secret: "$argon2id$v=19$m=4096,t=3,p=1$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"},
		{title: "too many parts", secret: "$lol$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"},
		{title: "bad format", secret: "$argon2d$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"},
		{title: "integer overflow parallelism", secret: "$argon2id$v=19$m=4096,t=3,p=137174$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"},
		{title: "extra characters in parameters", secret: "$argon2id$v=19$m=4096,t=3,p=1lololol$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U"},
	}

	mapping := `
    root = this.user_input.compare_argon2(this.hashed_secret)
  `
	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.title, func(t *testing.T) {
			input := map[string]any{"hashed_secret": testCase.secret, "user_input": "some-fancy-secret"}

			res, err := exe.Query(input)
			require.ErrorIs(t, err, errInvalidArgon2Hash)
			require.Nil(t, res)
		})
	}
}
