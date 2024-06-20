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

func TestBloblangCompareBCrypt(t *testing.T) {
	// "some-fancy-secret" (cost: 10)
	hashedPassword := "$2y$10$ywv67wCBlpSVu.M7WrZwxuivaNrY.8fe4OF0YzQPtPomk7RS.W9aq"

	mapping := `
    root = this.user_input.compare_bcrypt(this.hashed_password)
  `
	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	testCases := []struct {
		title    string
		input    map[string]any
		expected bool
	}{
		{
			title:    "same values",
			input:    map[string]any{"hashed_password": hashedPassword, "user_input": "some-fancy-secret"},
			expected: true,
		},
		{
			title:    "different values",
			input:    map[string]any{"hashed_password": hashedPassword, "user_input": "a-blobs-tale"},
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
