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

func TestBloblangCompareBCryptV2(t *testing.T) {
	// "there-are-many-blobs-in-the-sea"
	secret := "$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay"

	exe, err := bloblangv2.Parse(`output = input.user_input.compare_bcrypt(input.hashed_secret)`)
	require.NoError(t, err)

	testCases := []struct {
		title    string
		input    map[string]any
		expected bool
	}{
		{
			title:    "matching secret",
			input:    map[string]any{"hashed_secret": secret, "user_input": "there-are-many-blobs-in-the-sea"},
			expected: true,
		},
		{
			title:    "mismatched secret",
			input:    map[string]any{"hashed_secret": secret, "user_input": "will-i-ever-find-love"},
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
