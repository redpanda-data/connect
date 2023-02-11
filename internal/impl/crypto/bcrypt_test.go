package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
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
			require.Equal(t, res, testCase.expected)
		})
	}
}
