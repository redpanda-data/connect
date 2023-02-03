package crypto

import (
	"testing"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/stretchr/testify/require"
)

func TestConstantTimeCompare(t *testing.T) {
	mapping := `
    root = this.foo.constant_time_compare(this.bar)
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
			input:    map[string]any{"foo": "some-fancy-string", "bar": "some-fancy-string"},
			expected: true,
		},
		{
			title:    "different values",
			input:    map[string]any{"foo": "some-fancy-string", "bar": "a-blobs-tale"},
			expected: false,
		},
		{
			title:    "different lengths",
			input:    map[string]any{"foo": "some-fancy-string", "bar": "so-fancy"},
			expected: false,
		},
		{
			title:    "one empty",
			input:    map[string]any{"foo": "some-fancy-string", "bar": ""},
			expected: false,
		},
		{
			title:    "both empty",
			input:    map[string]any{"foo": "", "bar": ""},
			expected: true,
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
