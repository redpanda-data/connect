package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBloblLinter(t *testing.T) {
	f := FieldString("foo", "").LinterBlobl(`root = if this.length() > 0 {
  if this.contains("meow") {
    [ "no cats allowed" ]
  } else if this.contains("woof") {
    [ "no dogs allowed", "no noise allowed" ]
  }
} else {
  "expected non-empty string, got empty %T".format(this)
}
`)

	lintCtx := NewLintContext()

	tests := []struct {
		name     string
		input    any
		expected []Lint
	}{
		{
			name:     "No lints",
			input:    `hello world`,
			expected: nil,
		},
		{
			name:  "Single type lint",
			input: "",
			expected: []Lint{
				NewLintError(0, LintCustom, "expected non-empty string, got empty string"),
			},
		},
		{
			name:  "One lint",
			input: `hello meow world`,
			expected: []Lint{
				NewLintError(0, LintCustom, "no cats allowed"),
			},
		},
		{
			name:  "Two lints",
			input: `hello woof world`,
			expected: []Lint{
				NewLintError(0, LintCustom, "no dogs allowed"),
				NewLintError(0, LintCustom, "no noise allowed"),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, node.Encode(test.input))

			lints := f.LintYAML(lintCtx, &node)
			assert.Equal(t, test.expected, lints)
		})
	}
}
