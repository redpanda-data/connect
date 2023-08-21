package docs

import (
	"errors"
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
				NewLintError(0, LintCustom, errors.New("expected non-empty string, got empty string")),
			},
		},
		{
			name:  "One lint",
			input: `hello meow world`,
			expected: []Lint{
				NewLintError(0, LintCustom, errors.New("no cats allowed")),
			},
		},
		{
			name:  "Two lints",
			input: `hello woof world`,
			expected: []Lint{
				NewLintError(0, LintCustom, errors.New("no dogs allowed")),
				NewLintError(0, LintCustom, errors.New("no noise allowed")),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, node.Encode(test.input))

			lints := f.LintYAML(NewLintContext(NewLintConfig()), &node)
			assert.Equal(t, test.expected, lints)
		})
	}
}

func TestFieldLinting(t *testing.T) {
	tests := []struct {
		name   string
		f      FieldSpec
		input  any
		output []Lint
	}{
		{
			name:  "normal string no linter",
			f:     FieldString("foo", "").LinterBlobl(`root = []`),
			input: "hello world",
		},
		{
			name:  "url valid",
			f:     FieldURL("foo", ""),
			input: "tcp://admin@example.com",
		},
		{
			name:  "url invalid",
			f:     FieldURL("foo", ""),
			input: "not a %#$ valid URL",
			// output: []Lint{
			// 	{Column: 1, What: "field `this`: parse \"not a %\": invalid URL escape \"%\""},
			// },
			// TODO: Disabled until our rule takes interpolation functions into account when necessary.
		},
		{
			name:  "enum valid option",
			f:     FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "foo",
		},
		{
			name:  "enum invalid option",
			f:     FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "buz",
			output: []Lint{
				{Column: 1, Type: 2, What: "value buz is not a valid option for this field"},
			},
		},
		{
			name:  "enum valid pattern option",
			f:     FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "baz:a",
		},
		{
			name:  "enum invalid pattern option",
			f:     FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "baz",
			output: []Lint{
				{Column: 1, Type: 2, What: "value baz is not a valid option for this field"},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var lints []Lint
			linter := test.f.getLintFunc()
			if linter != nil {
				lints = linter(NewLintContext(NewLintConfig()), 0, 0, test.input)
			}
			assert.Equal(t, test.output, lints)
		})
	}
}

func TestSecretScrubbing(t *testing.T) {
	tests := []struct {
		name   string
		f      FieldSpec
		input  any
		output any
	}{
		{
			name:   "not a secret",
			f:      FieldString("foo", ""),
			input:  "hello world",
			output: "hello world",
		},
		{
			name:   "raw secret",
			f:      FieldString("foo", "").Secret(),
			input:  "hello world",
			output: "!!!SECRET_SCRUBBED!!!",
		},
		{
			name:   "env var secret",
			f:      FieldString("foo", "").Secret(),
			input:  "${FOO}",
			output: "${FOO}",
		},
		{
			name:   "env var secret whitespaced",
			f:      FieldString("foo", "").Secret(),
			input:  "  ${FOO} ",
			output: "  ${FOO} ",
		},
		{
			name:   "url no user",
			f:      FieldURL("foo", ""),
			input:  "tcp://example.com",
			output: "tcp://example.com",
		},
		{
			name:   "url user no secret",
			f:      FieldURL("foo", ""),
			input:  "tcp://admin@example.com",
			output: "tcp://admin@example.com",
		},
		{
			name:   "url user with password secret",
			f:      FieldURL("foo", ""),
			input:  "tcp://admin:foo@example.com",
			output: "!!!SECRET_SCRUBBED!!!",
		},
		{
			name:   "url user with password env var",
			f:      FieldURL("foo", ""),
			input:  "tcp://admin:${FOO}@example.com",
			output: "tcp://admin:${FOO}@example.com",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			out, err := test.f.scrubValue(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.output, out)
		})
	}
}
