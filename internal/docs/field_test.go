package docs_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func TestBloblLinter(t *testing.T) {
	f := docs.FieldString("foo", "").LinterBlobl(`root = if this.length() > 0 {
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
		expected []docs.Lint
	}{
		{
			name:     "No lints",
			input:    `hello world`,
			expected: nil,
		},
		{
			name:  "Single type lint",
			input: "",
			expected: []docs.Lint{
				docs.NewLintError(0, docs.LintCustom, errors.New("expected non-empty string, got empty string")),
			},
		},
		{
			name:  "One lint",
			input: `hello meow world`,
			expected: []docs.Lint{
				docs.NewLintError(0, docs.LintCustom, errors.New("no cats allowed")),
			},
		},
		{
			name:  "Two lints",
			input: `hello woof world`,
			expected: []docs.Lint{
				docs.NewLintError(0, docs.LintCustom, errors.New("no dogs allowed")),
				docs.NewLintError(0, docs.LintCustom, errors.New("no noise allowed")),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, node.Encode(test.input))

			lints := f.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), &node)
			assert.Equal(t, test.expected, lints)
		})
	}
}

func TestFieldLinting(t *testing.T) {
	tests := []struct {
		name   string
		f      docs.FieldSpec
		input  any
		output []docs.Lint
	}{
		{
			name:  "normal string no linter",
			f:     docs.FieldString("foo", "").LinterBlobl(`root = []`),
			input: "hello world",
		},
		{
			name:  "url valid",
			f:     docs.FieldURL("foo", ""),
			input: "tcp://admin@example.com",
		},
		{
			name:  "url invalid",
			f:     docs.FieldURL("foo", ""),
			input: "not a %#$ valid URL",
			// output: []Lint{
			// 	{Column: 1, What: "field `this`: parse \"not a %\": invalid URL escape \"%\""},
			// },
			// TODO: Disabled until our rule takes interpolation functions into account when necessary.
		},
		{
			name:  "enum valid option",
			f:     docs.FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "foo",
		},
		{
			name:  "enum invalid option",
			f:     docs.FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "buz",
			output: []docs.Lint{
				{Column: 1, Type: 2, What: "value buz is not a valid option for this field"},
			},
		},
		{
			name:  "enum valid case insensitive option",
			f:     docs.FieldString("foo", "").HasOptions("foo", "bar", "baz"),
			input: "BAR",
		},
		{
			name:  "enum invalid pattern option",
			f:     docs.FieldString("foo", "").HasOptions("foo", "bar", "baz:x"),
			input: "baz",
			output: []docs.Lint{
				{Column: 1, Type: 2, What: "value baz is not a valid option for this field"},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var lints []docs.Lint
			linter := test.f.GetLintFunc()
			if linter != nil {
				lints = linter(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), 0, 0, test.input)
			}
			assert.Equal(t, test.output, lints)
		})
	}
}

func TestSecretScrubbing(t *testing.T) {
	tests := []struct {
		name   string
		f      docs.FieldSpec
		input  any
		output any
	}{
		{
			name:   "not a secret",
			f:      docs.FieldString("foo", ""),
			input:  "hello world",
			output: "hello world",
		},
		{
			name:   "raw secret",
			f:      docs.FieldString("foo", "").Secret(),
			input:  "hello world",
			output: "!!!SECRET_SCRUBBED!!!",
		},
		{
			name:   "raw secret with empty value",
			f:      docs.FieldString("foo", "").Secret(),
			input:  "",
			output: "",
		},
		{
			name:   "env var secret",
			f:      docs.FieldString("foo", "").Secret(),
			input:  "${FOO}",
			output: "${FOO}",
		},
		{
			name:   "env var secret whitespaced",
			f:      docs.FieldString("foo", "").Secret(),
			input:  "  ${FOO} ",
			output: "  ${FOO} ",
		},
		{
			name:   "url no user",
			f:      docs.FieldURL("foo", ""),
			input:  "tcp://example.com",
			output: "tcp://example.com",
		},
		{
			name:   "url user no secret",
			f:      docs.FieldURL("foo", ""),
			input:  "tcp://admin@example.com",
			output: "tcp://admin@example.com",
		},
		{
			name:   "url user with password secret",
			f:      docs.FieldURL("foo", ""),
			input:  "tcp://admin:foo@example.com",
			output: "!!!SECRET_SCRUBBED!!!",
		},
		{
			name:   "url user with password env var",
			f:      docs.FieldURL("foo", ""),
			input:  "tcp://admin:${FOO}@example.com",
			output: "tcp://admin:${FOO}@example.com",
		},
		{
			name:   "url user with empty password",
			f:      docs.FieldURL("foo", ""),
			input:  "tcp://admin:@example.com",
			output: "tcp://admin:@example.com",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			out, err := test.f.ScrubValue(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.output, out)
		})
	}
}
