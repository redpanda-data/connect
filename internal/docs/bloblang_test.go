package docs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLintBloblangMapping(t *testing.T) {
	type Test struct {
		mapping   string
		line      int
		col       int
		wantLints []Lint
	}
	tests := map[string]Test{
		"mapping": {
			mapping: `this.foo = "bar"`,
			line:    0,
			col:     0,
		},
		"empty mapping": {
			mapping: ``,
			line:    0,
			col:     0,
		},
		"invalid mapping": {
			mapping: `this.foo = #`,
			line:    2,
			col:     4,
			wantLints: []Lint{
				{
					Line:   2,
					Column: 16,
					Level:  LintError,
					Type:   LintBadBloblang,
					What:   `expected query, got: #`,
				},
			},
		},
	}

	ctx := NewLintContext(NewLintConfig())
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			gotLints := LintBloblangMapping(ctx, test.line, test.col, test.mapping)
			require.EqualValues(t, test.wantLints, gotLints)
		})
	}
}

func TestLintBloblangField(t *testing.T) {
	type Test struct {
		mapping   string
		line      int
		col       int
		wantLints []Lint
	}
	tests := map[string]Test{
		"static string field": {
			mapping: `foobar`,
			line:    0,
			col:     0,
		},
		"empty field": {
			mapping: ``,
			line:    0,
			col:     0,
		},
		"interpolated field": {
			mapping: `${! json() }`,
			line:    0,
			col:     0,
		},
		"invalid interpolated field": {
			mapping: `${! whoopsie{} }`,
			line:    2,
			col:     4,
			wantLints: []Lint{
				{
					Line:   2,
					Column: 17,
					Level:  LintError,
					Type:   LintBadBloblang,
					What:   `required: expected end of expression, got: {} }`,
				},
			},
		},
		"invalid empty interpolated field": {
			mapping: `${! }`,
			line:    2,
			col:     4,
			wantLints: []Lint{
				{
					Line:   2,
					Column: 9,
					Level:  LintError,
					Type:   LintBadBloblang,
					What:   `required: expected query, got: }`,
				},
			},
		},
	}

	ctx := NewLintContext(NewLintConfig())
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			gotLints := LintBloblangField(ctx, test.line, test.col, test.mapping)
			require.EqualValues(t, test.wantLints, gotLints)
		})
	}
}
