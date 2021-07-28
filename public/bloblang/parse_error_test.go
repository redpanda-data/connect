package bloblang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name        string
		mapping     string
		expLine     int
		expCol      int
		errContains string
	}{
		{
			name: "Bad assignment error",
			mapping: `
root = 
# there wasn't a value!
`,
			expLine:     2,
			expCol:      8,
			errContains: "expected query",
		},
		{
			name: "Bad function args error",
			mapping: `
root.foo = this.foo
root.bar = this.bar.uppercase().replace("something)`,
			expLine:     3,
			expCol:      52,
			errContains: "expected end quote",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.mapping)
			require.Error(t, err)

			pErr, ok := err.(*ParseError)
			require.True(t, ok)

			assert.Equal(t, test.expLine, pErr.Line)
			assert.Equal(t, test.expCol, pErr.Column)
			assert.Contains(t, pErr.ErrorMultiline(), test.errContains, pErr.ErrorMultiline())
		})
	}
}
