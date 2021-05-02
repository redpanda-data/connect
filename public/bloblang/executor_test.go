package bloblang

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutorQuery(t *testing.T) {
	tests := []struct {
		name        string
		mapping     string
		input       interface{}
		output      interface{}
		errContains string
	}{
		{
			name:        "no metadata get",
			mapping:     `root = meta("foo")`,
			errContains: "metadata value 'foo' not found",
		},
		{
			name:        "no metadata set",
			mapping:     `meta foo = "hello"`,
			errContains: "unable to assign metadata in the current context",
		},
		{
			name: "variable get and set",
			mapping: `let foo = "foo value"
root = $foo`,
			output: "foo value",
		},
		{
			name:    "not mapped",
			mapping: `root = if false { "not this" }`,
			input: map[string]interface{}{
				"hello": "world",
			},
			output: map[string]interface{}{
				"hello": "world",
			},
		},
		{
			name:        "delete root for some reason",
			mapping:     `root = deleted()`,
			errContains: "root was deleted",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := NewEnvironment().Parse(test.mapping)
			require.NoError(t, err)

			res, err := m.Query(test.input)
			if test.errContains == "" {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			} else {
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestExecutorOverlay(t *testing.T) {
	tests := []struct {
		name        string
		mapping     string
		overlay     interface{}
		input       interface{}
		output      interface{}
		errContains string
	}{
		{
			name:        "no metadata get",
			mapping:     `root = meta("foo")`,
			errContains: "metadata value 'foo' not found",
		},
		{
			name:        "no metadata set",
			mapping:     `meta foo = "hello"`,
			errContains: "unable to assign metadata in the current context",
		},
		{
			name: "variable get and set",
			mapping: `let foo = "foo value"
root = $foo`,
			output: "foo value",
		},
		{
			name:    "set nested field from nil",
			mapping: `root.foo.bar = "hello world"`,
			output: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "hello world",
				},
			},
		},
		{
			name:        "set nested field from value",
			mapping:     `root.foo.bar = "hello world"`,
			overlay:     "value type",
			errContains: "the root was a non-object type",
		},
		{
			name:    "set nested field from object",
			mapping: `root.foo.bar = "hello world"`,
			overlay: map[string]interface{}{
				"baz": "started with this",
			},
			output: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "hello world",
				},
				"baz": "started with this",
			},
		},
		{
			name:    "not mapped",
			mapping: `root = if false { "not this" }`,
			overlay: map[string]interface{}{
				"hello": "world",
			},
			output: map[string]interface{}{
				"hello": "world",
			},
		},
		{
			name:        "delete root for some reason",
			mapping:     `root = deleted()`,
			errContains: "root was deleted",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := NewEnvironment().Parse(test.mapping)
			require.NoError(t, err)

			res := test.overlay
			err = m.Overlay(test.input, &res)
			if test.errContains == "" {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestExecutorQueryVarAllocation(t *testing.T) {
	m, err := NewEnvironment().Parse(`
root.foo = $meow | "not init"
let meow = "meow meow"
root.bar = $meow | "not init"
root.baz = this.input
	`)
	require.NoError(t, err)

	expected := map[string]interface{}{
		"foo": "not init",
		"bar": "meow meow",
		"baz": "from input",
	}

	res, err := m.Query(map[string]interface{}{
		"input": "from input",
	})
	require.NoError(t, err)
	assert.Equal(t, expected, res)

	// Run it again and make sure our variables were reset.
	res, err = m.Query(map[string]interface{}{
		"input": "from input 2",
	})
	expected["baz"] = "from input 2"
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestExecutorOverlayVarAllocation(t *testing.T) {
	m, err := NewEnvironment().Parse(`
root.foo = $meow | "not init"
let meow = "meow meow"
root.bar = $meow | "not init"
root.baz = this.input
	`)
	require.NoError(t, err)

	expected := map[string]interface{}{
		"started": "with this",
		"foo":     "not init",
		"bar":     "meow meow",
		"baz":     "from input",
	}

	var onto interface{} = map[string]interface{}{
		"started": "with this",
	}

	err = m.Overlay(map[string]interface{}{
		"input": "from input",
	}, &onto)
	require.NoError(t, err)
	assert.Equal(t, expected, onto)

	// Run it again and make sure our variables were reset.
	err = m.Overlay(map[string]interface{}{
		"input": "from input 2",
	}, &onto)
	require.NoError(t, err)
	expected["baz"] = "from input 2"
	assert.Equal(t, expected, onto)
}
