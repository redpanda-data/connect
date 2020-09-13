package bloblang

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMappings(t *testing.T) {
	tests := map[string]struct {
		mapping string
		input   string
		output  string
	}{
		"basic query": {
			mapping: `root = this.foo
			let bar = $baz | this.bar.baz`,
			input:  `{"foo":"bar"}`,
			output: "bar",
		},
		"complex query": {
			mapping: `root = match this.foo {
				this.bar == "bruh" => this.baz.buz,
				_ => $foo
			}`,
			input:  `{"foo":{"bar":"bruh","baz":{"buz":"the result"}}}`,
			output: "the result",
		},
		"long assignment": {
			mapping: `root.foo.bar = "this"
			root.foo = "that"
			root.baz.buz.0.bev = "then this"`,
			output: `{"baz":{"buz":{"0":{"bev":"then this"}}},"foo":"that"}`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			m, err := NewMapping(test.mapping)
			require.NoError(t, err)

			res, err := m.MapPart(0, message.New([][]byte{[]byte(test.input)}))
			require.NoError(t, err)
			assert.Equal(t, test.output, string(res.Get()))
		})
	}
}

func TestFields(t *testing.T) {
	tests := map[string]struct {
		field  string
		input  string
		output string
	}{
		"basic query": {
			field:  `foo ${! json("foo") }`,
			input:  `{"foo":"bar"}`,
			output: "foo bar",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			f, err := NewField(test.field)
			require.NoError(t, err)

			res := f.String(0, message.New([][]byte{[]byte(test.input)}))
			assert.Equal(t, test.output, res)
		})
	}
}
