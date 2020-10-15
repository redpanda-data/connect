package metrics

import (
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathMapping(t *testing.T) {
	type test struct {
		input       string
		output      string
		allowLabels bool
		labels      []string
		values      []string
	}
	tests := map[string][]test{
		`if this.contains("foo") { deleted() }`: {
			{
				input:  "foo",
				output: "",
			},
			{
				input:  "foo",
				output: "",
			},
			{
				input:  "hello foo world",
				output: "",
			},
			{
				input:  "hello world",
				output: "hello world",
			},
		},
		`root = this.foo.bar.not_null()`: {
			{input: "foo", output: "foo"},
		},
		`root = this
		 meta foo = "bar"`: {
			{input: "foo", output: "foo"},
			{
				input: "foo", output: "foo",
				allowLabels: true,
				labels:      []string{"foo"},
				values:      []string{"bar"},
			},
		},
		`root = this
		 meta foo = "bar"
		 meta bar = "baz"`: {
			{input: "foo", output: "foo"},
			{
				input: "foo", output: "foo",
				allowLabels: true,
				labels:      []string{"bar", "foo"},
				values:      []string{"baz", "bar"},
			},
		},
		`this.replace("foo","bar")`: {
			{input: "foo", output: "bar"},
			{input: "hello foo world", output: "hello bar world"},
			{input: "hello world", output: "hello world"},
		},
		`10`: {
			{input: "foo", output: "foo"},
			{input: "hello foo world", output: "hello foo world"},
			{input: "hello world", output: "hello world"},
		},
		``: {
			{input: "foo", output: "foo"},
			{input: "hello foo world", output: "hello foo world"},
			{input: "hello world", output: "hello world"},
		},
	}

	for mapping, defs := range tests {
		mapping := mapping
		defs := defs
		t.Run(mapping, func(t *testing.T) {
			m, err := newPathMapping(mapping, log.Noop())
			require.NoError(t, err)
			for i, def := range defs {
				out, labels, values := m.mapPath(def.input, def.allowLabels)
				assert.Equal(t, def.output, out, strconv.Itoa(i))
				assert.Equal(t, def.labels, labels, strconv.Itoa(i))
				assert.Equal(t, def.values, values, strconv.Itoa(i))
			}
		})
	}
}
