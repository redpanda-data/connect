package metrics

import (
	"strconv"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathMapping(t *testing.T) {
	type testCase struct {
		input    string
		inLabels []string
		inValues []string
		output   string
		labels   []string
		values   []string
	}
	type test struct {
		name    string
		mapping string
		cases   []testCase
	}
	tests := []test{
		{
			name:    "delete some",
			mapping: `if this.contains("foo") { deleted() }`,
			cases: []testCase{
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
				{
					input:    "hello world",
					inLabels: []string{"foo", "bar"},
					inValues: []string{"foo1", "bar1"},
					output:   "hello world",
					labels:   []string{"bar", "foo"},
					values:   []string{"bar1", "foo1"},
				},
			},
		},
		{
			name:    "throw an error",
			mapping: `root = throw("nope")`,
			cases:   []testCase{{input: "foo", output: "foo"}},
		},
		{
			name: "set a static label",
			mapping: `root = this
			 meta foo = "bar"`,
			cases: []testCase{
				{
					input: "foo", output: "foo",
					labels: []string{"foo"},
					values: []string{"bar"},
				},
				{
					input: "foo", output: "foo",
					inLabels: []string{"a", "b"},
					inValues: []string{"a1", "b1"},
					labels:   []string{"a", "b", "foo"},
					values:   []string{"a1", "b1", "bar"},
				},
			},
		},
		{
			name: "set two static labels",
			mapping: `root = this
			 meta foo = "bar"
			 meta bar = "baz"`,
			cases: []testCase{
				{
					input: "foo", output: "foo",
					labels: []string{"bar", "foo"},
					values: []string{"baz", "bar"},
				},
			},
		},
		{
			name:    "replace foo with bar",
			mapping: `this.replace("foo","bar")`,
			cases: []testCase{
				{input: "foo", output: "bar"},
				{input: "hello foo world", output: "hello bar world"},
				{input: "hello world", output: "hello world"},
			},
		},
		{
			name:    "empty mapping",
			mapping: ``,
			cases: []testCase{
				{input: "foo", output: "foo"},
				{input: "hello world", output: "hello world"},
			},
		},
		{
			name:    "wrong value mapping",
			mapping: `root = 10`,
			cases: []testCase{
				{input: "foo", output: "foo"},
				{input: "hello world", output: "hello world"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, err := NewMapping(mock.NewManager(), test.mapping, log.Noop())
			require.NoError(t, err)
			for i, def := range test.cases {
				out, labels, values := m.mapPath(def.input, def.inLabels, def.inValues)
				assert.Equal(t, def.output, out, strconv.Itoa(i))
				assert.Equal(t, def.labels, labels, strconv.Itoa(i))
				assert.Equal(t, def.values, values, strconv.Itoa(i))
			}
		})
	}
}
