package bloblang

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMappings(t *testing.T) {
	tests := map[string]struct {
		mapping           string
		input             interface{}
		output            interface{}
		assignmentTargets []mapping.TargetPath
		queryTargets      []query.TargetPath
	}{
		"basic query": {
			mapping: `root = this.foo
			let bar = $baz | this.bar.baz`,
			input: map[string]interface{}{
				"foo": "bar",
			},
			output: "bar",
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetValue),
				mapping.NewTargetPath(mapping.TargetVariable, "bar"),
			},
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue, "foo"),
				query.NewTargetPath(query.TargetVariable, "baz"),
				query.NewTargetPath(query.TargetValue, "bar", "baz"),
			},
		},
		"complex query": {
			mapping: `root = match this.foo {
				this.bar == "bruh" => this.baz.buz,
				_ => $foo
			}`,
			input: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "bruh",
					"baz": map[string]interface{}{
						"buz": "the result",
					},
				},
			},
			output: "the result",
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetValue),
			},
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue, "foo", "bar"),
				query.NewTargetPath(query.TargetValue, "foo", "baz", "buz"),
				query.NewTargetPath(query.TargetVariable, "foo"),
				query.NewTargetPath(query.TargetValue, "foo"),
			},
		},
		"long assignment": {
			mapping: `root.foo.bar = "this"
			root.foo = "that"
			root.baz.buz.0.bev = "then this"`,
			output: map[string]interface{}{
				"foo": "that",
				"baz": map[string]interface{}{
					"buz": map[string]interface{}{
						"0": map[string]interface{}{
							"bev": "then this",
						},
					},
				},
			},
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetValue, "foo", "bar"),
				mapping.NewTargetPath(mapping.TargetValue, "foo"),
				mapping.NewTargetPath(mapping.TargetValue, "baz", "buz", "0", "bev"),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			m, err := NewMapping("", test.mapping)
			require.NoError(t, err)

			assert.Equal(t, test.assignmentTargets, m.AssignmentTargets())
			assert.Equal(t, test.queryTargets, m.QueryTargets())

			res, err := m.Exec(query.FunctionContext{
				Value:    &test.input,
				MsgBatch: message.New(nil),
				Vars:     map[string]interface{}{},
			})
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}
