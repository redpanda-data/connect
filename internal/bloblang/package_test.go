package bloblang

import (
	"sync"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/message"
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
		"root copies to root": {
			mapping: `
root = this
root.first = root
root.second = root
`,
			input: map[string]interface{}{
				"foo": "bar",
			},
			output: map[string]interface{}{
				"foo": "bar",
				"first": map[string]interface{}{
					"foo": "bar",
				},
				"second": map[string]interface{}{
					"foo": "bar",
					"first": map[string]interface{}{
						"foo": "bar",
					},
				},
			},
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetValue),
				mapping.NewTargetPath(mapping.TargetValue, "first"),
				mapping.NewTargetPath(mapping.TargetValue, "second"),
			},
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue),
				query.NewTargetPath(query.TargetRoot),
				query.NewTargetPath(query.TargetRoot),
			},
		},
		"root edit from map": {
			mapping: `
map foo {
	root.from_map = "hello world"
	root = root.from_map
}
root = this
root.meow = this.apply("foo") 
`,
			input: map[string]interface{}{
				"foo": "bar",
			},
			output: map[string]interface{}{
				"foo":  "bar",
				"meow": "hello world",
			},
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetValue),
				mapping.NewTargetPath(mapping.TargetValue, "meow"),
			},
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue),
				query.NewTargetPath(query.TargetValue),
				query.NewTargetPath(query.TargetRoot, "from_map"),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			m, err := GlobalEnvironment().NewMapping(test.mapping)
			require.NoError(t, err)

			assert.Equal(t, test.assignmentTargets, m.AssignmentTargets())

			_, targets := m.QueryTargets(query.TargetsContext{
				Maps: m.Maps(),
			})
			assert.Equal(t, test.queryTargets, targets)

			res, err := m.Exec(query.FunctionContext{
				MsgBatch: message.QuickBatch(nil),
				Maps:     m.Maps(),
				Vars:     map[string]interface{}{},
			}.WithValue(test.input))
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}

func TestMappingParallelExecution(t *testing.T) {
	tests := map[string]struct {
		mapping string
		input   interface{}
		output  interface{}
	}{
		"basic query using vars": {
			mapping: `let tmp = this.foo.uppercase()
			root.first = $tmp
			let tmp = this.foo.lowercase()
			root.second = $tmp`,
			input: map[string]interface{}{
				"foo": "HELLO world",
			},
			output: map[string]interface{}{
				"first":  "HELLO WORLD",
				"second": "hello world",
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			m, err := GlobalEnvironment().NewMapping(test.mapping)
			require.NoError(t, err)

			startChan := make(chan struct{})

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-startChan

					for j := 0; j < 100; j++ {
						part := message.NewPart(nil)
						part.SetJSON(test.input)

						msg := message.QuickBatch(nil)
						msg.Append(part)

						p, err := m.MapPart(0, msg)
						require.NoError(t, err)

						res, err := p.JSON()
						require.NoError(t, err)

						assert.Equal(t, test.output, res)
					}
				}()
			}

			close(startChan)
			wg.Wait()
		})
	}
}
