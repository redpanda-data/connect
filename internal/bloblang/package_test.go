package bloblang

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestMappings(t *testing.T) {
	tests := map[string]struct {
		mapping           string
		input             any
		output            any
		assignmentTargets []mapping.TargetPath
		queryTargets      []query.TargetPath
	}{
		"basic query": {
			mapping: `root = this.foo
			let bar = $baz | this.bar.baz`,
			input: map[string]any{
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
		"metadata stuff": {
			mapping: `
meta foo = this.foo
meta bar = @bar
meta baz = @
meta buz = @bar.string().length()
root.keys = @.keys().sort()
root.meta = @
`,
			input: map[string]any{
				"foo": "bar",
			},
			output: map[string]any{
				"keys": []any{"bar", "baz", "buz", "foo"},
				"meta": map[string]any{
					"foo": "bar",
					"bar": nil,
					"baz": map[string]any{
						"foo": "bar",
						"bar": nil,
					},
					"buz": int64(4),
				},
			},
			assignmentTargets: []mapping.TargetPath{
				mapping.NewTargetPath(mapping.TargetMetadata, "foo"),
				mapping.NewTargetPath(mapping.TargetMetadata, "bar"),
				mapping.NewTargetPath(mapping.TargetMetadata, "baz"),
				mapping.NewTargetPath(mapping.TargetMetadata, "buz"),
				mapping.NewTargetPath(mapping.TargetValue, "keys"),
				mapping.NewTargetPath(mapping.TargetValue, "meta"),
			},
			queryTargets: []query.TargetPath{
				query.NewTargetPath(query.TargetValue, "foo"),
				query.NewTargetPath(query.TargetMetadata, "bar"),
				query.NewTargetPath(query.TargetMetadata),
				query.NewTargetPath(query.TargetMetadata, "bar"),
				query.NewTargetPath(query.TargetMetadata),
				query.NewTargetPath(query.TargetMetadata),
			},
		},
		"complex query": {
			mapping: `root = match this.foo {
				this.bar == "bruh" => this.baz.buz,
				_ => $foo
			}`,
			input: map[string]any{
				"foo": map[string]any{
					"bar": "bruh",
					"baz": map[string]any{
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
			output: map[string]any{
				"foo": "that",
				"baz": map[string]any{
					"buz": map[string]any{
						"0": map[string]any{
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
			input: map[string]any{
				"foo": "bar",
			},
			output: map[string]any{
				"foo": "bar",
				"first": map[string]any{
					"foo": "bar",
				},
				"second": map[string]any{
					"foo": "bar",
					"first": map[string]any{
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
			input: map[string]any{
				"foo": "bar",
			},
			output: map[string]any{
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

			part := message.NewPart(nil)
			part.SetStructuredMut(test.input)

			resPart, err := m.MapPart(0, message.Batch{part})
			require.NoError(t, err)

			res, err := resPart.AsStructured()
			if err != nil {
				res = string(resPart.AsBytes())
			}
			assert.Equal(t, test.output, res)
		})
	}
}

func TestMappingParallelExecution(t *testing.T) {
	tests := map[string]struct {
		mapping string
		input   any
		output  any
	}{
		"basic query using vars": {
			mapping: `let tmp = this.foo.uppercase()
			root.first = $tmp
			let tmp = this.foo.lowercase()
			root.second = $tmp`,
			input: map[string]any{
				"foo": "HELLO world",
			},
			output: map[string]any{
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
						part.SetStructured(test.input)

						msg := message.QuickBatch(nil)
						msg = append(msg, part)

						p, err := m.MapPart(0, msg)
						require.NoError(t, err)

						res, err := p.AsStructuredMut()
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
