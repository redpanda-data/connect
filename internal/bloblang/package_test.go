package bloblang

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionExamples(t *testing.T) {
	tmpJSONFile, err := ioutil.TempFile("", "benthos_bloblang_functions_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.Write([]byte(`{"foo":"bar"}`))
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_FILE"
	os.Setenv(key, tmpJSONFile.Name())
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	for _, spec := range query.FunctionDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				m, err := NewMapping("", e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.New([][]byte{[]byte(io[0])})
					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
		})
	}
}

func TestMethodExamples(t *testing.T) {
	tmpJSONFile, err := ioutil.TempFile("", "benthos_bloblang_methods_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.Write([]byte(`
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}`))
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_SCHEMA_FILE"
	os.Setenv(key, tmpJSONFile.Name())
	t.Cleanup(func() {
		os.Unsetenv(key)
	})

	for _, spec := range query.MethodDocs() {
		spec := spec
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				m, err := NewMapping("", e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := message.New([][]byte{[]byte(io[0])})
					p, err := m.MapPart(0, msg)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)
						assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
			for _, target := range spec.Categories {
				for i, e := range target.Examples {
					m, err := NewMapping("", e.Mapping)
					require.NoError(t, err)

					for j, io := range e.Results {
						msg := message.New([][]byte{[]byte(io[0])})
						p, err := m.MapPart(0, msg)
						exp := io[1]
						if strings.HasPrefix(exp, "Error(") {
							exp = exp[7 : len(exp)-2]
							require.EqualError(t, err, exp, fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						} else {
							require.NoError(t, err)
							assert.Equal(t, exp, string(p.Get()), fmt.Sprintf("%v-%v-%v", target.Category, i, j))
						}
					}
				}
			}
		})
	}
}

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
			assert.Equal(t, test.queryTargets, m.QueryTargets(query.TargetsContext{
				Maps: map[string]query.Function{},
			}))

			res, err := m.Exec(query.FunctionContext{
				MsgBatch: message.New(nil),
				Vars:     map[string]interface{}{},
			}.WithValue(test.input))
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}
