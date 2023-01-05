package parser

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestMappingErrors(t *testing.T) {
	dir := t.TempDir()

	badMapFile := filepath.Join(dir, "bad_map.blobl")
	noMapsFile := filepath.Join(dir, "no_maps.blobl")
	goodMapFile := filepath.Join(dir, "good_map.blobl")

	require.NoError(t, os.WriteFile(badMapFile, []byte(`not a map bruh`), 0o777))
	require.NoError(t, os.WriteFile(noMapsFile, []byte(`foo = "this is valid but has no maps"`), 0o777))
	require.NoError(t, os.WriteFile(goodMapFile, []byte(`map foo { foo = "this is valid" }`), 0o777))

	tests := map[string]struct {
		mapping     string
		errContains string
	}{
		"bad variable name": {
			mapping:     `let foo+bar = baz`,
			errContains: "line 1 char 8: expected whitespace",
		},
		"bad meta name": {
			mapping:     `meta foo+bar = baz`,
			errContains: "line 1 char 9: expected =",
		},
		"no mappings": {
			mapping:     ``,
			errContains: `line 1 char 1: expected import, map, or assignment`,
		},
		"no mappings 2": {
			mapping: `
   `,
			errContains: `line 2 char 4: expected import, map, or assignment`,
		},
		"double mapping": {
			mapping:     `foo = bar bar = baz`,
			errContains: `line 1 char 11: expected line break`,
		},
		"double mapping line breaks": {
			mapping: `

foo = bar bar = baz

`,
			errContains: `line 3 char 11: expected line break`,
		},
		"double mapping line 2": {
			mapping: `let a = "a"
foo = bar bar = baz`,
			errContains: `line 2 char 11: expected line break`,
		},
		"double mapping line 3": {
			mapping: `let a = "a"
foo = bar bar = baz
	let a = "a"`,
			errContains: "line 2 char 11: expected line break",
		},
		"bad mapping": {
			mapping:     `foo wat bar`,
			errContains: `line 1 char 5: expected =`,
		},
		"bad char": {
			mapping:     `!foo = bar`,
			errContains: "line 1 char 6: expected the mapping to end here as the beginning is shorthand for `root = !foo`, but this shorthand form cannot be followed with more assignments",
		},
		"bad inline query": {
			mapping: `content().uppercase().lowercase()
meta foo = "bar"`,
			errContains: "line 2 char 1: expected the mapping to end here as the beginning is shorthand for `root = content().up...`, but this shorthand form cannot be followed with more assignments",
		},
		"bad char 2": {
			mapping: `let foo = bar
!foo = bar`,
			errContains: `line 2 char 1: expected import, map, or assignment`,
		},
		"bad char 3": {
			mapping: `let foo = bar
!foo = bar
this = that`,
			errContains: `line 2 char 1: expected import, map, or assignment`,
		},
		"bad query": {
			mapping:     `foo = blah.`,
			errContains: `line 1 char 12: required: expected method or field path`,
		},
		"bad variable assign": {
			mapping:     `let = blah`,
			errContains: `line 1 char 5: required: expected variable name`,
		},
		"double map definition": {
			mapping: `map foo {
  foo = bar
}
map foo {
  foo = bar
}
foo = bar.apply("foo")`,
			errContains: `line 4 char 1: map name collision: foo`,
		},
		"map contains meta assignment": {
			mapping: `map foo {
  meta foo = "bar"
}
foo = bar.apply("foo")`,
			errContains: `line 2 char 3: setting meta fields from within a map is not allowed`,
		},
		"no name map definition": {
			mapping: `map {
  foo = bar
}
foo = bar.apply("foo")`,
			errContains: `line 1 char 5: required: expected map name`,
		},
		"no file import": {
			mapping: `import "this file doesnt exist (i hope)"

foo = bar.apply("from_import")`,
			errContains: `this file doesnt exist (i hope): no such file or directory`,
		},
		"bad file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, badMapFile),
			errContains: fmt.Sprintf(`line 1 char 1: failed to parse import '%v': line 1 char 5: expected =`, badMapFile),
		},
		"no maps file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, noMapsFile),
			errContains: fmt.Sprintf(`line 1 char 1: no maps to import from '%v'`, noMapsFile),
		},
		"colliding maps file import": {
			mapping: fmt.Sprintf(`map "foo" { this = that }			

import "%v"

foo = bar.apply("foo")`, goodMapFile),
			errContains: fmt.Sprintf(`line 3 char 1: map name collisions from import '%v': [foo]`, goodMapFile),
		},
		"quotes at root": {
			mapping: `
"root.something" = 5 + 2`,
			errContains: "line 2 char 1: expected import, map, or assignment",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			exec, err := ParseMapping(GlobalContext(), test.mapping)
			require.NotNil(t, err)
			assert.Contains(t, err.ErrorAtPosition([]rune(test.mapping)), test.errContains)
			assert.Nil(t, exec)
		})
	}
}

func TestMappings(t *testing.T) {
	dir := t.TempDir()

	goodMapFile := filepath.Join(dir, "foo_map.blobl")
	require.NoError(t, os.WriteFile(goodMapFile, []byte(`map foo {
  foo = "this is valid"
  nested = this
}`), 0o777))

	directMapFile := filepath.Join(dir, "direct_map.blobl")
	require.NoError(t, os.WriteFile(directMapFile, []byte(`root.nested = this`), 0o777))

	type part struct {
		Content string
		Meta    map[string]any
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping string
		output  part
	}{
		"compressed arithmetic": {
			mapping: `this.foo+this.bar`,
			input: []part{
				{Content: `{"foo":5,"bar":3}`},
			},
			output: part{
				Content: `8`,
			},
		},
		"compressed arithmetic 2": {
			mapping: `this.foo-this.bar`,
			input: []part{
				{Content: `{"foo":5,"bar":3}`},
			},
			output: part{
				Content: `2`,
			},
		},
		"simple json map": {
			mapping: `foo = foo + 2
bar = "test1"
zed = deleted()`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple json map 2": {
			mapping: `
foo = foo + 2

bar = "test1"

zed = deleted()
`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple json map 3": {
			mapping: `  
  foo = foo + 2
      
   bar = "test1"

zed = deleted()   
  `,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"simple root query": {
			mapping: `{"result": foo + 2}`,
			input:   []part{{Content: `{"foo":10}`}},
			output:  part{Content: `{"result":12}`},
		},
		"simple root query 2": {
			mapping: `foo.bar`,
			input:   []part{{Content: `{"foo":{"bar":10}}`}},
			output:  part{Content: `10`},
		},
		"simple root query 3": {
			mapping: `root = foo.bar`,
			input:   []part{{Content: `{"foo":{"bar":10}}`}},
			output:  part{Content: `10`},
		},
		"simple json map with comments": {
			mapping: `
# Here's a comment
foo = foo + 2 # And here

bar = "test1"         # And one here

# And here
zed = deleted()
`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: part{Content: `{"bar":"test1","foo":12}`},
		},
		"test mapping metadata and json": {
			mapping: `meta foo = foo
bar.baz = meta("bar baz")
meta "bar baz" = deleted()`,
			input: []part{
				{
					Content: `{"foo":"bar"}`,
					Meta: map[string]any{
						"bar baz": "test1",
					},
				},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
				Meta: map[string]any{
					"foo": "bar",
				},
			},
		},
		"test mapping metadata and json 2": {
			mapping: `meta = foo
meta "bar baz" = "test1"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: part{
				Content: `{"foo":{"bar":"baz"}}`,
				Meta: map[string]any{
					"bar":     "baz",
					"bar baz": "test1",
				},
			},
		},
		"test mapping delete and json": {
			mapping: `meta foo = foo
bar.baz = meta("bar baz")
meta = deleted()`,
			input: []part{
				{
					Content: `{"foo":"bar"}`,
					Meta: map[string]any{
						"bar baz": "test1",
					},
				},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
			},
		},
		"test variables and json": {
			mapping: `let foo = foo
let "bar baz" = "test1"
bar.baz = var("bar baz")`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
			},
		},
		"map json root": {
			mapping: `root = {
  "foo": "this is a literal map"
}`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: part{Content: `{"foo":"this is a literal map"}`},
		},
		"map json root 2": {
			mapping: `root = {
  "foo": "this is a literal map"
}
bar = "this is another thing"`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: part{Content: `{"bar":"this is another thing","foo":"this is a literal map"}`},
		},
		"test mapping metadata without json": {
			mapping: `meta foo = "foo"
meta bar = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `this isn't json`,
				Meta: map[string]any{
					"foo": "foo",
					"bar": int64(7),
				},
			},
		},
		"field called root": {
			mapping: `root.root = "not set at root"`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `{"root":"not set at root"}`,
			},
		},
		"quoted paths": {
			mapping: `
meta "foo bar" = "hello world"
root."bar baz".test = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: part{
				Content: `{"bar baz":{"test":7}}`,
				Meta: map[string]any{
					"foo bar": "hello world",
				},
			},
		},
		"test mapping raw content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `hello world`},
			},
			output: part{
				Content: `{"foo":"static"}`,
				Meta: map[string]any{
					"content": []byte(`hello world`),
				},
			},
		},
		"test mapping raw json content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: part{
				Content: `{"foo":"static"}`,
				Meta: map[string]any{
					"content": []byte(`{"foo":{"bar":"baz"}}`),
				},
			},
		},
		"test mapping to string": {
			mapping: `root = "static string"`,
			input: []part{
				{Content: `{"this":"is a json doc"}`},
			},
			output: part{
				Content: `static string`,
			},
		},
		"test maps": {
			mapping: `map foo {
  foo = "static foo"
  bar = this
  applied = ["foo"]
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["foo"],"bar":{"outer":{"inner":"hello world"}},"foo":"static foo"}`,
			},
		},
		"test nested maps": {
			mapping: `map foo {
  let tmp = this.apply("bar")
  foo = var("tmp")
  applied = var("tmp").applied.merge("foo")
  foo.applied = deleted()
}
map bar {
  static = "this is valid"
  bar = this
  applied = ["bar"]
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["bar","foo"],"foo":{"bar":{"outer":{"inner":"hello world"}},"static":"this is valid"}}`,
			},
		},
		"test imported map": {
			mapping: fmt.Sprintf(`import "%v"

root = this.apply("foo")`, goodMapFile),
			input: []part{
				{Content: `{"outer":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"foo":"this is valid","nested":{"outer":{"inner":"hello world"}}}`,
			},
		},
		"test directly imported map": {
			mapping: fmt.Sprintf(`from "%v"`, directMapFile),
			input: []part{
				{Content: `{"inner":"hello world"}`},
			},
			output: part{
				Content: `{"nested":{"inner":"hello world"}}`,
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.QuickBatch(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				for k, v := range p.Meta {
					part.MetaSetMut(k, v)
				}
				msg = append(msg, part)
			}
			if test.output.Meta == nil {
				test.output.Meta = map[string]any{}
			}

			exec, perr := ParseMapping(GlobalContext(), test.mapping)
			require.Nil(t, perr)

			resPart, err := exec.MapPart(test.index, msg)
			require.NoError(t, err)

			newPart := part{
				Content: string(resPart.AsBytes()),
				Meta:    map[string]any{},
			}
			_ = resPart.MetaIterMut(func(k string, v any) error {
				newPart.Meta[k] = v
				return nil
			})

			assert.Equal(t, test.output, newPart)
		})
	}
}
