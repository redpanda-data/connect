package mapping

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMappingErrors(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_mapping_errors")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	badMapFile := filepath.Join(dir, "bad_map.blobl")
	noMapsFile := filepath.Join(dir, "no_maps.blobl")
	goodMapFile := filepath.Join(dir, "good_map.blobl")

	require.NoError(t, ioutil.WriteFile(badMapFile, []byte(`not a map bruh`), 0777))
	require.NoError(t, ioutil.WriteFile(noMapsFile, []byte(`foo = "this is valid but has no maps"`), 0777))
	require.NoError(t, ioutil.WriteFile(goodMapFile, []byte(`map foo { foo = "this is valid" }`), 0777))

	tests := map[string]struct {
		mapping string
		err     string
	}{
		"no mappings": {
			mapping: ``,
			err:     `line 1 char 1: expected one of: [import map let meta target-path]`,
		},
		"no mappings 2": {
			mapping: `
   `,
			err: `line 2 char 4: expected one of: [import map let meta target-path]`,
		},
		"double mapping": {
			mapping: `foo = bar bar = baz`,
			err:     `line 1 char 11: expected: line-break`,
		},
		"double mapping line breaks": {
			mapping: `

foo = bar bar = baz

`,
			err: `line 3 char 11: expected: line-break`,
		},
		"double mapping line 2": {
			mapping: `let a = "a"
foo = bar bar = baz`,
			err: `line 2 char 11: expected: line-break`,
		},
		"double mapping line 3": {
			mapping: `let a = "a"
foo = bar bar = baz
	let a = "a"`,
			err: "line 2 char 11: expected: line-break",
		},
		"bad mapping": {
			mapping: `foo wat bar`,
			err:     `line 1 char 5: expected: =`,
		},
		"bad char": {
			mapping: `!foo = bar`,
			err:     `line 1 char 6: expected: end-of-input`,
		},
		"bad char 2": {
			mapping: `let foo = bar
!foo = bar`,
			err: `line 2 char 1: expected one of: [import map let meta target-path]`,
		},
		"bad char 3": {
			mapping: `let foo = bar
!foo = bar
this = that`,
			err: `line 2 char 1: expected one of: [import map let meta target-path]`,
		},
		"bad query": {
			mapping: `foo = blah.`,
			err:     `line 1 char 12: required one of: [method field-path]`,
		},
		"bad variable assign": {
			mapping: `let = blah`,
			err:     `line 1 char 5: required: variable-name`,
		},
		"double map definition": {
			mapping: `map foo {
  foo = bar
}
map foo {
  foo = bar
}
foo = bar.apply("foo")`,
			err: `line 4 char 1: map name collision: foo`,
		},
		"map contains meta assignment": {
			mapping: `map foo {
  meta foo = "bar"
}
foo = bar.apply("foo")`,
			err: `line 2 char 3: setting meta fields from within a map is not allowed`,
		},
		"no name map definition": {
			mapping: `map {
  foo = bar
}
foo = bar.apply("foo")`,
			err: `line 1 char 5: required: map-name`,
		},
		"no file import": {
			mapping: `import "this file doesnt exist (i hope)"

foo = bar.apply("from_import")`,
			err: `line 1 char 1: failed to read import: open this file doesnt exist (i hope): no such file or directory`,
		},
		"bad file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, badMapFile),
			err: fmt.Sprintf(`line 1 char 1: failed to parse import '%v': line 1 char 5: expected: =`, badMapFile),
		},
		"no maps file import": {
			mapping: fmt.Sprintf(`import "%v"

foo = bar.apply("from_import")`, noMapsFile),
			err: fmt.Sprintf(`line 1 char 1: no maps to import from '%v'`, noMapsFile),
		},
		"colliding maps file import": {
			mapping: fmt.Sprintf(`map "foo" { this = that }			

import "%v"

foo = bar.apply("foo")`, goodMapFile),
			err: fmt.Sprintf(`line 3 char 1: map name collisions from import '%v': [foo]`, goodMapFile),
		},
		"quotes at root": {
			mapping: `
"root.something" = 5 + 2`,
			err: "line 2 char 1: expected one of: [import map let meta target-path]",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			exec, err := NewExecutor(test.mapping)
			assert.EqualError(t, err, test.err)
			assert.Nil(t, exec)
		})
	}
}

func TestMappings(t *testing.T) {
	dir, err := ioutil.TempDir("", "benthos_mapping")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	goodMapFile := filepath.Join(dir, "foo_map.blobl")
	require.NoError(t, ioutil.WriteFile(goodMapFile, []byte(`map foo {
  foo = "this is valid"
  nested = this
}`), 0777))

	type part struct {
		Content string
		Meta    map[string]string
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping string
		output  part
	}{
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
					Meta: map[string]string{
						"bar baz": "test1",
					},
				},
			},
			output: part{
				Content: `{"bar":{"baz":"test1"}}`,
				Meta: map[string]string{
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
				Meta: map[string]string{
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
					Meta: map[string]string{
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
				Meta: map[string]string{
					"foo": "foo",
					"bar": "7",
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
				Meta: map[string]string{
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
				Meta: map[string]string{
					"content": `hello world`,
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
				Meta: map[string]string{
					"content": `{"foo":{"bar":"baz"}}`,
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
				{Content: `{"outter":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["foo"],"bar":{"outter":{"inner":"hello world"}},"foo":"static foo"}`,
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
				{Content: `{"outter":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"applied":["bar","foo"],"foo":{"bar":{"outter":{"inner":"hello world"}},"static":"this is valid"}}`,
			},
		},
		"test imported map": {
			mapping: fmt.Sprintf(`import "%v"

root = this.apply("foo")`, goodMapFile),
			input: []part{
				{Content: `{"outter":{"inner":"hello world"}}`},
			},
			output: part{
				Content: `{"foo":"this is valid","nested":{"outter":{"inner":"hello world"}}}`,
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.New(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				for k, v := range p.Meta {
					part.Metadata().Set(k, v)
				}
				msg.Append(part)
			}
			if test.output.Meta == nil {
				test.output.Meta = map[string]string{}
			}

			exec, err := NewExecutor(test.mapping)
			require.NoError(t, err)

			resPart, err := exec.MapPart(test.index, msg)
			require.NoError(t, err)

			newPart := part{
				Content: string(resPart.Get()),
				Meta:    map[string]string{},
			}
			resPart.Metadata().Iter(func(k, v string) error {
				newPart.Meta[k] = v
				return nil
			})

			assert.Equal(t, test.output, newPart)
		})
	}
}
