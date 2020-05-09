package mapping

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMappingErrors(t *testing.T) {
	tests := map[string]struct {
		mapping string
		err     string
	}{
		"no mappings": {
			mapping: ``,
			err:     `failed to parse mapping: line 1 char 1: expected one of: [map let meta target-path]`,
		},
		"no mappings 2": {
			mapping: `
   `,
			err: `failed to parse mapping: line 2 char 4: expected one of: [map let meta target-path]`,
		},
		"double mapping": {
			mapping: `foo = bar bar = baz`,
			err:     `failed to parse mapping: line 1 char 11: expected: line-break`,
		},
		"double mapping line 2": {
			mapping: `let a = "a"
foo = bar bar = baz`,
			err: `failed to parse mapping: line 2 char 11: expected: line-break`,
		},
		"double mapping line 3": {
			mapping: `let a = "a"
foo = bar bar = baz
	let a = "a"`,
			err: "failed to parse mapping: line 2 char 11: expected: line-break",
		},
		"bad mapping": {
			mapping: `foo wat bar`,
			err:     `failed to parse mapping: line 1 char 5: expected: =`,
		},
		"bad char": {
			mapping: `!foo = bar`,
			err:     `failed to parse mapping: line 1 char 1: expected one of: [map let meta target-path]`,
		},
		"bad char 2": {
			mapping: `let foo = bar
!foo = bar`,
			err: `failed to parse mapping: line 2 char 1: expected one of: [map let meta target-path]`,
		},
		"bad char 3": {
			mapping: `let foo = bar
!foo = bar
this = that`,
			err: `failed to parse mapping: line 2 char 1: expected one of: [map let meta target-path]`,
		},
		"bad query": {
			mapping: `foo = blah.`,
			err:     `failed to parse mapping: line 1 char 12: required one of: [method field-path]`,
		},
		"bad variable assign": {
			mapping: `let = blah`,
			err:     `failed to parse mapping: line 1 char 5: required: variable-name`,
		},
		"double map definition": {
			mapping: `map foo {
  foo = bar
}
map foo {
  foo = bar
}
foo = bar.apply("foo")`,
			err: `failed to parse mapping: line 4 char 1: map name collision: foo`,
		},
		"no name map definition": {
			mapping: `map {
  foo = bar
}
foo = bar.apply("foo")`,
			err: `failed to parse mapping: line 1 char 5: required: map-name`,
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
	type part struct {
		Content string
		Meta    map[string]string
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping string
		output  []part
	}{
		"simple json map": {
			mapping: `foo = foo + 2
bar = "test1"
zed = deleted()`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: []part{{Content: `{"bar":"test1","foo":12}`}},
		},
		"simple json map 2": {
			mapping: `
foo = foo + 2

bar = "test1"

zed = deleted()
`,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: []part{{Content: `{"bar":"test1","foo":12}`}},
		},
		"simple json map 3": {
			mapping: `  
  foo = foo + 2
      
   bar = "test1"

zed = deleted()   
  `,
			input:  []part{{Content: `{"foo":10,"zed":"gone"}`}},
			output: []part{{Content: `{"bar":"test1","foo":12}`}},
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
			output: []part{{Content: `{"bar":"test1","foo":12}`}},
		},
		"test mapping metadata and json": {
			mapping: `meta foo = foo
meta "bar baz" = "test1"
bar.baz = meta("bar baz")
meta "bar baz" = deleted()`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: []part{
				{
					Content: `{"bar":{"baz":"test1"}}`,
					Meta: map[string]string{
						"foo": "bar",
					},
				},
			},
		},
		"test mapping metadata and json 2": {
			mapping: `meta = foo
meta "bar baz" = "test1"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: []part{
				{
					Content: `{"foo":{"bar":"baz"}}`,
					Meta: map[string]string{
						"bar":     "baz",
						"bar baz": "test1",
					},
				},
			},
		},
		"test mapping delete and json": {
			mapping: `meta foo = foo
meta "bar baz" = "test1"
bar.baz = meta("bar baz")
meta = deleted()`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: []part{
				{Content: `{"bar":{"baz":"test1"}}`},
			},
		},
		"test variables and json": {
			mapping: `let foo = foo
let "bar baz" = "test1"
bar.baz = var("bar baz")`,
			input: []part{
				{Content: `{"foo":"bar"}`},
			},
			output: []part{
				{Content: `{"bar":{"baz":"test1"}}`},
			},
		},
		"map json root": {
			mapping: `root = {
  "foo": "this is a literal map"
}`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: []part{{Content: `{"foo":"this is a literal map"}`}},
		},
		"map json root 2": {
			mapping: `root = {
  "foo": "this is a literal map"
}
bar = "this is another thing"`,
			input:  []part{{Content: `{"zed":"gone"}`}},
			output: []part{{Content: `{"bar":"this is another thing","foo":"this is a literal map"}`}},
		},
		"test mapping metadata without json": {
			mapping: `meta foo = "foo"
meta bar = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: []part{
				{
					Content: `this isn't json`,
					Meta: map[string]string{
						"foo": "foo",
						"bar": "7",
					},
				},
			},
		},
		"field called root": {
			mapping: `root.root = "not set at root"`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: []part{
				{Content: `{"root":"not set at root"}`},
			},
		},
		"quoted paths": {
			mapping: `
meta "foo bar" = "hello world"
"root.bar baz.test" = 5 + 2`,
			input: []part{
				{Content: `this isn't json`},
			},
			output: []part{
				{
					Content: `{"bar baz":{"test":7}}`,
					Meta: map[string]string{
						"foo bar": "hello world",
					},
				},
			},
		},
		"test mapping raw content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `hello world`},
			},
			output: []part{
				{
					Content: `{"foo":"static"}`,
					Meta: map[string]string{
						"content": `hello world`,
					},
				},
			},
		},
		"test mapping raw json content": {
			mapping: `meta content = content()
foo = "static"`,
			input: []part{
				{Content: `{"foo":{"bar":"baz"}}`},
			},
			output: []part{
				{
					Content: `{"foo":"static"}`,
					Meta: map[string]string{
						"content": `{"foo":{"bar":"baz"}}`,
					},
				},
			},
		},
		"test maps": {
			mapping: `map foo {
  meta "map applied" = "true"
  foo = "static foo"
  bar = this
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outter":{"inner":"hello world"}}`},
			},
			output: []part{
				{
					Content: `{"bar":{"outter":{"inner":"hello world"}},"foo":"static foo"}`,
					Meta: map[string]string{
						"map applied": `true`,
					},
				},
			},
		},
		"test nested maps": {
			mapping: `map foo {
  meta "foo applied" = "true"
  foo = this.apply("bar")
}
map bar {
  meta "bar applied" = "true"
  bar = this
}
root = this.apply("foo")`,
			input: []part{
				{Content: `{"outter":{"inner":"hello world"}}`},
			},
			output: []part{
				{
					Content: `{"foo":{"bar":{"outter":{"inner":"hello world"}}}}`,
					Meta: map[string]string{
						"foo applied": `true`,
						"bar applied": `true`,
					},
				},
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
			for i, o := range test.output {
				if o.Meta == nil {
					o.Meta = map[string]string{}
					test.output[i] = o
				}
			}

			exec, err := NewExecutor(test.mapping)
			require.NoError(t, err)

			err = exec.MapPart(test.index, msg)
			require.NoError(t, err)

			resParts := []part{}
			msg.Iter(func(i int, p types.Part) error {
				newPart := part{
					Content: string(p.Get()),
					Meta:    map[string]string{},
				}
				p.Metadata().Iter(func(k, v string) error {
					newPart.Meta[k] = v
					return nil
				})

				resParts = append(resParts, newPart)
				return nil
			})

			assert.Equal(t, test.output, resParts)
		})
	}
}
