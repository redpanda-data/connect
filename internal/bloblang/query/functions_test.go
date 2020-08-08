package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		output   interface{}
		err      string
		messages []easyMsg
		vars     map[string]interface{}
		index    int
	}{
		"check throw function 1": {
			input: `throw("foo")`,
			err:   "foo",
		},
		"check throw function 2": {
			input:  `throw("foo").catch("bar")`,
			output: "bar",
		},
		"check throw function 3": {
			input:  `if false { throw("foo") } else { "bar" }`,
			output: "bar",
		},
		"check throw function 4": {
			input: `if true { throw("foo") } else { "bar" }`,
			err:   "foo",
		},
		"check var function": {
			input:  `var("foo").uppercase()`,
			output: "FOOBAR",
			vars: map[string]interface{}{
				"foo": "foobar",
			},
		},
		"check var literal": {
			input:  `$foo.uppercase()`,
			output: "FOOBAR",
			vars: map[string]interface{}{
				"foo": "foobar",
			},
		},
		"check var function object": {
			input:  `var("foo").bar.uppercase()`,
			output: "FOOBAR",
			vars: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "foobar",
				},
			},
		},
		"check var function object 2": {
			input:  `var("foo").bar.baz.bev.uppercase()`,
			output: "FOOBAR",
			vars: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": map[string]interface{}{
						"baz": map[string]interface{}{
							"bev": "foobar",
						},
					},
				},
			},
		},
		"check var literal object": {
			input:  `$foo.bar.uppercase()`,
			output: "FOOBAR",
			vars: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "foobar",
				},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			msg := message.New(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.Metadata().Set(k, v)
					}
				}
				msg.Append(part)
			}

			e, perr := tryParse(test.input, false)
			require.Nil(t, perr)

			for i := 0; i < 10; i++ {
				res, err := e.Exec(FunctionContext{
					Vars:     test.vars,
					Maps:     map[string]Function{},
					Index:    test.index,
					MsgBatch: msg,
				})
				if len(test.err) > 0 {
					require.EqualError(t, err, test.err)
				} else {
					require.NoError(t, err)
				}
				assert.Equal(t, test.output, res)
			}

			// Ensure nothing changed
			for i, m := range test.messages {
				doc, err := msg.Get(i).JSON()
				if err == nil {
					msg.Get(i).SetJSON(doc)
				}
				assert.Equal(t, m.content, string(msg.Get(i).Get()))
			}
		})
	}
}

func TestRandomInt(t *testing.T) {
	e, err := tryParse(`random_int()`, false)
	require.Nil(t, err)

	tallies := map[int64]int64{}

	for i := 0; i < 100; i++ {
		res, err := e.Exec(FunctionContext{})
		require.NoError(t, err)
		require.IsType(t, int64(0), res)
		tallies[res.(int64)] = tallies[res.(int64)] + 1
	}

	// Can't prove it ain't random, but I can kick up a fuss if something
	// stinks.
	assert.GreaterOrEqual(t, len(tallies), 20)
	for _, v := range tallies {
		assert.LessOrEqual(t, v, int64(10))
	}
}
