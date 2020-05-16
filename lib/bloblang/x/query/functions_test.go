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

			e, err := tryParse(test.input, false)
			require.NoError(t, err)

			res, err := e.Exec(FunctionContext{
				Vars:  test.vars,
				Maps:  map[string]Function{},
				Index: test.index,
				Msg:   msg,
			})
			if len(test.err) > 0 {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.output, res)

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
