package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		value    *interface{}
		output   interface{}
		err      string
		messages []easyMsg
		index    int
	}{
		"check if inline false": {
			input:  `if 10 > 20 { "foo" }`,
			output: Nothing(nil),
		},
		"check if inline true": {
			input:  `if 30 > 20 { "foo" }`,
			output: "foo",
		},
		"check if/else inline false": {
			input:  `if 10 > 20 { "foo" } else { "bar" }`,
			output: "bar",
		},
		"check if/else inline true": {
			input:  `if 30 > 20 { "foo" } else { "bar" }`,
			output: "foo",
		},
		"check if/else inline error": {
			input: `if "foo" > 10 { "foo" } else { "bar" }`,
			err:   "failed to check if condition: expected string value, found number: 10",
		},
		"check if/else inline false compressed": {
			input:  `if 10 > 20{"foo"}else{"bar"}`,
			output: "bar",
		},
		"check if/else expanded false": {
			input: `if 10 > 20 {
  "foo"
} else {
  "bar"
}`,
			output: "bar",
		},
		"check if/else expanded more false": {
			input: `if 10 > 20
{
  "foo"
}
else
{
  "bar"
}`,
			output: "bar",
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
			require.Nil(t, err)

			for i := 0; i < 10; i++ {
				res, err := e.Exec(FunctionContext{
					Value:    test.value,
					Maps:     map[string]Function{},
					Index:    test.index,
					MsgBatch: msg,
				})
				if len(test.err) > 0 {
					require.EqualError(t, err, test.err)
				} else {
					require.NoError(t, err)
				}
				if !assert.Equal(t, test.output, res) {
					break
				}
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
