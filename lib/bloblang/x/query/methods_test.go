package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMethods(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		output   interface{}
		err      string
		messages []easyMsg
		index    int
	}{
		"check merge": {
			input: `{"foo":"val1"}.merge({"bar":"val2"})`,
			output: map[string]interface{}{
				"foo": "val1",
				"bar": "val2",
			},
		},
		"check merge 2": {
			input: `json().(foo.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]interface{}{
				"first":  "val1",
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6)},
			},
		},
		"check merge 3": {
			input: `json().(this.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]interface{}{
				"foo": map[string]interface{}{
					"first": "val1",
					"third": float64(3),
				},
				"bar": map[string]interface{}{
					"second": "val2",
					"third":  float64(6),
				},
				"second": "val2",
				"third":  float64(6),
			},
		},
		"check merge 4": {
			input: `json().(foo.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]interface{}{
				"first":  "val1",
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6)},
			},
		},
		"check merge 5": {
			input: `json().(foo.merge(bar).merge(foo))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]interface{}{
				"first":  []interface{}{"val1", "val1"},
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6), float64(3)},
			},
		},
		"check merge arrays": {
			input: `[].merge("foo")`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []interface{}{"foo"},
		},
		"check merge arrays 2": {
			input: `["foo"].merge(["bar","baz"])`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []interface{}{"foo", "bar", "baz"},
		},
		"check contains array": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `["nope","foo","bar"]`}},
			output:   true,
		},
		"check contains array 2": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `["nope","bar"]`}},
			output:   false,
		},
		"check contains array 3": {
			input: `json().contains(meta("against"))`,
			messages: []easyMsg{{
				content: `["nope","foo","bar"]`,
				meta:    map[string]string{"against": "bar"},
			}},
			output: true,
		},
		"check contains map": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `{"1":"nope","2":"foo","3":"bar"}`}},
			output:   true,
		},
		"check contains map 2": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `{"1":"nope","3":"bar"}`}},
			output:   false,
		},
		"check contains invalid type": {
			input:    `json("nope").contains("foo")`,
			messages: []easyMsg{{content: `{"nope":"this is just a string"}`}},
			err:      "expected map or array target, found string",
		},
		"check substr": {
			input:    `json("foo").substr("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello foo world"}`}},
			output:   true,
		},
		"check substr 2": {
			input:    `json("foo").substr("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello bar world"}`}},
			output:   false,
		},
		"check substr 3": {
			input: `json("foo").substr(meta("against"))`,
			messages: []easyMsg{{
				content: `{"foo":"nope foo bar"}`,
				meta:    map[string]string{"against": "bar"},
			}},
			output: true,
		},
		"check substr invalid type": {
			input:    `json("nope").substr("foo")`,
			messages: []easyMsg{{content: `{"nope":{"not":"a string"}}`}},
			err:      "expected string target, found map[string]interface {}",
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
