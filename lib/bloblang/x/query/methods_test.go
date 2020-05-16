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
		"check slice": {
			input:  `"foo bar baz".slice(0, 3)`,
			output: "foo",
		},
		"check slice 2": {
			input:  `"foo bar baz".slice(8)`,
			output: "baz",
		},
		"check slice invalid": {
			input: `10.slice(8)`,
			err:   `expected string value, received int64`,
		},
		"check regexp match": {
			input:  `"there are 10 puppies".re_match("[0-9]")`,
			output: true,
		},
		"check regexp match 2": {
			input:  `"there are ten puppies".re_match("[0-9]")`,
			output: false,
		},
		"check regexp match dynamic": {
			input: `json("input").re_match(json("re"))`,
			messages: []easyMsg{
				{content: `{"input":"there are 10 puppies","re":"[0-9]"}`},
			},
			output: true,
		},
		"check regexp replace": {
			input:  `"foo ADD 70".re_replace("ADD ([0-9]+)","+($1)")`,
			output: "foo +(70)",
		},
		"check regexp replace dynamic": {
			input: `json("input").re_replace(json("re"), json("replace"))`,
			messages: []easyMsg{
				{content: `{"input":"foo ADD 70","re":"ADD ([0-9]+)","replace":"+($1)"}`},
			},
			output: "foo +(70)",
		},
		"check parse json": {
			input: `"{\"foo\":\"bar\"}".parse_json()`,
			output: map[string]interface{}{
				"foo": "bar",
			},
		},
		"check parse json invalid": {
			input: `"not valid json".parse_json()`,
			err:   `failed to parse value as JSON: invalid character 'o' in literal null (expecting 'u')`,
		},
		"check append": {
			input: `["foo"].append("bar","baz")`,
			output: []interface{}{
				"foo", "bar", "baz",
			},
		},
		"check append 2": {
			input: `["foo"].(this.append(this))`,
			output: []interface{}{
				"foo", []interface{}{"foo"},
			},
		},
		"check enumerated": {
			input: `["foo","bar","baz"].enumerated()`,
			output: []interface{}{
				map[string]interface{}{
					"index": int64(0),
					"value": "foo",
				},
				map[string]interface{}{
					"index": int64(1),
					"value": "bar",
				},
				map[string]interface{}{
					"index": int64(2),
					"value": "baz",
				},
			},
		},
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
			messages: []easyMsg{{content: `{"nope":false}`}},
			err:      "expected string, array or map target, found bool",
		},
		"check substr": {
			input:    `json("foo").contains("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello foo world"}`}},
			output:   true,
		},
		"check substr 2": {
			input:    `json("foo").contains("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello bar world"}`}},
			output:   false,
		},
		"check substr 3": {
			input: `json("foo").contains(meta("against"))`,
			messages: []easyMsg{{
				content: `{"foo":"nope foo bar"}`,
				meta:    map[string]string{"against": "bar"},
			}},
			output: true,
		},
		"check map each": {
			input:  `["foo","bar"].map_each(this.uppercase())`,
			output: []interface{}{"FOO", "BAR"},
		},
		"check map each 2": {
			input:  `["foo","bar"].map_each("(%v)".format(this).uppercase())`,
			output: []interface{}{"(FOO)", "(BAR)"},
		},
		"check fold": {
			input: `[3, 5, 2].fold(0, tally + value)`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: float64(10),
		},
		"check fold 2": {
			input: `["foo","bar"].fold("", "%v%v".format(tally, value))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: "foobar",
		},
		"check fold 3": {
			input: `["foo","bar"].fold({"values":[]}, tally.merge({
				"values":[value]
			}))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: map[string]interface{}{
				"values": []interface{}{"foo", "bar"},
			},
		},
		"check fold exec err": {
			input: `["foo","bar"].fold(this.does.not.exist, tally.merge({
				"values":[value]
			}))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			err: "failed to extract tally initial value: context was undefined",
		},
		"check fold exec err 2": {
			input: `["foo","bar"].fold({"values":[]}, this.does.not.exist.number())`,
			messages: []easyMsg{
				{content: `{}`},
			},
			err: "function returned non-numerical type: <nil>",
		},
		"check keys literal": {
			input:    `{"foo":1,"bar":2}.keys().sort()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{"bar", "foo"},
		},
		"check keys empty": {
			input:    `{}.keys()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{},
		},
		"check keys function": {
			input:    `json().keys().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []interface{}{"bar", "foo"},
		},
		"check keys error": {
			input:    `"foo".keys().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected map, found string`,
		},
		"check values literal": {
			input:    `{"foo":1,"bar":2}.values().sort()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{int64(1), int64(2)},
		},
		"check values empty": {
			input:    `{}.values()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{},
		},
		"check values function": {
			input:    `json().values().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []interface{}{1.0, 2.0},
		},
		"check values error": {
			input:    `"foo".values().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected map, found string`,
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

			for i := 0; i < 10; i++ {
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
