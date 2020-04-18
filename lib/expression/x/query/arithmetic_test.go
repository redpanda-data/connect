package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestArithmetic(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		index    int
	}{
		"add two ints": {
			input:  `json("foo") + json("bar")`,
			output: `17`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"add two ints 2": {
			input:  `(json("foo")) + (json("bar"))`,
			output: `17`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"add two ints 3": {
			input:  `json("foo") + 5`,
			output: `10`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"subtract two ints": {
			input:  `json("foo") - 5`,
			output: `0`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"subtract two ints 2": {
			input:  `json("foo") - 7`,
			output: `-2`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"two ints and a string": {
			input:  `json("foo") + json("bar") + meta("baz")`,
			output: `17`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]string{
						"baz": "this aint a number",
					},
				},
			},
		},
		"two ints and a string 2": {
			input:  `json("foo") + json("bar") + "baz"`,
			output: `17`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"two ints and a string 3": {
			input:  `json("foo") + json("bar") + "2"`,
			output: `19`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"add three ints": {
			input:  `json("foo") + json("bar") + meta("baz")`,
			output: `20`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]string{
						"baz": "3",
					},
				},
			},
		},
		"sub two ints": {
			input:  `json("foo") - json("bar")`,
			output: `-7`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"sub and add two ints": {
			input:  `json("foo") + json("bar") - meta("foo") - meta("bar")`,
			output: `6`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]string{
						"foo": "3",
						"bar": "8",
					},
				},
			},
		},
		"sub bad int": {
			input:  `json("foo") - json("bar")`,
			output: `5`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":"not a number"}`},
			},
		},
		"sub from bad int": {
			input:  `json("foo") - json("bar")`,
			output: `-7`,
			messages: []easyMsg{
				{content: `{"foo":"not a number","bar":7}`},
			},
		},
		"multiply two ints": {
			input:  `json("foo") * json("bar")`,
			output: `10`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":2}`},
			},
		},
		"multiply three ints": {
			input:  `json("foo") * json("bar") * 2`,
			output: `20`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":2}`},
			},
		},
		"multiply and additions of ints": {
			input:    `2 + 3 * 2 + 1`,
			output:   `9`,
			messages: []easyMsg{{}},
		},
		"multiply and additions of ints 2": {
			input:    `( 2 + 3 ) * (2 + 1)`,
			output:   `15`,
			messages: []easyMsg{{}},
		},
		"multiply and additions of ints 3": {
			input:    `2 + 3 * 2 + 1 * 3`,
			output:   `11`,
			messages: []easyMsg{{}},
		},
		"multiply and additions of ints 4": {
			input:    `(2 + 3 )* (2+1 ) * 3`,
			output:   `45`,
			messages: []easyMsg{{}},
		},
		"division and subtractions of ints": {
			input:    `6 - 6 / 2 + 1`,
			output:   `4`,
			messages: []easyMsg{{}},
		},
		"division and subtractions of ints 2": {
			input:    `(8 - 4) / (1 + 1)`,
			output:   `2`,
			messages: []easyMsg{{}},
		},
		"compare ints": {
			input:    `8 == 2`,
			output:   `false`,
			messages: []easyMsg{{}},
		},
		"compare ints 2": {
			input:    `8 != 2`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"compare ints 3": {
			input:    `8 == 8`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"compare ints 4": {
			input:    `8 > 7`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"compare ints 5": {
			input:    `8 > 2*6`,
			output:   `false`,
			messages: []easyMsg{{}},
		},
		"compare ints 6": {
			input:    `8 >= 7+1`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"compare ints 7": {
			input:    `5 < 2*3`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"compare ints 8": {
			input:    `5 < 2*3`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"coalesce json": {
			input:  `json("foo") | json("bar")`,
			output: `from_bar`,
			messages: []easyMsg{
				{content: `{"foo":null,"bar":"from_bar"}`},
			},
		},
		"coalesce json 2": {
			input:  `json("foo") | "notthis"`,
			output: `from_foo`,
			messages: []easyMsg{
				{content: `{"foo":"from_foo"}`},
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

			e, err := tryParse(test.input)
			if !assert.NoError(t, err) {
				return
			}
			res := e.ToString(FunctionContext{
				Index: test.index,
				Msg:   msg,
			})
			assert.Equal(t, test.output, res)
			res = string(e.ToBytes(FunctionContext{
				Index: test.index,
				Msg:   msg,
			}))
			assert.Equal(t, test.output, res)
		})
	}
}

func TestArithmeticLiterals(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]string{
		`2 == 3`:               `false`,
		`"2" == 2`:             `false`,
		`"2" == "2"`:           `true`,
		`"2" == "3"`:           `false`,
		`2.0 == 2`:             `true`,
		`true == true`:         `true`,
		`true == false`:        `false`,
		`2 == -2`:              `false`,
		`2 == 3-1`:             `true`,
		`2 == 2`:               `true`,
		`2 != 3`:               `true`,
		`2.5 == 3.2`:           `false`,
		`2.5 == 3.5-1`:         `true`,
		`2.3 == 2.3`:           `true`,
		`2.3 != 2.2`:           `true`,
		`3 != 3`:               `false`,
		`5 < 2*3`:              `true`,
		`5 > 2*3`:              `false`,
		`5 <= 2.5*2`:           `true`,
		`2 > -3`:               `true`,
		`-2 < 2`:               `true`,
		`(2 == 2) && (1 != 2)`: `true`,
		`(2 == 2) && (2 != 2)`: `false`,
		`(2 == 2) || (2 != 2)`: `true`,
		`(2 == 1) || (2 != 2)`: `false`,
	}

	for k, v := range tests {
		msg := message.New(nil)
		e, err := tryParse(k)
		if !assert.NoError(t, err) {
			return
		}
		res := e.ToString(FunctionContext{
			Index: 0,
			Msg:   msg,
		})
		assert.Equal(t, v, res, k)
		res = string(e.ToBytes(FunctionContext{Msg: msg}))
		assert.Equal(t, v, res, k)
	}
}
