package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestArithmeticParser(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		index    int
	}{
		"compare string to int": {
			input:  `"foo" != 5`,
			output: `true`,
		},
		"compare string to null": {
			input:  `"foo" != null`,
			output: `true`,
		},
		"compare string to int 2": {
			input:  `5 != "foo"`,
			output: `true`,
		},
		"compare string to null 2": {
			input:  `null != "foo"`,
			output: `true`,
		},
		"add strings": {
			input:  `"foo" + "bar" + "%v-%v".format(10, 20)`,
			output: `foobar10-20`,
		},
		"comparisons with not": {
			input:  `!true || false`,
			output: `false`,
		},
		"comparisons with not 2": {
			input:  `false || !false`,
			output: `true`,
		},
		"mod two ints": {
			input:  `5 % 2`,
			output: `1`,
		},
		"mod two strings": {
			input:  `"7".number() % "4".number()`,
			output: `3`,
		},
		"comparisons": {
			input:  `true && false || true && false`,
			output: `false`,
		},
		"comparisons 2": {
			input:  `false || true && true || false`,
			output: `true`,
		},
		"comparisons 3": {
			input:  `true || false && true`,
			output: `true`,
		},
		"and exit early": {
			input:  `false && ("not a number".number() > 0)`,
			output: `false`,
		},
		"and second exit early": {
			input:  `true && false && ("not a number".number() > 0)`,
			output: `false`,
		},
		"or exit early": {
			input:  `true || ("not a number".number() > 0)`,
			output: `true`,
		},
		"or second early": {
			input:  `false || true || ("not a number".number() > 0)`,
			output: `true`,
		},
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
			input:  `json("foo") + json("bar") + meta("baz").number(0)`,
			output: `17`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]any{
						"baz": "this aint a number",
					},
				},
			},
		},
		"two ints and a string 2": {
			input:  `json("foo") + json("bar") + "baz".number(0)`,
			output: `17`,
			messages: []easyMsg{
				{content: `{"foo":5,"bar":12}`},
			},
		},
		"add three ints": {
			input:  `json("foo") + json("bar") + meta("baz").number()`,
			output: `20`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]any{
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
		"negate json int": {
			input:  `-json("foo")`,
			output: `-5`,
			messages: []easyMsg{
				{content: `{"foo":5}`},
			},
		},
		"sub and add two ints": {
			input:  `json("foo") + json("bar") - meta("foo").number() - meta("bar").number()`,
			output: `6`,
			messages: []easyMsg{
				{
					content: `{"foo":5,"bar":12}`,
					meta: map[string]any{
						"foo": "3",
						"bar": "8",
					},
				},
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
		"compare ints 9": {
			input:    `8 > 3 * 5`,
			output:   `false`,
			messages: []easyMsg{{}},
		},
		"compare ints chained boolean": {
			input:    `8 > 3 && 1 < 4`,
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
		"coalesce deleted": {
			input:    `deleted() | "this"`,
			output:   `this`,
			messages: []easyMsg{{}},
		},
		"coalesce nothing": {
			input:    `nothing() | "this"`,
			output:   `this`,
			messages: []easyMsg{{}},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			msg := message.QuickBatch(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSetMut(k, v)
					}
				}
				msg = append(msg, part)
			}

			e, pErr := tryParseQuery(test.input)
			require.Nil(t, pErr)

			res, err := query.ExecToString(e, query.FunctionContext{
				Index:    test.index,
				MsgBatch: msg,
			})
			require.NoError(t, err)
			assert.Equal(t, test.output, res)

			bRes, err := query.ExecToBytes(e, query.FunctionContext{
				Index:    test.index,
				MsgBatch: msg,
			})
			require.NoError(t, err)
			res = string(bRes)
			assert.Equal(t, test.output, res)
		})
	}
}

func TestArithmeticLiteralsParser(t *testing.T) {
	tests := map[string]string{
		`2 == 3`:               `false`,
		`"2".number() == 2`:    `true`,
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
		`-(2) < 2`:             `true`,
		`2 < -"2".number()`:    `false`,
		`2 > -"2".number()`:    `true`,
		`(2 == 2) && (1 != 2)`: `true`,
		`(2 == 2) && (2 != 2)`: `false`,
		`(2 == 2) || (2 != 2)`: `true`,
		`(2 == 1) || (2 != 2)`: `false`,
		`null == null`:         `true`,
		`{} == {}`:             `true`,
		`["foo"] == ["foo"]`:   `true`,
		`["bar"] == ["foo"]`:   `false`,
		`["bar"] != ["foo"]`:   `true`,
		`{} != null`:           `true`,
	}

	for k, v := range tests {
		msg := message.QuickBatch(nil)
		e, pErr := tryParseQuery(k)
		require.Nil(t, pErr)

		res, err := query.ExecToString(e, query.FunctionContext{
			Index:    0,
			MsgBatch: msg,
		})
		require.NoError(t, err)
		assert.Equal(t, v, res, k)

		bres, err := query.ExecToBytes(e, query.FunctionContext{MsgBatch: msg})
		require.NoError(t, err)
		res = string(bres)
		assert.Equal(t, v, res, k)
	}
}
