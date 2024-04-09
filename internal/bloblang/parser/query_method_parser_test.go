package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestMethodParser(t *testing.T) {
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
		"literal function": {
			input:    `5.from(0)`,
			output:   `5`,
			messages: []easyMsg{{}},
		},
		"json from": {
			input:  `json("foo").from(0)`,
			output: `1`,
			messages: []easyMsg{
				{content: `{"foo":1}`},
				{content: `{"foo":2}`},
				{content: `{"foo":3}`},
				{content: `{"foo":4}`},
			},
		},
		"json from 2": {
			input:  `json("foo").from(1)`,
			output: `2`,
			messages: []easyMsg{
				{content: `{"foo":1}`},
				{content: `{"foo":2}`},
				{content: `{"foo":3}`},
				{content: `{"foo":4}`},
			},
		},
		"json from 3": {
			input:  `json("foo").from(-1)`,
			output: `4`,
			messages: []easyMsg{
				{content: `{"foo":1}`},
				{content: `{"foo":2}`},
				{content: `{"foo":3}`},
				{content: `{"foo":4}`},
			},
		},
		"json from 4": {
			input:  `json("foo").from(-2)`,
			output: `3`,
			messages: []easyMsg{
				{content: `{"foo":1}`},
				{content: `{"foo":2}`},
				{content: `{"foo":3}`},
				{content: `{"foo":4}`},
			},
		},
		"json from all": {
			input:  `json("foo").from_all()`,
			output: `["a","b","c"]`,
			messages: []easyMsg{
				{content: `{"foo":"a"}`},
				{content: `{"foo":"b"}`},
				{content: `{"foo":"c"}`},
			},
		},
		"json from all/or": {
			input:  `json("foo").or("fallback").from_all()`,
			output: `["a","fallback","c","fallback"]`,
			messages: []easyMsg{
				{content: `{"foo":"a"}`},
				{content: `{}`},
				{content: `{"foo":"c"}`},
				{content: `not even json`},
			},
		},
		"json from all/or 2": {
			input:  `(json().foo | "fallback").from_all()`,
			output: `["a","fallback","c","fallback"]`,
			messages: []easyMsg{
				{content: `{"foo":"a"}`},
				{content: `{}`},
				{content: `{"foo":"c"}`},
				{content: `not even json`},
			},
		},
		"json from all/or 3": {
			input:  `json().foo.or("fallback").from_all()`,
			output: `["a","fallback","c","fallback"]`,
			messages: []easyMsg{
				{content: `{"foo":"a"}`},
				{content: `{}`},
				{content: `{"foo":"c"}`},
				{content: `not even json`},
			},
		},
		"deleted to or": {
			input:    `deleted().or("fallback")`,
			output:   `fallback`,
			messages: []easyMsg{{}},
		},
		"nothing to or": {
			input:    `nothing().or("fallback")`,
			output:   `fallback`,
			messages: []easyMsg{{}},
		},
		"json catch": {
			input:  `json().catch("nope")`,
			output: `nope`,
			messages: []easyMsg{
				{content: `this %$#% isnt json`},
			},
		},
		"json catch 2": {
			input:  `json().catch("nope")`,
			output: `null`,
			messages: []easyMsg{
				{content: `null`},
			},
		},
		"json catch 3": {
			input:  `json("foo").catch("nope")`,
			output: `null`,
			messages: []easyMsg{
				{content: `{"foo":null}`},
			},
		},
		"json catch 4": {
			input:  `json("foo").catch("nope")`,
			output: `yep`,
			messages: []easyMsg{
				{content: `{"foo":"yep"}`},
			},
		},
		"meta from all": {
			input:  `meta("foo").from_all()`,
			output: `["bar",null,"baz"]`,
			messages: []easyMsg{
				{meta: map[string]any{"foo": "bar"}},
				{},
				{meta: map[string]any{"foo": "baz"}},
			},
		},
		"or json null": {
			input:  `json("foo").or("backup")`,
			output: `backup`,
			messages: []easyMsg{
				{content: `{"foo":null}`},
			},
		},
		"or json null 2": {
			input:  `json("foo").or("backup")`,
			output: `backup`,
			messages: []easyMsg{
				{content: `{"bar":"nope"}`},
			},
		},
		"or json null 3": {
			input:  `json("foo").or(json("bar"))`,
			output: `yep`,
			messages: []easyMsg{
				{content: `{"bar":"yep"}`},
			},
		},
		"or boolean from all": {
			input:  `json("foo").or( json("bar") == "yep" ).from_all()`,
			output: `["from foo",true,false,"from foo 2"]`,
			messages: []easyMsg{
				{content: `{"foo":"from foo"}`},
				{content: `{"bar":"yep"}`},
				{content: `{"bar":"nope"}`},
				{content: `{"foo":"from foo 2","bar":"yep"}`},
			},
		},
		"or boolean from metadata": {
			input:  `meta("foo").or( meta("bar") == "yep" ).from_all()`,
			output: `["from foo",true,false,"from foo 2"]`,
			messages: []easyMsg{
				{meta: map[string]any{"foo": "from foo"}},
				{meta: map[string]any{"bar": "yep"}},
				{meta: map[string]any{"bar": "nope"}},
				{meta: map[string]any{"foo": "from foo 2", "bar": "yep"}},
			},
		},
		"map each": {
			input:  `json("foo").map_each(this + 10)`,
			output: `[11,12,12]`,
			messages: []easyMsg{
				{content: `{"foo":[1,2,2]}`},
			},
		},
		"map each inner map": {
			input:  `json("foo").map_each((this.bar + 10) | "woops")`,
			output: `[11,"woops",12]`,
			messages: []easyMsg{
				{content: `{"foo":[{"bar":1},2,{"bar":2}]}`},
			},
		},
		"map each some errors": {
			input:  `json("foo").map_each((this + 10) | "failed")`,
			output: `[11,12,"failed",12]`,
			messages: []easyMsg{
				{content: `{"foo":[1,2,"nope",2]}`},
			},
		},
		"map each uncaught errors": {
			input:  `json("foo").map_each(this.number(0) + 10)`,
			output: `[11,12,10,12]`,
			messages: []easyMsg{
				{content: `{"foo":[1,2,"nope",2]}`},
			},
		},
		"map each delete some elements": {
			input: `json("foo").map_each(
	match this {
		this < 10 => deleted()
		_ => this - 10
	}
)`,
			output: `[1,2,3]`,
			messages: []easyMsg{
				{content: `{"foo":[11,12,7,13]}`},
			},
		},
		"map each delete all elements for some reason": {
			input:  `json("foo").map_each(deleted())`,
			output: `[]`,
			messages: []easyMsg{
				{content: `{"foo":[11,12,7,13]}`},
			},
		},
		"map each object": {
			input:  `json("foo").map_each(value + 10)`,
			output: `{"a":11,"b":12,"c":12}`,
			messages: []easyMsg{
				{content: `{"foo":{"a":1,"b":2,"c":2}}`},
			},
		},
		"map each object delete some elements": {
			input: `json("foo").map_each(
	match {
		value < 10 => deleted()
		_ => value - 10
	}
)`,
			output: `{"a":1,"b":2,"d":3}`,
			messages: []easyMsg{
				{content: `{"foo":{"a":11,"b":12,"c":7,"d":13}}`},
			},
		},
		"map each object delete all elements": {
			input:  `json("foo").map_each(deleted())`,
			output: `{}`,
			messages: []easyMsg{
				{content: `{"foo":{"a":11,"b":12,"c":7,"d":13}}`},
			},
		},
		"test sum standard array": {
			input:  `json("foo").sum()`,
			output: `5`,
			messages: []easyMsg{
				{content: `{"foo":[1,2,2]}`},
			},
		},
		"test sum standard array 4": {
			input:  `json("foo").from_all().sum()`,
			output: `16`,
			messages: []easyMsg{
				{content: `{"foo":1}`},
				{content: `{"foo":3}`},
				{content: `{"foo":4}`},
				{content: `{"foo":8}`},
			},
		},
		"test map json": {
			input:  `json("foo").map(bar)`,
			output: `yep`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"yep"}}`},
			},
		},
		"test map json 2": {
			input:  `json("foo").map(bar.number() + 10)`,
			output: `13`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"3"}}`},
			},
		},
		"test map json 3": {
			input:  `json("foo").map(("static"))`,
			output: `static`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"3"}}`},
			},
		},
		"test string method": {
			input:    `5.string() == "5"`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"test number method": {
			input:    `"5".number() == 5`,
			output:   `true`,
			messages: []easyMsg{{}},
		},
		"test uppercase method": {
			input:    `"foobar".uppercase()`,
			output:   `FOOBAR`,
			messages: []easyMsg{{}},
		},
		"test lowercase method": {
			input:    `"FOOBAR".lowercase()`,
			output:   `foobar`,
			messages: []easyMsg{{}},
		},
		"test format method": {
			input:    `"foo %v bar".format("test")`,
			output:   `foo test bar`,
			messages: []easyMsg{{}},
		},
		"test format method 2": {
			input:  `"foo %v bar".format(meta("foo"))`,
			output: `foo test bar`,
			messages: []easyMsg{{
				meta: map[string]any{"foo": "test"},
			}},
		},
		"test format method 3": {
			input:  `json().("foo %v, %v, %v bar".format(value, meta("foo"), 3))`,
			output: `foo yup, bar, 3 bar`,
			messages: []easyMsg{{
				content: `{"value":"yup"}`,
				meta:    map[string]any{"foo": "bar"},
			}},
		},
		"test length string": {
			input:    `json("foo").length()`,
			output:   `5`,
			messages: []easyMsg{{content: `{"foo":"hello"}`}},
		},
		"test length array": {
			input:    `json("foo").length()`,
			output:   `3`,
			messages: []easyMsg{{content: `{"foo":["foo","bar","baz"]}`}},
		},
		"test length object": {
			input:    `json("foo").length()`,
			output:   `3`,
			messages: []easyMsg{{content: `{"foo":{"foo":1,"bar":2,"baz":3}}`}},
		},
		"test get": {
			input:    `json().get("foo")`,
			output:   `bar`,
			messages: []easyMsg{{content: `{"foo":"bar"}`}},
		},
		"test get 2": {
			input:    `json().get("foo")`,
			output:   `null`,
			messages: []easyMsg{{content: `{"nope":"bar"}`}},
		},
		"test get 3": {
			input:    `json().get("foo.bar")`,
			output:   `baz`,
			messages: []easyMsg{{content: `{"foo":{"bar":"baz"}}`}},
		},
		"test exists": {
			input:    `json().exists("foo")`,
			output:   `true`,
			messages: []easyMsg{{content: `{"foo":"bar"}`}},
		},
		"test exists 2": {
			input:    `json().exists("foo")`,
			output:   `false`,
			messages: []easyMsg{{content: `{"nope":"bar"}`}},
		},
		"test exists 3": {
			input:    `json().exists("foo.bar")`,
			output:   `true`,
			messages: []easyMsg{{content: `{"foo":{"bar":"baz"}}`}},
		},
		"test exists 4": {
			input:    `json().exists("foo.bar")`,
			output:   `false`,
			messages: []easyMsg{{content: `{"foo":{"nope":"baz"}}`}},
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

			e, perr := tryParseQuery(test.input)
			require.Nil(t, perr)
			res, err := query.ExecToString(e, query.FunctionContext{
				Index:    test.index,
				MsgBatch: msg,
			})
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}

func TestMethodErrors(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		errStr   string
		messages []easyMsg
		index    int
	}{
		"literal function": {
			input:    `"not a number".number()`,
			errStr:   "string literal: strconv.ParseFloat: parsing \"not a number\": invalid syntax",
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

			e, perr := tryParseQuery(test.input)
			require.Nil(t, perr)

			_, err := e.Exec(query.FunctionContext{
				Index:    test.index,
				MsgBatch: msg,
			})
			assert.EqualError(t, err, test.errStr)
		})
	}
}

func TestMethodMaps(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	tests := map[string]struct {
		input    string
		output   any
		err      string
		maps     map[string]query.Function
		messages []easyMsg
		index    int
	}{
		"no maps": {
			input:    `"foo".apply("nope")`,
			err:      "no maps were found",
			messages: []easyMsg{{}},
		},
		"map not exist": {
			input:    `"foo".apply("nope")`,
			err:      "map nope was not found",
			maps:     map[string]query.Function{},
			messages: []easyMsg{{}},
		},
		"map static": {
			input:  `"foo".apply("foo")`,
			output: "hello world",
			maps: map[string]query.Function{
				"foo": query.NewLiteralFunction("", "hello world"),
			},
			messages: []easyMsg{{}},
		},
		"map context": {
			input:  `json().apply("foo")`,
			output: "this value",
			maps: map[string]query.Function{
				"foo": query.NewFieldFunction("foo"),
			},
			messages: []easyMsg{{
				content: `{"foo":"this value"}`,
			}},
		},
		"map dynamic": {
			input:  `json().apply(meta("dyn_map"))`,
			output: "this value",
			maps: map[string]query.Function{
				"foo": query.NewFieldFunction("foo"),
				"bar": query.NewFieldFunction("bar"),
			},
			messages: []easyMsg{{
				content: `{"foo":"this value","bar":"and this value"}`,
				meta: map[string]any{
					"dyn_map": "foo",
				},
			}},
		},
		"map dynamic 2": {
			input:  `json().apply(meta("dyn_map"))`,
			output: "and this value",
			maps: map[string]query.Function{
				"foo": query.NewFieldFunction("foo"),
				"bar": query.NewFieldFunction("bar"),
			},
			messages: []easyMsg{{
				content: `{"foo":"this value","bar":"and this value"}`,
				meta: map[string]any{
					"dyn_map": "bar",
				},
			}},
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

			e, perr := tryParseQuery(test.input)
			require.Nil(t, perr)

			res, err := e.Exec(query.FunctionContext{
				Maps:     test.maps,
				Index:    test.index,
				MsgBatch: msg,
			})
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.output, res)
		})
	}
}
