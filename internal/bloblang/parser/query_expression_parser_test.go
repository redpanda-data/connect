package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestExpressionsParser(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		value    *any
		index    int
	}{
		"match literals": {
			input: `match "string literal" {
  this == "string literal" => "first"
  this != "string literal" => "second"
  _ => "third"
  
} `,
			output:   `first`,
			messages: []easyMsg{},
		},
		"match literals 2": {
			input: `match "string literal"
{
  this != "string literal" => "first"
  this == "string literal" => "second"
  _ => "third" }`,
			output:   `second`,
			messages: []easyMsg{},
		},
		"match literals 3": {
			input: `match "string literal" {
  this != "string literal" => "first"
  this == "nope" => "second"
  _ => "third"
}`,
			output:   `third`,
			messages: []easyMsg{},
		},
		"match literals 4": {
			input: `match "foo" {
  "foo" => "first"
  "bar" => "second"
  _ => "third"
}`,
			output:   `first`,
			messages: []easyMsg{},
		},
		"match literals 5": {
			input: `match "bar" {
  "foo" => "first"
  "bar" => "second"
  _ => "third"
}`,
			output:   `second`,
			messages: []easyMsg{},
		},
		"match literals 6": {
			input: `match "baz" {
  "foo" => "first"
  "bar" => "second"
  _ => "third"
}`,
			output:   `third`,
			messages: []easyMsg{},
		},
		"match function": {
			input: `match json("foo") {
  this > 10 =>  this + 1
  this > 5 => this + 2
  _ => this + 3
  }`,
			output: `9`,
			messages: []easyMsg{
				{content: `{"foo":7}`},
			},
		},
		"match function 2": {
			input: `match json("foo") {
  this > 10 =>  this + 1
  this > 5 => this + 2
  _ => this + 3 }`,
			output: `16`,
			messages: []easyMsg{
				{content: `{"foo":15}`},
			},
		},
		"match function 3": {
			input: `match json("foo") {
  this > 10 =>  this + 1
  this > 5 => this + 2
  _ => this + 3
}`,
			output: `5`,
			messages: []easyMsg{
				{content: `{"foo":2}`},
			},
		},
		"match empty": {
			input: `match "" {
  json().foo > 5 => json().foo
  json().bar > 5 => "bigbar"
  _ => json().baz
}`,
			output: `6`,
			messages: []easyMsg{
				{content: `{"foo":6,"bar":3,"baz":"isbaz"}`},
			},
		},
		"match empty 2": {
			input: `match "" {
  json().foo > 5 => "bigfoo"
  json().bar > 5 => "bigbar"
  _ => json().baz }`,
			output: `bigbar`,
			messages: []easyMsg{
				{content: `{"foo":2,"bar":7,"baz":"isbaz"}`},
			},
		},
		"match empty 3": {
			input: `match "" {
  json().foo > 5 => "bigfoo"
  json().bar.number().or(0) > 5 => "bigbar"
  _ => json().baz }`,
			output: `isbaz`,
			messages: []easyMsg{
				{content: `{"foo":2,"bar":"not a number","baz":"isbaz"}`},
			},
		},
		"match function in braces": {
			input: `(match json("foo") {
  this > 10 =>  this + 1
  this > 5 => this + 2
  _ => this + 3 })`,
			output: `9`,
			messages: []easyMsg{
				{content: `{"foo":7}`},
			},
		},
		"match function in braces 2": {
			input: `(match (json("foo")) {
  (this > 10) => (this + 1)
  (this > 5) => (this + 2)
  _ => (this + 3) })`,
			output: `9`,
			messages: []easyMsg{
				{content: `{"foo":7}`},
			},
		},
		"no matches": {
			input: `match "value" {
  this == "not this value" => "yep"
}`,
			output: `null`,
			messages: []easyMsg{
				{content: `{"foo":6,"bar":3,"baz":"isbaz"}`},
			},
		},
		"match function no expression": {
			input: `match {
  json("foo") > 10 =>  json("foo") + 1
  json("foo") > 5 =>  json("foo") + 2
  _ => "nope"
  }`,
			output: `9`,
			messages: []easyMsg{
				{content: `{"foo":7}`},
			},
		},
		"match with commas": {
			input: `match "bar" {
  "foo" => "first",
  "bar" => "second",
  _ => "third",
}`,
			output:   `second`,
			messages: []easyMsg{},
		},
		"match with commas 2": {
			input:    `match "bar" { "foo" => "first", "bar" => "second", _ => "third" }`,
			output:   `second`,
			messages: []easyMsg{},
		},
		"match with commas 3": {
			input:    `match "bar" {"foo"=>"first","bar"=>"second",_=>"third"}`,
			output:   `second`,
			messages: []easyMsg{},
		},
		"if statement inline": {
			input:  `if "foo" == "foo" { "foo" } else { "bar" }`,
			output: `foo`,
		},
		"if statement with else ifs": {
			input: `if json("type") == "foo" {
  "was foo"
} else if json("type") == "bar" {
  "was bar"
} else if json("type") == "baz" {
  "was baz"
} else {
  "was none"
}`,
			output: `was foo`,
			messages: []easyMsg{
				{content: `{"type":"foo"}`},
			},
		},
		"if statement with else ifs 2": {
			input:  `if json("type") == "foo"{"was foo"} else if json("type") == "bar"{"was bar"} else if json("type") == "baz"{"was baz"}else{"was none"}`,
			output: `was baz`,
			messages: []easyMsg{
				{content: `{"type":"baz"}`},
			},
		},
		"if statement with else ifs 3": {
			input: `if json("type") == "foo" {
  "was foo"
} else if json("type") == "bar" {
  "was bar"
} else if json("type") == "baz" {
  "was baz"
} else {
  "was none"
}`,
			output: `was none`,
			messages: []easyMsg{
				{content: `{"type":"none of them"}`},
			},
		},
		"match array values": {
			input: `match [ "foo" ] {
  [ "foo" ] => "first",
  "bar" => "second",
  _ => "third",
}`,
			output:   `first`,
			messages: []easyMsg{},
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
				Index: test.index, MsgBatch: msg,
			}.WithValueFunc(func() *any { return test.value }))
			require.NoError(t, err)
			assert.Equal(t, test.output, res)

			bres, err := query.ExecToBytes(e, query.FunctionContext{
				Index: test.index, MsgBatch: msg,
			}.WithValueFunc(func() *any { return test.value }))
			require.NoError(t, err)
			res = string(bres)
			assert.Equal(t, test.output, res)
		})
	}
}
