package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionsParser(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input      string
		deprecated bool
		output     string
		messages   []easyMsg
		value      *interface{}
		index      int
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

			e, err := tryParse(test.input, test.deprecated)
			require.Nil(t, err)

			res := ExecToString(e, FunctionContext{
				Index: test.index, MsgBatch: msg,
				Value: test.value,
			})
			assert.Equal(t, test.output, res)
			res = string(ExecToBytes(e, FunctionContext{
				Index: test.index, MsgBatch: msg,
				Value: test.value,
			}))
			assert.Equal(t, test.output, res)
		})
	}
}
