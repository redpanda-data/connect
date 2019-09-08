// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestCheckFieldString(t *testing.T) {
	type fields struct {
		path  string
		part  int
		parts []int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "invalid json",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  0,
			},
			arg: [][]byte{
				[]byte("foobar"),
			},
			want: false,
		},
		{
			name: "string val",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":"foobar"}}`),
			},
			want: true,
		},
		{
			name: "string val 2",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  1,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":"foobar"}}`),
				[]byte(`{"foo":{"bar":"nope"}}`),
			},
			want: false,
		},
		{
			name: "string val 3",
			fields: fields{
				path:  "foo.bar",
				parts: []int{1},
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":"foobar"}}`),
				[]byte(`{"foo":{"bar":"nope"}}`),
			},
			want: false,
		},
		{
			name: "string val 4",
			fields: fields{
				path:  "foo.bar",
				parts: []int{0},
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":"foobar"}}`),
				[]byte(`{"foo":{"bar":"nope"}}`),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tConf := NewConfig()
			tConf.Type = "text"
			tConf.Text.Operator = "equals"
			tConf.Text.Part = tt.fields.part
			tConf.Text.Arg = "foobar"

			conf := NewConfig()
			conf.Type = "check_field"
			conf.CheckField.Path = tt.fields.path
			conf.CheckField.Parts = tt.fields.parts
			conf.CheckField.Condition = &tConf

			c, err := NewCheckField(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("CheckField.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckFieldJMESPath(t *testing.T) {
	type fields struct {
		path  string
		part  int
		parts []int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "invalid json",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  0,
			},
			arg: [][]byte{
				[]byte("foobar"),
			},
			want: false,
		},
		{
			name: "valid json string",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":{"baz":"foobar"}}}`),
			},
			want: true,
		},
		{
			name: "valid json object",
			fields: fields{
				path:  "foo.bar",
				parts: []int{},
				part:  0,
			},
			arg: [][]byte{
				[]byte(`{"foo":{"bar":"{\"baz\":\"foobar\"}"}}`),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tConf := NewConfig()
			tConf.Type = "jmespath"
			tConf.JMESPath.Part = tt.fields.part
			tConf.JMESPath.Query = `baz == 'foobar'`

			conf := NewConfig()
			conf.Type = "check_field"
			conf.CheckField.Path = tt.fields.path
			conf.CheckField.Parts = tt.fields.parts
			conf.CheckField.Condition = &tConf

			c, err := NewCheckField(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("CheckField.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckFieldBadChild(t *testing.T) {
	conf := NewConfig()
	conf.Type = "check_field"

	_, err := NewCheckField(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad child")
	}
}
