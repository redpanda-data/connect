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

func TestTextCheck(t *testing.T) {
	type fields struct {
		operator string
		part     int
		arg      interface{}
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "equals_cs foo pos",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals_cs foo neg",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("not foo"),
			},
			want: false,
		},
		{
			name: "equals foo pos",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals foo pos 2",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("fOo"),
			},
			want: true,
		},
		{
			name: "equals foo neg",
			fields: fields{
				operator: "equals",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("f0o"),
			},
			want: false,
		},
		{
			name: "contains_cs foo pos",
			fields: fields{
				operator: "contains_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: true,
		},
		{
			name: "contains_cs foo neg",
			fields: fields{
				operator: "contains_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello fOo world"),
			},
			want: false,
		},
		{
			name: "contains foo pos",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: true,
		},
		{
			name: "contains foo pos 2",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello fOo world"),
			},
			want: true,
		},
		{
			name: "contains foo neg",
			fields: fields{
				operator: "contains",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello f0o world"),
			},
			want: false,
		},
		{
			name: "equals_cs foo pos from neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -1,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "equals_cs foo neg from neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -2,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("bar"),
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "equals_cs neg empty msg",
			fields: fields{
				operator: "equals_cs",
				part:     0,
				arg:      "foo",
			},
			arg:  [][]byte{},
			want: false,
		},
		{
			name: "equals_cs neg oob",
			fields: fields{
				operator: "equals_cs",
				part:     1,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "equals_cs neg oob neg index",
			fields: fields{
				operator: "equals_cs",
				part:     -2,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "prefix_cs foo pos",
			fields: fields{
				operator: "prefix_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo hello world"),
			},
			want: true,
		},
		{
			name: "prefix_cs foo neg",
			fields: fields{
				operator: "prefix_cs",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("foo hello world"),
			},
			want: false,
		},
		{
			name: "prefix_cs foo neg 2",
			fields: fields{
				operator: "prefix_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: false,
		},
		{
			name: "prefix foo pos",
			fields: fields{
				operator: "prefix",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo hello world"),
			},
			want: true,
		},
		{
			name: "prefix foo pos 2",
			fields: fields{
				operator: "prefix",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("FoO hello world"),
			},
			want: true,
		},
		{
			name: "prefix foo neg",
			fields: fields{
				operator: "prefix",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: false,
		},
		{
			name: "suffix_cs foo pos",
			fields: fields{
				operator: "suffix_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello world foo"),
			},
			want: true,
		},
		{
			name: "suffix_cs foo neg",
			fields: fields{
				operator: "suffix_cs",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello world foo"),
			},
			want: false,
		},
		{
			name: "suffix_cs foo neg 2",
			fields: fields{
				operator: "suffix_cs",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: false,
		},
		{
			name: "suffix foo pos",
			fields: fields{
				operator: "suffix",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello world foo"),
			},
			want: true,
		},
		{
			name: "suffix foo pos 2",
			fields: fields{
				operator: "suffix",
				part:     0,
				arg:      "fOo",
			},
			arg: [][]byte{
				[]byte("hello world FoO"),
			},
			want: true,
		},
		{
			name: "suffix foo neg",
			fields: fields{
				operator: "suffix",
				part:     0,
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("hello foo world"),
			},
			want: false,
		},
		{
			name: "regexp_partial 1",
			fields: fields{
				operator: "regexp_partial",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("hello 1a2 world"),
			},
			want: true,
		},
		{
			name: "regexp_partial 2",
			fields: fields{
				operator: "regexp_partial",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("1a2"),
			},
			want: true,
		},
		{
			name: "regexp_partial 3",
			fields: fields{
				operator: "regexp_partial",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("hello 12 world"),
			},
			want: false,
		},
		{
			name: "regexp_exact 1",
			fields: fields{
				operator: "regexp_exact",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("hello 1a2 world"),
			},
			want: false,
		},
		{
			name: "regexp_exact 2",
			fields: fields{
				operator: "regexp_exact",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("1a2"),
			},
			want: true,
		},
		{
			name: "regexp_exact 3",
			fields: fields{
				operator: "regexp_exact",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: [][]byte{
				[]byte("12"),
			},
			want: false,
		},
		{
			name: "enum 1",
			fields: fields{
				operator: "enum",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "enum 2",
			fields: fields{
				operator: "enum",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("bar"),
			},
			want: true,
		},
		{
			name: "enum 3",
			fields: fields{
				operator: "enum",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("baz"),
			},
			want: false,
		},
		{
			name: "enum 4",
			fields: fields{
				operator: "enum",
				arg:      "foo",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: true,
		},
		{
			name: "is ip pos 1",
			fields: fields{
				operator: "is",
				arg:      "ip",
			},
			arg: [][]byte{
				[]byte("8.8.8.8"),
			},
			want: true,
		},
		{
			name: "is ip pos 2",
			fields: fields{
				operator: "is",
				arg:      "ip",
			},
			arg: [][]byte{
				[]byte("ffff:0000:0000:0000:0000:ffff:0100:0000"),
			},
			want: true,
		},
		{
			name: "is ip neg 1",
			fields: fields{
				operator: "is",
				arg:      "ip",
			},
			arg: [][]byte{
				[]byte("42.42.42.420"),
			},
			want: false,
		},
		{
			name: "is ip neg 2",
			fields: fields{
				operator: "is",
				arg:      "ip",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
		{
			name: "is ip neg 3",
			fields: fields{
				operator: "is",
				arg:      "ip",
			},
			arg: [][]byte{
				[]byte("1.2.3"),
			},
			want: false,
		},
		{
			name: "is ipv4 pos 1",
			fields: fields{
				operator: "is",
				arg:      "ipv4",
			},
			arg: [][]byte{
				[]byte("1.2.3.4"),
			},
			want: true,
		},
		{
			name: "is ipv4 neg 1",
			fields: fields{
				operator: "is",
				arg:      "ipv4",
			},
			arg: [][]byte{
				[]byte("1.2.3"),
			},
			want: false,
		},
		{
			name: "is ipv4 neg 2",
			fields: fields{
				operator: "is",
				arg:      "ipv4",
			},
			arg: [][]byte{
				[]byte("0000:0000:0000:0000:0000:ffff:0100:0000"),
			},
			want: false,
		},
		{
			name: "is ipv6 pos 1",
			fields: fields{
				operator: "is",
				arg:      "ipv6",
			},
			arg: [][]byte{
				[]byte("ffff:0000:0000:0000:0000:ffff:0100:0000"),
			},
			want: true,
		},
		{
			name: "is ipv6 neg 1",
			fields: fields{
				operator: "is",
				arg:      "ipv6",
			},
			arg: [][]byte{
				[]byte("42.42.42.42"),
			},
			want: false,
		},
		{
			name: "is ipv6 neg 2",
			fields: fields{
				operator: "is",
				arg:      "ipv6",
			},
			arg: [][]byte{
				[]byte("foo"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "text"
			conf.Text.Operator = tt.fields.operator
			conf.Text.Part = tt.fields.part
			conf.Text.Arg = tt.fields.arg

			c, err := NewText(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("Text.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTextBadOperator(t *testing.T) {
	conf := NewConfig()
	conf.Type = "text"
	conf.Text.Operator = "NOT_EXIST"

	_, err := NewText(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}

func TestTextBadArg(t *testing.T) {
	conf := NewConfig()
	conf.Type = "text"
	conf.Text.Operator = "equals"
	conf.Text.Arg = 10

	_, err := NewText(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}

func TestTextBadEnumArg(t *testing.T) {
	conf := NewConfig()
	conf.Type = "text"
	conf.Text.Operator = "enum"
	conf.Text.Arg = []int{1, 2}

	_, err := NewText(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
