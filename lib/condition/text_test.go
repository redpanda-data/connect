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
			name: "contains_any_cs 1",
			fields: fields{
				operator: "contains_any_cs",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("foobaz"),
			},
			want: true,
		},
		{
			name: "contains_any_cs 2",
			fields: fields{
				operator: "contains_any_cs",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("fobarbaz"),
			},
			want: true,
		},
		{
			name: "contains_any_cs 3",
			fields: fields{
				operator: "contains_any_cs",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("fobabaz"),
			},
			want: false,
		},
		{
			name: "contains_any 1",
			fields: fields{
				operator: "contains_any",
				arg:      []string{"fOo", "bar"},
			},
			arg: [][]byte{
				[]byte("foobaz"),
			},
			want: true,
		},
		{
			name: "contains_any 2",
			fields: fields{
				operator: "contains_any",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("fobARbaz"),
			},
			want: true,
		},
		{
			name: "contains_any 3",
			fields: fields{
				operator: "contains_any",
				arg:      []string{"foo", "bar"},
			},
			arg: [][]byte{
				[]byte("fobabaz"),
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
	conf.Text.Arg = struct{}{}

	_, err := NewText(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
