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
