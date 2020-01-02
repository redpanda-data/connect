package condition

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestMetadataCheck(t *testing.T) {
	type fields struct {
		operator string
		part     int
		key      string
		arg      interface{}
	}
	tests := []struct {
		name   string
		fields fields
		arg    map[string]string
		want   bool
	}{
		{
			name: "enum pos 1",
			fields: fields{
				operator: "enum",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"bar", "baz", "qux", "quux", 333, 8.31},
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			name: "enum pos 2",
			fields: fields{
				operator: "enum",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"bar", "baz", "qux", "quux", 333, 8.31},
			},
			arg: map[string]string{
				"foo": "333",
			},
			want: true,
		},
		{
			name: "enum pos 3",
			fields: fields{
				operator: "enum",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"bar", "baz", "qux", "quux", 333, 8.31},
			},
			arg: map[string]string{
				"foo": "8.31",
			},
			want: true,
		},
		{
			name: "enum neg 1",
			fields: fields{
				operator: "enum",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"bar", "baz", "qux", "quux", 333, 8.31},
			},
			arg: map[string]string{
				"foo": "quz",
			},
			want: false,
		},
		{
			name: "equals_cs foo pos",
			fields: fields{
				operator: "equals_cs",
				key:      "foo",
				part:     0,
				arg:      "bar",
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			name: "equals_cs foo neg",
			fields: fields{
				operator: "equals_cs",
				key:      "foo",
				part:     0,
				arg:      "BAR",
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: false,
		},
		{
			name: "equals foo pos",
			fields: fields{
				operator: "equals",
				key:      "foo",
				part:     0,
				arg:      "BAR",
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			name: "equals foo neg",
			fields: fields{
				operator: "equals",
				key:      "foo",
				part:     0,
				arg:      "baz",
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: false,
		},
		{
			name: "exists pos",
			fields: fields{
				operator: "exists",
				key:      "foo",
				part:     0,
			},
			arg: map[string]string{
				"foo": "bar",
			},
			want: true,
		},
		{
			name: "exists neg",
			fields: fields{
				operator: "exists",
				key:      "foo",
				part:     0,
			},
			arg: map[string]string{
				"bar": "baz",
			},
			want: false,
		},
		{
			name: "gt foo pos 1",
			fields: fields{
				operator: "greater_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "11",
			},
			want: true,
		},
		{
			name: "gt foo pos 2",
			fields: fields{
				operator: "greater_than",
				key:      "foo",
				part:     0,
				arg:      "10",
			},
			arg: map[string]string{
				"foo": "11",
			},
			want: true,
		},
		{
			name: "gt foo nan neg",
			fields: fields{
				operator: "greater_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "nope",
			},
			want: false,
		},
		{
			name: "gt foo neg",
			fields: fields{
				operator: "greater_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "9",
			},
			want: false,
		},
		{
			name: "has_prefix pos 1",
			fields: fields{
				operator: "has_prefix",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"foo", "bar", "baz"},
			},
			arg: map[string]string{
				"foo": "barley",
			},
			want: true,
		},
		{
			name: "has_prefix pos 2",
			fields: fields{
				operator: "has_prefix",
				key:      "foo",
				part:     0,
				arg:      "foo bar baz",
			},
			arg: map[string]string{
				"foo": "foo bar bazley",
			},
			want: true,
		},
		{
			name: "has_prefix neg 1",
			fields: fields{
				operator: "has_prefix",
				key:      "foo",
				part:     0,
				arg:      []interface{}{"foo", "bar", "baz"},
			},
			arg: map[string]string{
				"foo": "quz",
			},
			want: false,
		},
		{
			name: "has_prefix neg 2",
			fields: fields{
				operator: "has_prefix",
				key:      "foo",
				part:     0,
				arg:      "foo bar baz",
			},
			arg: map[string]string{
				"foo": "barley",
			},
			want: false,
		},
		{
			name: "lt foo pos",
			fields: fields{
				operator: "less_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "9",
			},
			want: true,
		},
		{
			name: "lt foo nan neg",
			fields: fields{
				operator: "less_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "nope",
			},
			want: false,
		},
		{
			name: "lt foo neg",
			fields: fields{
				operator: "less_than",
				key:      "foo",
				part:     0,
				arg:      10,
			},
			arg: map[string]string{
				"foo": "11",
			},
			want: false,
		},
		{
			name: "regexp_partial 1",
			fields: fields{
				operator: "regexp_partial",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "hello 1a2 world",
			},
			want: true,
		},
		{
			name: "regexp_partial 2",
			fields: fields{
				operator: "regexp_partial",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "1a2",
			},
			want: true,
		},
		{
			name: "regexp_partial 3",
			fields: fields{
				operator: "regexp_partial",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "hello 12 world",
			},
			want: false,
		},
		{
			name: "regexp_exact 1",
			fields: fields{
				operator: "regexp_exact",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "hello 1a2 world",
			},
			want: false,
		},
		{
			name: "regexp_exact 2",
			fields: fields{
				operator: "regexp_exact",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "1a2",
			},
			want: true,
		},
		{
			name: "regexp_exact 3",
			fields: fields{
				operator: "regexp_exact",
				key:      "foo",
				part:     0,
				arg:      "1[a-z]2",
			},
			arg: map[string]string{
				"foo": "12",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = TypeMetadata
			conf.Metadata.Operator = tt.fields.operator
			conf.Metadata.Key = tt.fields.key
			conf.Metadata.Part = tt.fields.part
			conf.Metadata.Arg = tt.fields.arg

			c, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Fatal(err)
			}
			part := message.NewPart(nil).SetMetadata(metadata.New(tt.arg))
			msg := message.New(nil)
			msg.Append(part)
			if got := c.Check(msg); got != tt.want {
				t.Errorf("Metadata.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetadataBadOperator(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

	conf := NewConfig()
	conf.Type = TypeMetadata
	conf.Metadata.Operator = "NOT_EXIST"

	_, err := NewMetadata(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
