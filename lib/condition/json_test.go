package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestJSONCheck(t *testing.T) {
	type fields struct {
		operator string
		part     int
		path     string
		arg      interface{}
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "exists true",
			fields: fields{
				operator: "exists",
				part:     0,
				path:     "foo",
			},
			arg: [][]byte{
				[]byte(`{"foo": 123, "bar": 42}`),
			},
			want: true,
		},
		{
			name: "exists false",
			fields: fields{
				operator: "exists",
				part:     0,
				path:     "foo",
			},
			arg: [][]byte{
				[]byte(`{"fo0": 123, "bar": 42}`),
			},
			want: false,
		},
		{
			name: "exists invalid_json false",
			fields: fields{
				operator: "exists",
				part:     0,
				path:     "foo",
			},
			arg: [][]byte{
				[]byte(`{"foo": 123, "bar": 42`),
			},
			want: false,
		},
		{
			name: "exists nested true",
			fields: fields{
				operator: "exists",
				part:     0,
				path:     "foo.bar",
			},
			arg: [][]byte{
				[]byte(`{"foo": { "bar": 42 }, "baz": 42}`),
			},
			want: true,
		},
		{
			name: "contains true",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      "val",
			},
			arg: [][]byte{
				[]byte(`{"foo": [ "val", "bar" ]}`),
			},
			want: true,
		},
		{
			name: "contains false",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      "val",
			},
			arg: [][]byte{
				[]byte(`{"foo": ["val1", "bar"]}`),
			},
			want: false,
		},
		{
			name: "contains bool true",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      true,
			},
			arg: [][]byte{
				[]byte(`{"foo": [true, "bar"]}`),
			},
			want: true,
		},
		{
			name: "contains bool false",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      true,
			},
			arg: [][]byte{
				[]byte(`{"foo": ["true", "bar"]}`),
			},
			want: false,
		},
		{
			name: "contains int true",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      11,
			},
			arg: [][]byte{
				[]byte(`{"foo": ["true", 11]}`),
			},
			want: true,
		},
		{
			name: "contains int false",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      11,
			},
			arg: [][]byte{
				[]byte(`{"foo": ["true", 10]}`),
			},
			want: false,
		},
		{
			name: "contains float64 true",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      42.42,
			},
			arg: [][]byte{
				[]byte(`{"foo": ["true", 10, 42.42]}`),
			},
			want: true,
		},
		{
			name: "contains float64 false",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo",
				arg:      42.42,
			},
			arg: [][]byte{
				[]byte(`{"foo": ["true", 10, 42, 42]}`),
			},
			want: false,
		},
		{
			name: "contains nested_bool true",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo.bar",
				arg:      true,
			},
			arg: [][]byte{
				[]byte(`{"foo": {"bar": [true, 10, "foo"] }}`),
			},
			want: true,
		},
		{
			name: "contains nested_bool false",
			fields: fields{
				operator: "contains",
				part:     0,
				path:     "foo.bar",
				arg:      true,
			},
			arg: [][]byte{
				[]byte(`{"foo": {"bar": ["true", 10, "foo"] }}`),
			},
			want: false,
		},
		{
			name: "equals int true",
			fields: fields{
				operator: "equals",
				path:     "foo",
				arg:      10.0,
			},
			arg: [][]byte{
				[]byte(`{"foo":10}`),
			},
			want: true,
		},
		{
			name: "equals int false",
			fields: fields{
				operator: "equals",
				path:     "foo",
				arg:      10,
			},
			arg: [][]byte{
				[]byte(`{"foo":"10"}`),
			},
			want: false,
		},
		{
			name: "equals string true",
			fields: fields{
				operator: "equals",
				path:     "foo",
				arg:      "Foo",
			},
			arg: [][]byte{
				[]byte(`{"foo":"Foo"}`),
			},
			want: true,
		},
		{
			name: "equals string false",
			fields: fields{
				operator: "equals",
				path:     "foo",
				arg:      "Foo",
			},
			arg: [][]byte{
				[]byte(`{"foo":"foo"}`),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "json"
			conf.JSON.Operator = tt.fields.operator
			conf.JSON.Part = tt.fields.part
			conf.JSON.Path = tt.fields.path
			conf.JSON.Arg = tt.fields.arg

			c, err := NewJSON(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("JSON.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONBadOperator(t *testing.T) {
	conf := NewConfig()
	conf.Type = "json"
	conf.JSON.Operator = "NOT_EXIST"

	_, err := NewJSON(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
