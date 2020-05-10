package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestBloblangCheck(t *testing.T) {
	tests := []struct {
		name  string
		query string
		arg   [][]byte
		want  bool
	}{
		{
			name:  "bool result pos",
			query: `foo == "bar"`,
			arg: [][]byte{
				[]byte(`{"foo":"bar"}`),
			},
			want: true,
		},
		{
			name:  "bool result neg",
			query: `foo == "bar"`,
			arg: [][]byte{
				[]byte(`{"foo":"baz"}`),
			},
			want: false,
		},
		{
			name:  "bool result neg spaces",
			query: `  foo   == "bar"`,
			arg: [][]byte{
				[]byte(`{"foo":"baz"}`),
			},
			want: false,
		},
		{
			name:  "str result neg",
			query: "foo",
			arg: [][]byte{
				[]byte(`{"foo":"baz"}`),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "bloblang"
			conf.Bloblang = BloblangConfig(tt.query)

			c, err := NewBloblang(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("Bloblang.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBloblangBadQuery(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeBloblang
	conf.Bloblang = "this@#$@#$%@#%$@# is a bad query"

	_, err := NewBloblang(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad query")
	}
}
