package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestCheckInterpolationString(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		arg     string
		payload [][]byte
		want    bool
	}{
		{
			name:  "static value",
			value: "foo",
			arg:   "foo",
			payload: [][]byte{
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "batch size 1",
			value: "${! batch_size() }",
			arg:   "1",
			payload: [][]byte{
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "batch size not 1",
			value: "${! batch_size() }",
			arg:   "1",
			payload: [][]byte{
				[]byte("foobar"),
				[]byte("foobar"),
			},
			want: false,
		},
		{
			name:  "batch size 2",
			value: "${! batch_size() }",
			arg:   "2",
			payload: [][]byte{
				[]byte("foobar"),
				[]byte("foobar"),
			},
			want: true,
		},
		{
			name:  "two interps",
			value: `${! batch_size() }-${! json("id") }`,
			arg:   "1-foo",
			payload: [][]byte{
				[]byte(`{"id":"foo"}`),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tConf := NewConfig()
			tConf.Type = "text"
			tConf.Text.Operator = "equals"
			tConf.Text.Arg = tt.arg

			conf := NewConfig()
			conf.Type = "check_interpolation"
			conf.CheckInterpolation.Value = tt.value
			conf.CheckInterpolation.Condition = &tConf

			c, err := NewCheckInterpolation(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.payload)); got != tt.want {
				t.Errorf("CheckInterpolation.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckInterpolationBadChild(t *testing.T) {
	conf := NewConfig()
	conf.Type = "check_interpolation"

	_, err := NewCheckInterpolation(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad child")
	}
}
