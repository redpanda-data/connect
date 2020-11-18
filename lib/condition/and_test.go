package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestAndCheck(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	testMsg := message.New(
		[][]byte{
			[]byte("foo"),
		},
	)

	passConf := NewConfig()
	passConf.Text.Operator = "contains"
	passConf.Text.Arg = "foo"

	failConf := NewConfig()
	failConf.Text.Operator = "contains"
	failConf.Text.Arg = "bar"

	tests := []struct {
		name string
		arg  []Config
		want bool
	}{
		{
			name: "one pass",
			arg: []Config{
				passConf,
			},
			want: true,
		},
		{
			name: "two pass",
			arg: []Config{
				passConf,
				passConf,
			},
			want: true,
		},
		{
			name: "one fail",
			arg: []Config{
				failConf,
			},
			want: false,
		},
		{
			name: "two fail",
			arg: []Config{
				failConf,
				failConf,
			},
			want: false,
		},
		{
			name: "first fail",
			arg: []Config{
				failConf,
				passConf,
			},
			want: false,
		},
		{
			name: "second fail",
			arg: []Config{
				passConf,
				failConf,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "and"
			conf.And = tt.arg

			c, err := New(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(testMsg); got != tt.want {
				t.Errorf("And.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAndBadOperator(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	cConf := NewConfig()
	cConf.Type = "text"
	cConf.Text.Operator = "NOT_EXIST"

	conf := NewConfig()
	conf.Type = "and"
	conf.And = []Config{
		cConf,
	}

	_, err := NewAnd(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from bad operator")
	}
}
