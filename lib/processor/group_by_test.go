package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestGroupByWithDefaults(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeGroupBy

	condConf := condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Arg = "foo"
	condConf.Text.Operator = "contains"

	procConf := NewConfig()
	procConf.Type = TypeArchive
	procConf.Archive.Format = "lines"

	conf.GroupBy = append(conf.GroupBy, GroupByElement{
		Condition: condConf,
		Processors: []Config{
			procConf,
		},
	})

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Arg = "bar"
	condConf.Text.Operator = "contains"

	procConf = NewConfig()
	procConf.Type = TypeText
	procConf.Text.Operator = "to_upper"

	procConf2 := NewConfig()
	procConf2.Type = TypeText
	procConf2.Text.Operator = "trim_space"

	conf.GroupBy = append(conf.GroupBy, GroupByElement{
		Condition: condConf,
		Processors: []Config{
			procConf,
			procConf2,
		},
	})

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][][]byte{
		{
			[]byte(` hello foo world 1 
 hello foo bar world 1 
 hello foo world 2 
 hello foo bar world 2 `),
		},
		{
			[]byte(`HELLO BAR WORLD 1`),
			[]byte(`HELLO BAR WORLD 2`),
		},
		{
			[]byte(` hello world 1 `),
			[]byte(` hello world 2 `),
		},
	}
	act := [][][]byte{}

	input := message.New([][]byte{
		[]byte(` hello foo world 1 `),
		[]byte(` hello world 1 `),
		[]byte(` hello foo bar world 1 `),
		[]byte(` hello bar world 1 `),
		[]byte(` hello foo world 2 `),
		[]byte(` hello world 2 `),
		[]byte(` hello foo bar world 2 `),
		[]byte(` hello bar world 2 `),
	})
	msgs, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	for _, msg := range msgs {
		act = append(act, message.GetAllBytes(msg))
	}
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

//------------------------------------------------------------------------------
