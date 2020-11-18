package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestConditionalWithStaticFalse(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	conf := NewConfig()
	conf.Type = "conditional"
	conf.Conditional.Condition.Type = "static"
	conf.Conditional.Condition.Static = false

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	elseProcConf := NewConfig()
	elseProcConf.Type = "insert_part"
	elseProcConf.InsertPart.Content = "baz"
	elseProcConf.InsertPart.Index = 0

	conf.Conditional.Processors = append(conf.Conditional.Processors, procConf)
	conf.Conditional.ElseProcessors = append(conf.Conditional.ElseProcessors, elseProcConf)

	c, err := New(conf, nil, testLog, testMet)
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`baz`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestConditionalWithStaticTrue(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	conf := NewConfig()
	conf.Type = "conditional"
	conf.Conditional.Condition.Type = "static"
	conf.Conditional.Condition.Static = true

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	elseProcConf := NewConfig()
	elseProcConf.Type = "insert_part"
	elseProcConf.InsertPart.Content = "baz"
	elseProcConf.InsertPart.Index = 0

	conf.Conditional.Processors = append(conf.Conditional.Processors, procConf)
	conf.Conditional.ElseProcessors = append(conf.Conditional.ElseProcessors, elseProcConf)

	c, err := New(conf, nil, testLog, testMet)
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
