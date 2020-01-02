package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestProcessMapParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["."] = "foo"
	conf.ProcessMap.Postmap["foo.baz"] = "baz"
	conf.ProcessMap.Parts = []int{1}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "bar"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapConditions(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["."] = "foo"
	conf.ProcessMap.Postmap["foo.baz"] = "baz"

	condConf := condition.NewConfig()
	condConf.Type = "text"
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "select"

	conf.ProcessMap.Conditions = append(conf.ProcessMap.Conditions, condConf)

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "bar"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"a":"select","foo":{"bar":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"a":"select","foo":{"bar":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapAllParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["."] = "foo"
	conf.ProcessMap.Postmap["foo.baz"] = "baz"
	conf.ProcessMap.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "bar"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapStrict(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["bar"] = "foo.bar"
	conf.ProcessMap.Premap["bar2"] = "foo.bar2"
	conf.ProcessMap.PostmapOptional["foo.baz"] = "baz"
	conf.ProcessMap.PostmapOptional["foo.baz2"] = "baz2"
	conf.ProcessMap.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "bar"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"bar2":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"bar2":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapOptional(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["bar"] = "foo.bar"
	conf.ProcessMap.PremapOptional["bar2"] = "foo.bar2"
	conf.ProcessMap.PostmapOptional["foo.baz"] = "baz"
	conf.ProcessMap.PostmapOptional["foo.baz2"] = "baz2"
	conf.ProcessMap.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "bar"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"bar2":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"baz":"put me at the root"}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"},"bar2":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapBadProc(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["."] = "foo.bar"
	conf.ProcessMap.Postmap["foo.baz"] = "."
	conf.ProcessMap.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "archive"

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"encode me"}}`),
		[]byte(`{"foo":{"bar":"encode me too"}}`),
	}

	msg, res := c.ProcessMessage(message.New(exp))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessMapBadProcTwo(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_map"
	conf.ProcessMap.Premap["."] = "foo.bar"
	conf.ProcessMap.Postmap["foo.baz"] = "."
	conf.ProcessMap.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "filter"
	procConf.Filter.Type = "static"
	procConf.Filter.Static = false

	conf.ProcessMap.Processors = append(conf.ProcessMap.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"encode me"}}`),
		[]byte(`{"foo":{"bar":"encode me too"}}`),
	}

	msg, res := c.ProcessMessage(message.New(exp))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
