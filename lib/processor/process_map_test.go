// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
