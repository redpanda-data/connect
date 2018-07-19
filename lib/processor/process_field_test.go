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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestProcessFieldParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_field"
	conf.ProcessField.Path = "foo.bar"
	conf.ProcessField.Parts = []int{1}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "baz"

	conf.ProcessField.Processors = append(conf.ProcessField.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"foo":{"bar":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}

	msg, res := c.ProcessMessage(types.NewMessage([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"original"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := msg[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessFieldAllParts(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_field"
	conf.ProcessField.Path = "foo.bar"
	conf.ProcessField.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "json"
	procConf.JSON.Operator = "select"
	procConf.JSON.Path = "baz"

	conf.ProcessField.Processors = append(conf.ProcessField.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"put me at the root"}}`),
		[]byte(`{"foo":{"bar":"put me at the root"}}`),
	}

	msg, res := c.ProcessMessage(types.NewMessage([][]byte{
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
		[]byte(`{"foo":{"bar":{"baz":"put me at the root"}}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := msg[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessFieldString(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_field"
	conf.ProcessField.Path = "foo.bar"
	conf.ProcessField.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "encode"

	conf.ProcessField.Processors = append(conf.ProcessField.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"ZW5jb2RlIG1l"}}`),
		[]byte(`{"foo":{"bar":"ZW5jb2RlIG1lIHRvbw=="}}`),
	}

	msg, res := c.ProcessMessage(types.NewMessage([][]byte{
		[]byte(`{"foo":{"bar":"encode me"}}`),
		[]byte(`{"foo":{"bar":"encode me too"}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := msg[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessFieldBadProc(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_field"
	conf.ProcessField.Path = "foo.bar"
	conf.ProcessField.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "archive"

	conf.ProcessField.Processors = append(conf.ProcessField.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"encode me"}}`),
		[]byte(`{"foo":{"bar":"encode me too"}}`),
	}

	msg, res := c.ProcessMessage(types.NewMessage(exp))
	if res != nil {
		t.Error(res.Error())
	}
	if act := msg[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessFieldBadProcTwo(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_field"
	conf.ProcessField.Path = "foo.bar"
	conf.ProcessField.Parts = []int{}

	procConf := NewConfig()
	procConf.Type = "filter"
	procConf.Filter.Type = "static"
	procConf.Filter.Static = false

	conf.ProcessField.Processors = append(conf.ProcessField.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	exp := [][]byte{
		[]byte(`{"foo":{"bar":"encode me"}}`),
		[]byte(`{"foo":{"bar":"encode me too"}}`),
	}

	msg, res := c.ProcessMessage(types.NewMessage(exp))
	if res != nil {
		t.Error(res.Error())
	}
	if act := msg[0].GetAll(); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
