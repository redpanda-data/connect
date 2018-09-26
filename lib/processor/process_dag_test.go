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
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
)

func createProcMapConf(inPath string, outPath string, deps ...string) DepProcessMapConfig {
	procConf := NewConfig()
	procConf.Type = "noop"

	conf := NewProcessMapConfig()
	conf.Premap["."] = inPath
	conf.Postmap[outPath] = "."
	conf.Processors = append(conf.Processors, procConf)

	return DepProcessMapConfig{
		Dependencies:     deps,
		ProcessMapConfig: conf,
	}
}

func TestProcessDAGCircular(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo"] = createProcMapConf("tmp.baz", "tmp.foo")
	conf.ProcessDAG["bar"] = createProcMapConf("tmp.foo", "tmp.bar")
	conf.ProcessDAG["baz"] = createProcMapConf("tmp.bar", "tmp.baz")

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from circular deps")
	}
}

func TestProcessDAGSimple(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo"] = createProcMapConf("root", "tmp.foo")
	conf.ProcessDAG["bar"] = createProcMapConf("tmp.foo", "tmp.bar")
	conf.ProcessDAG["baz"] = createProcMapConf("tmp.bar", "tmp.baz")

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	exp := [][]byte{
		[]byte(`{"oops":"no root"}`),
		[]byte(`{"root":"foobarbaz","tmp":{"bar":"foobarbaz","baz":"foobarbaz","foo":"foobarbaz"}}`),
		[]byte(`{"root":"foobarbaz","tmp":{"also":"here","bar":"foobarbaz","baz":"foobarbaz","foo":"foobarbaz"}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"oops":"no root"}`),
		[]byte(`{"root":"foobarbaz"}`),
		[]byte(`{"root":"foobarbaz","tmp":{"also":"here"}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestProcessDAGDiamond(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo"] = createProcMapConf(".", "foo_result")
	conf.ProcessDAG["bar"] = createProcMapConf("root.path", "bar_result")
	conf.ProcessDAG["baz"] = createProcMapConf(".", "baz_result", "foo_result", "bar_result")

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	exp := [][]byte{
		[]byte(`{"bar_result":"nested","baz_result":{"bar_result":"nested","foo_result":{"outter":"value","root":{"path":"nested"}},"outter":"value","root":{"path":"nested"}},"foo_result":{"outter":"value","root":{"path":"nested"}},"outter":"value","root":{"path":"nested"}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"outter":"value","root":{"path":"nested"}}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}
	if act := message.GetAllBytes(msg[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
