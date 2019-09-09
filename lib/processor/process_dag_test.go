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

func createProcMapConf(inPath string, outPath string, deps ...string) DepProcessMapConfig {
	procConf := NewConfig()
	procConf.Type = "noop"

	conf := NewProcessMapConfig()
	conf.Premap["."] = inPath
	conf.Postmap[outPath] = "."
	conf.Processors = append(conf.Processors, procConf)

	depsConf := NewDAGDepsConfig()
	depsConf.Dependencies = deps

	return DepProcessMapConfig{
		DAGDepsConfig:    depsConf,
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

func TestProcessDAGBadNames(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo,bar"] = createProcMapConf("tmp.baz", "tmp.foo")

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("expected error from bad name")
	}

	conf = NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo$"] = createProcMapConf("tmp.baz", "tmp.foo")

	if _, err = New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("expected error from bad name")
	}
}

func TestProcessDAGGoodNames(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo_bar"] = createProcMapConf("tmp.baz", "tmp.foo")
	conf.ProcessDAG["FOO-BAR"] = createProcMapConf("tmp.baz", "tmp.foo")
	conf.ProcessDAG["FOO-9"] = createProcMapConf("tmp.baz", "tmp.foo")
	conf.ProcessDAG["FOO-10"] = createProcMapConf("tmp.baz", "tmp.foo")

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err != nil {
		t.Error(err)
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

func TestProcessDAGParallel(t *testing.T) {
	condConf := condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "foo"

	procConf := NewConfig()
	procConf.Type = TypeMetadata
	procConf.Metadata.Operator = "set"
	procConf.Metadata.Key = "A"
	procConf.Metadata.Value = "foo: ${!json_field:foo}"

	fooConf := NewProcessMapConfig()
	fooConf.Conditions = []condition.Config{condConf}
	fooConf.Premap["."] = "."
	fooConf.Postmap["tmp.A"] = "."
	fooConf.Processors = []Config{procConf}

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "bar"

	procConf = NewConfig()
	procConf.Type = TypeMetadata
	procConf.Metadata.Operator = "set"
	procConf.Metadata.Key = "A"
	procConf.Metadata.Value = "bar: ${!json_field:bar}"

	barConf := NewProcessMapConfig()
	barConf.Conditions = []condition.Config{condConf}
	barConf.Premap["."] = "."
	barConf.Postmap["tmp.A"] = "."
	barConf.Processors = []Config{procConf}

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "baz"

	procConf = NewConfig()
	procConf.Type = TypeMetadata
	procConf.Metadata.Operator = "set"
	procConf.Metadata.Key = "B"
	procConf.Metadata.Value = "${!metadata:A}"

	bazConf := NewProcessMapConfig()
	bazConf.Conditions = []condition.Config{condConf}
	bazConf.Premap["."] = "tmp.A"
	bazConf.Postmap["tmp.B"] = "."
	bazConf.Processors = []Config{procConf}

	condConf = condition.NewConfig()
	condConf.Type = condition.TypeText
	condConf.Text.Operator = "contains"
	condConf.Text.Arg = "qux"

	procConf = NewConfig()
	procConf.Type = TypeMetadata
	procConf.Metadata.Operator = "set"
	procConf.Metadata.Key = "B"
	procConf.Metadata.Value = "${!metadata:A}"

	quxConf := NewProcessMapConfig()
	quxConf.Conditions = []condition.Config{condConf}
	quxConf.Premap["."] = "tmp.A"
	quxConf.Postmap["tmp.B"] = "."
	quxConf.Processors = []Config{procConf}

	conf := NewConfig()
	conf.Type = TypeProcessDAG
	conf.ProcessDAG["foo"] = DepProcessMapConfig{
		ProcessMapConfig: fooConf,
	}
	conf.ProcessDAG["bar"] = DepProcessMapConfig{
		ProcessMapConfig: barConf,
	}
	conf.ProcessDAG["baz"] = DepProcessMapConfig{
		ProcessMapConfig: bazConf,
	}
	conf.ProcessDAG["qux"] = DepProcessMapConfig{
		ProcessMapConfig: quxConf,
	}

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	expParts := [][]byte{
		[]byte(`{"foo":"1","qux":"2","tmp":{"A":{"foo":"1","qux":"2"},"B":{"foo":"1","qux":"2"}}}`),
		[]byte(`{"bar":"3","qux":"4","tmp":{"A":{"bar":"3","qux":"4"},"B":{"bar":"3","qux":"4"}}}`),
		[]byte(`{"baz":"6","foo":"5","tmp":{"A":{"baz":"6","foo":"5"},"B":{"baz":"6","foo":"5"}}}`),
		[]byte(`{"bar":"7","baz":"8","tmp":{"A":{"bar":"7","baz":"8"},"B":{"bar":"7","baz":"8"}}}`),
		[]byte(`{"foo":"9","tmp":{"A":{"foo":"9"}}}`),
		[]byte(`{"bar":"10","tmp":{"A":{"bar":"10"}}}`),
		[]byte(`{"foo":"11","qux":"12","tmp":{"A":{"foo":"11","qux":"12"},"B":{"foo":"11","qux":"12"}}}`),
		[]byte(`{"bar":"13","qux":"14","tmp":{"A":{"bar":"13","qux":"14"},"B":{"bar":"13","qux":"14"}}}`),
		[]byte(`{"baz":"16","foo":"15","tmp":{"A":{"baz":"16","foo":"15"},"B":{"baz":"16","foo":"15"}}}`),
		[]byte(`{"bar":"17","baz":"18","tmp":{"A":{"bar":"17","baz":"18"},"B":{"bar":"17","baz":"18"}}}`),
	}

	msg, res := c.ProcessMessage(message.New([][]byte{
		[]byte(`{"foo":"1","qux":"2"}`),
		[]byte(`{"bar":"3","qux":"4"}`),
		[]byte(`{"foo":"5","baz":"6"}`),
		[]byte(`{"bar":"7","baz":"8"}`),
		[]byte(`{"foo":"9"}`),
		[]byte(`{"bar":"10"}`),
		[]byte(`{"foo":"11","qux":"12"}`),
		[]byte(`{"bar":"13","qux":"14"}`),
		[]byte(`{"foo":"15","baz":"16"}`),
		[]byte(`{"bar":"17","baz":"18"}`),
		[]byte(`{"baz":"19"}`),
		[]byte(`{"qux":"20"}`),
	}))
	if res != nil {
		t.Error(res.Error())
	}

	actParts := message.GetAllBytes(msg[0])
	for i, exp := range expParts {
		if len(actParts) <= i {
			t.Errorf("Missing result part index '%v': %s", i, exp)
		}
		if actStr, expStr := string(actParts[i]), string(exp); actStr != expStr {
			t.Errorf("Wrong part result: %v != %v", actStr, expStr)
		}
	}
}

func TestProcessDAGRoot(t *testing.T) {
	conf := NewConfig()
	conf.Type = "process_dag"
	conf.ProcessDAG["foo"] = createProcMapConf("root", "tmp.foo")
	conf.ProcessDAG["bar"] = createProcMapConf("", "tmp.bar")
	conf.ProcessDAG["baz"] = createProcMapConf("tmp.bar", "tmp.baz")

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	exp := [][]byte{
		[]byte(`{"oops":"no root","tmp":{"bar":{"oops":"no root"},"baz":{"oops":"no root"}}}`),
		[]byte(`{"root":"foobarbaz","tmp":{"bar":{"root":"foobarbaz"},"baz":{"root":"foobarbaz"},"foo":"foobarbaz"}}`),
		[]byte(`{"root":"foobarbaz","tmp":{"also":"here","bar":{"root":"foobarbaz","tmp":{"also":"here"}},"baz":{"root":"foobarbaz","tmp":{"also":"here"}},"foo":"foobarbaz"}}`),
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
