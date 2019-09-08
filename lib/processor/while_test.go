// Copyright (c) 2019 Ashley Jeffs
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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestWhileWithCount(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Condition.Type = "count"
	conf.While.Condition.Count.Arg = 3

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
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

func TestWhileWithContentCheck(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Condition.Type = "bounds_check"
	conf.While.Condition.BoundsCheck.MaxParts = 3

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
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

func TestWhileWithCountALO(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.AtLeastOnce = true
	conf.While.Condition.Type = "count"
	conf.While.Condition.Count.Arg = 3

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
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

func TestWhileMaxLoops(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.MaxLoops = 3
	conf.While.Condition.Type = "static"
	conf.While.Condition.Static = true

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{
		[]byte(`foo`),
		[]byte(`foo`),
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

func TestWhileWithStaticTrue(t *testing.T) {
	conf := NewConfig()
	conf.Type = "while"
	conf.While.Condition.Type = "static"
	conf.While.Condition.Static = true

	procConf := NewConfig()
	procConf.Type = "insert_part"
	procConf.InsertPart.Content = "foo"
	procConf.InsertPart.Index = 0

	conf.While.Processors = append(conf.While.Processors, procConf)

	procConf = NewConfig()
	procConf.Type = "sleep"
	procConf.Sleep.Duration = "100ms"

	conf.While.Processors = append(conf.While.Processors, procConf)

	c, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		<-time.After(time.Millisecond * 100)
		c.CloseAsync()
	}()

	_, res := c.ProcessMessage(message.New([][]byte{[]byte("bar")}))
	if res == nil {
		t.Error("Expected error response due to shut down")
	}

	if err := c.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
