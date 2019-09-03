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
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestConditionalWithStaticFalse(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

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
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

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
