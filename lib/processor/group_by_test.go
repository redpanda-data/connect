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
