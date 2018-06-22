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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestMergeJSON(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		first  string
		second string
		output string
	}

	tests := []jTest{
		{
			name:   "object 1",
			first:  `{"baz":{"foo":1}}`,
			second: `{"baz":{"bar":5}}`,
			output: `{"baz":{"bar":5,"foo":1}}`,
		},
		{
			name:   "val to array 1",
			first:  `{"baz":{"foo":3}}`,
			second: `{"baz":{"foo":5}}`,
			output: `{"baz":{"foo":[3,5]}}`,
		},
		{
			name:   "array 1",
			first:  `{"baz":{"foo":[1,2,3]}}`,
			second: `{"baz":{"foo":5}}`,
			output: `{"baz":{"foo":[1,2,3,5]}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.MergeJSON.Parts = []int{}
		conf.MergeJSON.RetainParts = false

		jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.first),
				[]byte(test.second),
			},
		)
		msgs, _ := jMrg.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestMergeJSONRetention(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.MergeJSON.Parts = []int{}
	conf.MergeJSON.RetainParts = true

	jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input := types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput := input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":[1,2]}`))
	exp := expOutput.GetAll()

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act := msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":1}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	conf = NewConfig()
	conf.MergeJSON.Parts = []int{0, -1}
	conf.MergeJSON.RetainParts = true

	jMrg, err = NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":[1,2]}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = types.NewMessage(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.ShallowCopy()
	expOutput.Append([]byte(`{"foo":1}`))
	exp = expOutput.GetAll()

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = msgs[0].GetAll()
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}
