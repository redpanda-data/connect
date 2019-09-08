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

		inMsg := message.New(
			[][]byte{
				[]byte(test.first),
				[]byte(test.second),
			},
		)
		msgs, _ := jMrg.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
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

	input := message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput := input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":[1,2]}`)))
	exp := message.GetAllBytes(expOutput)

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act := message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":1}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
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

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":[1,2]}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	input = message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
		},
	)
	expOutput = input.Copy()
	expOutput.Append(message.NewPart([]byte(`{"foo":1}`)))
	exp = message.GetAllBytes(expOutput)

	msgs, _ = jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	act = message.GetAllBytes(msgs[0])
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestMergeJSONNoRetention(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.MergeJSON.Parts = []int{0, -1}
	conf.MergeJSON.RetainParts = false

	jMrg, err := NewMergeJSON(conf, nil, tLog, tStats)
	if err != nil {
		t.Fatal(err)
	}

	input := message.New(
		[][]byte{
			[]byte(`{"foo":1}`),
			[]byte(`not related`),
			[]byte(`{"foo":2}`),
		},
	)
	input.Get(0).Metadata().Set("foo", "1")
	input.Get(1).Metadata().Set("foo", "2")
	input.Get(2).Metadata().Set("foo", "3")

	expParts := [][]byte{
		[]byte(`not related`),
		[]byte(`{"foo":[1,2]}`),
	}

	msgs, _ := jMrg.ProcessMessage(input)
	if len(msgs) != 1 {
		t.Errorf("Wrong output count")
	}
	if act, exp := message.GetAllBytes(msgs[0]), expParts; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "2", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
	if exp, act := "1", msgs[0].Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong metadata: %v != %v", act, exp)
	}
}
