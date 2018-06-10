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
	"testing"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

func TestDeleteJSONValidation(t *testing.T) {
	conf := NewConfig()
	conf.DeleteJSON.Parts = []int{0}
	conf.DeleteJSON.Path = ""

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})

	_, err := NewDeleteJSON(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from empty path")
	}

	conf.DeleteJSON.Path = "."

	_, err = NewDeleteJSON(conf, nil, testLog, metrics.DudType{})
	if err == nil {
		t.Error("Expected error from root path")
	}
}

func TestDeleteJSONPartBounds(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.DeleteJSON.Path = "foo.bar"

	exp := `{"foo":{}}`

	tests := map[int]int{
		-3: 0,
		-2: 1,
		-1: 2,
		0:  0,
		1:  1,
		2:  2,
	}

	for i, j := range tests {
		input := [][]byte{
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{"foo":{"bar":2}}`),
			[]byte(`{"foo":{"bar":2}}`),
		}

		conf.DeleteJSON.Parts = []int{i}
		proc, err := NewDeleteJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(types.NewMessage(input))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if act := string(msgs[0].GetAll()[j]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
	}
}

func TestDeleteJSON(t *testing.T) {
	tLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "del field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "del obj field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":{"baz":5}}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "del array field 1",
			path:   "foo.bar",
			input:  `{"foo":{"bar":[5]}}`,
			output: `{"foo":{}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.DeleteJSON.Parts = []int{0}
		conf.DeleteJSON.Path = test.path

		jSet, err := NewDeleteJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := jSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
