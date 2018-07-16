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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	yaml "gopkg.in/yaml.v2"
)

func TestJSONValidation(t *testing.T) {
	conf := NewConfig()
	conf.JSON.Operator = "dfjjkdsgjkdfhgjfh"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`this isnt valid json`)

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from bad operator")
	}

	conf = NewConfig()
	conf.JSON.Operator = "move"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`#%#@$his isnt valid json`)

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from bad value")
	}

	conf = NewConfig()
	conf.JSON.Operator = "move"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = ""
	conf.JSON.Value = []byte(`"foo.bar"`)

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from empty move path")
	}

	conf = NewConfig()
	conf.JSON.Operator = "move"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`""`)

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from empty move destination")
	}

	conf = NewConfig()
	conf.JSON.Operator = "copy"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = ""
	conf.JSON.Value = []byte(`"foo.bar"`)

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from empty copy path")
	}

	conf = NewConfig()
	conf.JSON.Operator = "copy"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`""`)

	if _, err := NewJSON(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from empty copy destination")
	}

	conf = NewConfig()
	conf.JSON.Operator = "set"
	conf.JSON.Parts = []int{0}
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`this isnt valid json`)

	jSet, err := NewJSON(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn := types.NewMessage([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is bad json", string(msgs[0].GetAll()[0]); exp != act {
		t.Errorf("Wrong output from bad json: %v != %v", act, exp)
	}

	conf.JSON.Parts = []int{5}

	jSet, err = NewJSON(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	msgIn = types.NewMessage([][]byte{[]byte("{}")})
	msgs, res = jSet.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad index")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "{}", string(msgs[0].GetAll()[0]); exp != act {
		t.Errorf("Wrong output from bad index: %v != %v", act, exp)
	}
}

func TestJSONPartBounds(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.JSON.Operator = "set"
	conf.JSON.Path = "foo.bar"
	conf.JSON.Value = []byte(`{"baz":1}`)

	exp := `{"foo":{"bar":{"baz":1}}}`

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

		conf.JSON.Parts = []int{i}
		proc, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONAppend(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "append 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":[5,{"baz":1}]}}`,
		},
		{
			name:   "append nil 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{"bar":null}}`,
			output: `{"foo":{"bar":[null,{"baz":1}]}}`,
		},
		{
			name:   "append nil 2",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{"bar":[null]}}`,
			output: `{"foo":{"bar":[null,{"baz":1}]}}`,
		},
		{
			name:   "append empty 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{}}`,
			output: `{"foo":{"bar":[{"baz":1}]}}`,
		},
		{
			name:   "append collision 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":0}`,
			output: `{"foo":0}`,
		},
		{
			name:   "append array 1",
			path:   "foo.bar",
			value:  `[1,2,3]`,
			input:  `{"foo":{"bar":[0]}}`,
			output: `{"foo":{"bar":[0,1,2,3]}}`,
		},
		{
			name:   "append array 2",
			path:   "foo.bar",
			value:  `[1,2,3]`,
			input:  `{"foo":{"bar":0}}`,
			output: `{"foo":{"bar":[0,1,2,3]}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "append"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path
		conf.JSON.Value = []byte(test.value)

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONMove(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "move 1",
			path:   "foo.bar",
			value:  `"bar.baz"`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"bar":{"baz":5},"foo":{}}`,
		},
		{
			name:   "move 2",
			path:   "foo.bar",
			value:  `"bar.baz"`,
			input:  `{"foo":{"bar":5},"bar":{"qux":6}}`,
			output: `{"bar":{"baz":5,"qux":6},"foo":{}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "move"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path
		conf.JSON.Value = []byte(test.value)

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONCopy(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "copy 1",
			path:   "foo.bar",
			value:  `"bar.baz"`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"bar":{"baz":5},"foo":{"bar":5}}`,
		},
		{
			name:   "copy 2",
			path:   "foo.bar",
			value:  `"bar.baz"`,
			input:  `{"foo":{"bar":5},"bar":{"qux":6}}`,
			output: `{"bar":{"baz":5,"qux":6},"foo":{"bar":5}}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "copy"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path
		conf.JSON.Value = []byte(test.value)

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONClean(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "clean nothing",
			path:   "foo.bar",
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":5}}`,
		},
		{
			name:   "clean array",
			path:   "foo.bar",
			input:  `{"foo":{"bar":[]}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "clean array 2",
			path:   "foo.bar",
			input:  `{"foo":{"b":[1],"bar":[]}}`,
			output: `{"foo":{"b":[1]}}`,
		},
		{
			name:   "clean array 3",
			path:   "foo",
			input:  `{"foo":{"b":[1],"bar":[]}}`,
			output: `{"foo":{"b":[1]}}`,
		},
		{
			name:   "clean object",
			path:   "foo.bar",
			input:  `{"foo":{"bar":{}}}`,
			output: `{"foo":{}}`,
		},
		{
			name:   "clean object 2",
			path:   "foo.bar",
			input:  `{"foo":{"b":{"1":1},"bar":{}}}`,
			output: `{"foo":{"b":{"1":1}}}`,
		},
		{
			name:   "clean object 3",
			path:   "foo",
			input:  `{"foo":{"b":{"1":1},"bar":{}}}`,
			output: `{"foo":{"b":{"1":1}}}`,
		},
		{
			name:   "clean array from root",
			path:   "",
			input:  `{"foo":{"b":"b","bar":[]}}`,
			output: `{"foo":{"b":"b"}}`,
		},
		{
			name:   "clean object from root",
			path:   "",
			input:  `{"foo":{"b":"b","bar":{}}}`,
			output: `{"foo":{"b":"b"}}`,
		},
		{
			name:   "clean everything object",
			path:   "",
			input:  `{"foo":{"bar":{}}}`,
			output: `{}`,
		},
		{
			name:   "clean everything array",
			path:   "",
			input:  `[{"foo":{"bar":{}}},[]]`,
			output: `[]`,
		},
		{
			name:   "clean everything string",
			path:   "",
			input:  `""`,
			output: `null`,
		},
		{
			name:   "clean arrays",
			path:   "",
			input:  `[[],1,"",2,{},"test",{"foo":{}}]`,
			output: `[1,2,"test"]`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "clean"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONSet(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "set 1",
			path:   "foo.bar",
			value:  `{"baz":1}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":1}}}`,
		},
		{
			name:   "set 2",
			path:   "foo",
			value:  `5`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":5}`,
		},
		{
			name:   "set 3",
			path:   "foo",
			value:  `"5"`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":"5"}`,
		},
		{
			name: "set 4",
			path: "foo.bar",
			value: `{
					"baz": 1
				}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":1}}}`,
		},
		{
			name:   "set 5",
			path:   "foo.bar",
			value:  `{"baz":${!echo:"foo"}}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":{"baz":"foo"}}}`,
		},
		{
			name:   "set 6",
			path:   "foo.bar",
			value:  `${!echo:10}`,
			input:  `{"foo":{"bar":5}}`,
			output: `{"foo":{"bar":10}}`,
		},
		{
			name:   "set root 1",
			path:   "",
			value:  `{"baz":1}`,
			input:  `"hello world"`,
			output: `{"baz":1}`,
		},
		{
			name:   "set root 2",
			path:   ".",
			value:  `{"baz":1}`,
			input:  `{"foo":2}`,
			output: `{"baz":1}`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "set"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path
		conf.JSON.Value = []byte(test.value)

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONConfigYAML(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	input := `{"foo":{"bar":5}}`

	tests := map[string]string{
		`value: 10`:            `{"foo":{"bar":10}}`,
		`value: "hello world"`: `{"foo":{"bar":"hello world"}}`,
		`value: hello world`:   `{"foo":{"bar":"hello world"}}`,
		`
value:
  baz: 10`: `{"foo":{"bar":{"baz":10}}}`,
		`
value:
  baz:
  - first
  - 2
  - third`: `{"foo":{"bar":{"baz":["first",2,"third"]}}}`,
		`
value:
  baz:
    deeper: look at me
  here: 11`: `{"foo":{"bar":{"baz":{"deeper":"look at me"},"here":11}}}`,
	}

	for config, exp := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "set"
		conf.JSON.Parts = []int{}
		conf.JSON.Path = "foo.bar"

		if err := yaml.Unmarshal([]byte(config), &conf.JSON); err != nil {
			t.Fatal(err)
		}

		jSet, err := NewJSON(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error creating proc '%v': %v", config, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(input),
			},
		)
		msgs, _ := jSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test did not succeed with config: %v", config)
		}

		if act := string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", config, act, exp)
		}
	}
}

func TestJSONConfigYAMLMarshal(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	tests := []string{
		`parts:
- 0
operator: set
path: foo.bar
value:
  baz:
    deeper: look at me
  here: 11
`,
		`parts:
- 0
operator: set
path: foo.bar
value:
  baz:
    deeper:
    - first
    - second
    - third
  here: 11
`,
		`parts:
- 5
operator: set
path: foo.bar.baz
value: 5
`,
		`parts:
- 0
operator: set
path: foo.bar
value: hello world
`,
		`parts:
- 0
operator: set
path: foo.bar
value:
  foo:
    bar:
      baz:
        value: true
`,
	}

	for _, config := range tests {
		conf := NewConfig()
		if err := yaml.Unmarshal([]byte(config), &conf.JSON); err != nil {
			t.Error(err)
			continue
		}

		if act, err := yaml.Marshal(conf.JSON); err != nil {
			t.Error(err)
		} else if string(act) != config {
			t.Errorf("Marshalled config does not match: %v != %v", string(act), config)
		}

		if _, err := NewJSON(conf, nil, tLog, tStats); err != nil {
			t.Errorf("Error creating proc '%v': %v", config, err)
		}
	}
}

func TestJSONSelect(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "select obj",
			path:   "foo.bar",
			input:  `{"foo":{"bar":{"baz":1}}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select array",
			path:   "foo.bar",
			input:  `{"foo":{"bar":["baz","qux"]}}`,
			output: `["baz","qux"]`,
		},
		{
			name:   "select obj as str",
			path:   "foo.bar",
			input:  `{"foo":{"bar":"{\"baz\":1}"}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select str",
			path:   "foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `hello world`,
		},
		{
			name:   "select float",
			path:   "foo.bar",
			input:  `{"foo":{"bar":0.123}}`,
			output: `0.123`,
		},
		{
			name:   "select int",
			path:   "foo.bar",
			input:  `{"foo":{"bar":123}}`,
			output: `123`,
		},
		{
			name:   "select bool",
			path:   "foo.bar",
			input:  `{"foo":{"bar":true}}`,
			output: `true`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.JSON.Operator = "select"
		conf.JSON.Parts = []int{0}
		conf.JSON.Path = test.path

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONDeletePartBounds(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.JSON.Path = "foo.bar"
	conf.JSON.Operator = "delete"

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

		conf.JSON.Parts = []int{i}
		proc, err := NewJSON(conf, nil, tLog, tStats)
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

func TestJSONDelete(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
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
		conf.JSON.Parts = []int{0}
		conf.JSON.Operator = "delete"
		conf.JSON.Path = test.path

		jSet, err := NewJSON(conf, nil, tLog, tStats)
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
