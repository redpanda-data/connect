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
)

func TestTextValidation(t *testing.T) {
	conf := NewConfig()
	conf.Text.Operator = "dfjjkdsgjkdfhgjfh"
	conf.Text.Parts = []int{0}
	conf.Text.Arg = "foobar"
	conf.Text.Value = "foo"

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	if _, err := NewText(conf, nil, testLog, metrics.DudType{}); err == nil {
		t.Error("Expected error from bad operator")
	}
}

func TestTextPartBounds(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	conf := NewConfig()
	conf.Text.Operator = "trim_space"

	exp := `foobar`

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
			[]byte(`  foobar   `),
			[]byte(`  foobar   `),
			[]byte(`  foobar   `),
		}

		conf.Text.Parts = []int{i}
		proc, err := NewText(conf, nil, tLog, tStats)
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

func TestTextAppend(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "append 1",
			value:  `baz`,
			input:  `foo bar `,
			output: `foo bar baz`,
		},
		{
			name:   "append 2",
			value:  ``,
			input:  `foo bar `,
			output: `foo bar `,
		},
		{
			name:   "append 3",
			value:  `baz`,
			input:  ``,
			output: `baz`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "append"
		conf.Text.Parts = []int{0}
		conf.Text.Value = test.value

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextPrepend(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "prepend 1",
			value:  `baz `,
			input:  `foo bar`,
			output: `baz foo bar`,
		},
		{
			name:   "prepend 2",
			value:  ``,
			input:  `foo bar`,
			output: `foo bar`,
		},
		{
			name:   "prepend 3",
			value:  `baz`,
			input:  ``,
			output: `baz`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "prepend"
		conf.Text.Parts = []int{0}
		conf.Text.Value = test.value

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextTrimSpace(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "trim space 1",
			input:  `foo bar`,
			output: `foo bar`,
		},
		{
			name:   "trim space 2",
			input:  `  foo   bar   `,
			output: `foo   bar`,
		},
		{
			name: "trim space 3",
			input: `
			foo   bar
			`,
			output: `foo   bar`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "trim_space"
		conf.Text.Parts = []int{0}

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextTrim(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		arg    string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "trim 1",
			arg:    "0",
			input:  `foo bar`,
			output: `foo bar`,
		},
		{
			name:   "trim 2",
			arg:    "0",
			input:  `0foo0bar0`,
			output: `foo0bar`,
		},
		{
			name:   "trim 3",
			arg:    "012",
			input:  `021foo012bar210`,
			output: `foo012bar`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "trim"
		conf.Text.Arg = test.arg
		conf.Text.Parts = []int{0}

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextReplace(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		arg    string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "replace 1",
			arg:    "foo",
			value:  "bar",
			input:  `foo bar`,
			output: `bar bar`,
		},
		{
			name:   "replace 2",
			arg:    "foo",
			value:  "bar",
			input:  `baz foo bar foo`,
			output: `baz bar bar bar`,
		},
		{
			name:   "replace 3",
			arg:    "foo",
			value:  "bar",
			input:  `baz baz baz baz`,
			output: `baz baz baz baz`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "replace"
		conf.Text.Arg = test.arg
		conf.Text.Value = test.value
		conf.Text.Parts = []int{0}

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextReplaceRegexp(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		arg    string
		value  string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "replace regexp 1",
			arg:    "foo?",
			value:  "bar",
			input:  `foo bar`,
			output: `bar bar`,
		},
		{
			name:   "replace regexp 2",
			arg:    "foo?",
			value:  "bar",
			input:  `fo bar`,
			output: `bar bar`,
		},
		{
			name:   "replace regexp 3",
			arg:    "foo?",
			value:  "bar",
			input:  `fooo bar`,
			output: `baro bar`,
		},
		{
			name:   "replace regexp 4",
			arg:    "foo?",
			value:  "bar",
			input:  `baz bar`,
			output: `baz bar`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "replace_regexp"
		conf.Text.Arg = test.arg
		conf.Text.Value = test.value
		conf.Text.Parts = []int{0}

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestTextStripHTML(t *testing.T) {
	tLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	tStats := metrics.DudType{}

	type jTest struct {
		name   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "strip html 1",
			input:  `foo <a>bar</a>`,
			output: `foo bar`,
		},
		{
			name:   "strip html 2",
			input:  `<div>foo <a>bar</a></div>`,
			output: `foo bar`,
		},
		{
			name:   "strip html 3",
			input:  `<div field="bar">foo <a>bar</a></div>`,
			output: `foo bar`,
		},
		{
			name:   "strip html 4",
			input:  `<div field="bar">foo<broken <a>bar</a>`,
			output: `foobar`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Text.Operator = "strip_html"
		conf.Text.Parts = []int{0}

		tp, err := NewText(conf, nil, tLog, tStats)
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := tp.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(msgs[0].GetAll()[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
