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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestNumberBasic(t *testing.T) {
	type testCase struct {
		name     string
		operator string
		value    interface{}
		input    []string
		output   []string
	}

	tests := []testCase{
		{
			name:     "add float64 1",
			operator: "add",
			value:    5.0,
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"11", "15.1",
			},
		},
		{
			name:     "add int 1",
			operator: "add",
			value:    5,
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"11", "15.1",
			},
		},
		{
			name:     "add json.Number 1",
			operator: "add",
			value:    json.Number("5"),
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"11", "15.1",
			},
		},
		{
			name:     "add string 1",
			operator: "add",
			value:    "5",
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"11", "15.1",
			},
		},
		{
			name:     "add interpolated string 1",
			operator: "add",
			value:    "${!batch_size}",
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"8", "12.1",
			},
		},
		{
			name:     "subtract float64 1",
			operator: "subtract",
			value:    5.0,
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"1", "5.1",
			},
		},
		{
			name:     "subtract int 1",
			operator: "subtract",
			value:    5,
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"1", "5.1",
			},
		},
		{
			name:     "subtract json.Number 1",
			operator: "subtract",
			value:    json.Number("5"),
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"1", "5.1",
			},
		},
		{
			name:     "subtract string 1",
			operator: "subtract",
			value:    "5",
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"1", "5.1",
			},
		},
		{
			name:     "subtract interpolated string 1",
			operator: "subtract",
			value:    "${!batch_size}",
			input: []string{
				"6", "10.1",
			},
			output: []string{
				"4", "8.1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := NewConfig()
			conf.Type = TypeNumber
			conf.Number.Value = test.value
			conf.Number.Operator = test.operator

			proc, err := New(conf, nil, log.Noop(), metrics.Noop())
			if err != nil {
				tt.Fatal(err)
			}

			input := message.New(nil)
			for _, p := range test.input {
				input.Append(message.NewPart([]byte(p)))
			}

			exp := make([][]byte, len(test.output))
			for i, p := range test.output {
				exp[i] = []byte(p)
			}

			msgs, res := proc.ProcessMessage(input)
			if res != nil {
				tt.Fatal(res.Error())
			}

			if len(msgs) != 1 {
				tt.Fatalf("Expected one message, received: %v", len(msgs))
			}
			if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
				tt.Errorf("Unexpected output: %s != %s", exp, act)
			}
		})
	}
}

func TestNumberBadContent(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeNumber
	conf.Number.Value = "5"
	conf.Number.Operator = "add"

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte("nope"),
		[]byte("7"),
	})

	exp := [][]byte{
		[]byte("nope"),
		[]byte("12"),
	}

	msgs, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Fatalf("Expected one message, received: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected output: %s != %s", exp, act)
	}

	msgs[0].Iter(func(i int, p types.Part) error {
		if i == 0 {
			if !HasFailed(p) {
				t.Error("Expected fail flag")
			}
		} else if HasFailed(p) {
			t.Error("Expected fail flag")
		}
		return nil
	})
}

func TestNumberBadValue(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeNumber
	conf.Number.Value = "nah"
	conf.Number.Operator = "add"

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err == nil {
		t.Error("Expected error from bad value")
	}
}

func TestNumberBadInterpolatedValue(t *testing.T) {
	conf := NewConfig()
	conf.Type = TypeNumber
	conf.Number.Value = "${!batch_size} but this is never a number"
	conf.Number.Operator = "add"

	proc, err := New(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	input := message.New([][]byte{
		[]byte("11"),
		[]byte("7"),
	})

	exp := [][]byte{
		[]byte("11"),
		[]byte("7"),
	}

	msgs, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(msgs) != 1 {
		t.Fatalf("Expected one message, received: %v", len(msgs))
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(act, exp) {
		t.Errorf("Unexpected output: %s != %s", exp, act)
	}
	msgs[0].Iter(func(i int, p types.Part) error {
		if !HasFailed(p) {
			t.Error("Expected fail flag")
		}
		return nil
	})
}
