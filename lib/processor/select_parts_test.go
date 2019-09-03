// Copyright (c) 2017 Ashley Jeffs
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

func TestSelectParts(t *testing.T) {
	conf := NewConfig()
	conf.SelectParts.Parts = []int{1, 3}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSelectParts(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	type test struct {
		in  [][]byte
		out [][]byte
	}

	tests := []test{
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
			},
			out: [][]byte{
				[]byte("1"),
				[]byte("3"),
			},
		},
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
			},
			out: [][]byte{
				[]byte("1"),
			},
		},
		{
			in: [][]byte{
				[]byte("0"),
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
			},
			out: [][]byte{
				[]byte("1"),
				[]byte("3"),
			},
		},
	}

	for _, test := range tests {
		msgs, res := proc.ProcessMessage(message.New(test.in))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output: %s != %s", act, exp)
		}
	}
}

func TestSelectPartsIndexBounds(t *testing.T) {
	conf := NewConfig()
	conf.SelectParts.Parts = []int{1, 3}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})

	input := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}

	tests := map[int]string{
		-5: "0",
		-4: "1",
		-3: "2",
		-2: "3",
		-1: "4",
		0:  "0",
		1:  "1",
		2:  "2",
		3:  "3",
		4:  "4",
	}

	for i, exp := range tests {
		conf.SelectParts.Parts = []int{i}
		proc, err := NewSelectParts(conf, nil, testLog, metrics.DudType{})
		if err != nil {
			t.Fatal(err)
		}

		msgs, res := proc.ProcessMessage(message.New(input))
		if len(msgs) != 1 {
			t.Errorf("Select Parts failed on index: %v", i)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if act := string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Unexpected output for index %v: %v != %v", i, act, exp)
		}
	}
}

func TestSelectPartsEmpty(t *testing.T) {
	conf := NewConfig()
	conf.SelectParts.Parts = []int{3}

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	proc, err := NewSelectParts(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	msgs, _ := proc.ProcessMessage(message.New([][]byte{[]byte("foo")}))
	if len(msgs) != 0 {
		t.Error("Expected failure with zero parts selected")
	}
}
