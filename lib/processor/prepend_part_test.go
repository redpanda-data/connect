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
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestPrependPart(t *testing.T) {
	conf := NewConfig()
	conf.PrependPart.Content = "hello world"

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewPrependPart(conf, testLog, metrics.DudType{})
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
			},
			out: [][]byte{
				[]byte("hello world"),
				[]byte("0"),
				[]byte("1"),
			},
		},
		{
			in: [][]byte{},
			out: [][]byte{
				[]byte("hello world"),
			},
		},
	}

	for _, test := range tests {
		msg, res, check := proc.ProcessMessage(&types.Message{Parts: test.in})
		if !check {
			t.Errorf("Prepend Part failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, msg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output: %s != %s", act, exp)
		}
	}
}

func TestPrependPartInterpolation(t *testing.T) {
	conf := NewConfig()
	conf.PrependPart.Content = "hello ${!hostname} world"

	hostname, _ := os.Hostname()

	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	proc, err := NewPrependPart(conf, testLog, metrics.DudType{})
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
			},
			out: [][]byte{
				[]byte(fmt.Sprintf("hello %v world", hostname)),
				[]byte("0"),
				[]byte("1"),
			},
		},
		{
			in: [][]byte{},
			out: [][]byte{
				[]byte(fmt.Sprintf("hello %v world", hostname)),
			},
		},
	}

	for _, test := range tests {
		msg, res, check := proc.ProcessMessage(&types.Message{Parts: test.in})
		if !check {
			t.Errorf("Prepend Part failed on: %s", test.in)
		} else if res != nil {
			t.Errorf("Expected nil response: %v", res)
		}
		if exp, act := test.out, msg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Unexpected output: %s != %s", act, exp)
		}
	}
}
