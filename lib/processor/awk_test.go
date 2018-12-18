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
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
)

func TestAWKValidation(t *testing.T) {
	conf := NewConfig()
	conf.AWK.Parts = []int{0}
	conf.AWK.Codec = "json"
	conf.AWK.Program = "{ print foo_bar }"

	a, err := NewAWK(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New([][]byte{[]byte("this is bad json")})
	msgs, res := a.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is bad json", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad json: %v != %v", act, exp)
	}

	conf.AWK.Parts = []int{5}

	if a, err = NewAWK(conf, nil, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}

	msgIn = message.New([][]byte{[]byte("{}")})
	msgs, res = a.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad index")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "{}", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad index: %v != %v", act, exp)
	}

	conf.AWK.Codec = "not valid"
	if _, err = NewAWK(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad codec")
	}

	conf.AWK.Codec = "text"
	conf.AWK.Program = "not valid at all"
	if _, err = NewAWK(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from bad program")
	}
}

func TestAWKBadDateString(t *testing.T) {
	conf := NewConfig()
	conf.AWK.Parts = []int{0}
	conf.AWK.Codec = "none"
	conf.AWK.Program = `{ print timestamp_unix("this isnt a date string") }`

	a, err := NewAWK(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New([][]byte{[]byte("this is a value")})
	msgs, res := a.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough on error")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this is a value", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from bad function call: %v != %v", act, exp)
	}
}

func TestAWK(t *testing.T) {
	type jTest struct {
		name     string
		metadata map[string]string
		codec    string
		program  string
		input    string
		output   string
	}

	tests := []jTest{
		{
			name: "metadata 1",
			metadata: map[string]string{
				"meta.foo": "12",
				"meta.bar": "34",
			},
			codec:   "none",
			program: `{ print $2 " " meta_foo }`,
			input:   `hello world`,
			output:  `world 12`,
		},
		{
			name: "metadata plus json 1",
			metadata: map[string]string{
				"meta.foo": "12",
				"meta.bar": "34",
			},
			codec:   "json",
			program: `{ print obj_foo " " meta_foo }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello 12`,
		},
		{
			name:     "metadata not exist 1",
			metadata: map[string]string{},
			codec:    "none",
			program:  `{ print $2 meta_foo }`,
			input:    `foo`,
			output:   ``,
		},
		{
			name: "parse metadata datestring 1",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32",
			},
			codec:   "none",
			program: `{ foo = foostamp; print timestamp_unix(foo) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 2",
			metadata: map[string]string{
				"foostamp": "2018TOTALLY12CUSTOM18T11:57:32",
			},
			codec:   "none",
			program: `{ foo = foostamp; print timestamp_unix(foo, "2006TOTALLY01CUSTOM02T15:04:05") }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 3",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32",
			},
			codec:   "none",
			program: `{ print timestamp_unix(foostamp) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 4",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32.123",
			},
			codec:   "none",
			program: `{ foo = foostamp; print timestamp_unix_nano(foo) }`,
			input:   `foo`,
			output:  `1545134252123000064`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.AWK.Codec = test.codec
		conf.AWK.Program = test.program

		a, err := NewAWK(conf, nil, log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := message.New(
			[][]byte{
				[]byte(test.input),
			},
		)
		for k, v := range test.metadata {
			inMsg.Get(0).Metadata().Set(k, v)
		}
		msgs, _ := a.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
