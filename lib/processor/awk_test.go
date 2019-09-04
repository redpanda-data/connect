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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
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
	if !HasFailed(msgs[0].Get(0)) {
		t.Error("Expected fail flag on message part")
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
}

func TestAWKBadExitStatus(t *testing.T) {
	conf := NewConfig()
	conf.AWK.Parts = []int{0}
	conf.AWK.Codec = "none"
	conf.AWK.Program = "{ exit 1; print foo }"

	a, err := NewAWK(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New([][]byte{[]byte("this will fail")})
	msgs, res := a.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough for bad input data")
	}
	if res != nil {
		t.Fatal("Non-nil result")
	}
	if exp, act := "this will fail", string(message.GetAllBytes(msgs[0])[0]); exp != act {
		t.Errorf("Wrong output from exit status 1: %v != %v", act, exp)
	}
	if !HasFailed(msgs[0].Get(0)) {
		t.Error("Expected fail flag on message part")
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

func TestAWKJSONParts(t *testing.T) {
	conf := NewConfig()
	conf.AWK.Parts = []int{}
	conf.AWK.Codec = "none"
	conf.AWK.Program = `{
		json_set("foo.bar", json_get("init.val"));
		json_set("foo.bar", json_get("foo.bar") " extra");
	}`

	a, err := NewAWK(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgIn := message.New([][]byte{
		[]byte(`{"init":{"val":"first"}}`),
		[]byte(`{"init":{"val":"second"}}`),
		[]byte(`{"init":{"val":"third"}}`),
		[]byte(`{"init":{"val":"fourth"}}`),
	})
	msgs, res := a.ProcessMessage(msgIn)
	if len(msgs) != 1 {
		t.Fatal("No passthrough on error")
	}
	if res != nil {
		t.Fatalf("Non-nil result: %v", res.Error())
	}
	exp := [][]byte{
		[]byte(`{"foo":{"bar":"first extra"},"init":{"val":"first"}}`),
		[]byte(`{"foo":{"bar":"second extra"},"init":{"val":"second"}}`),
		[]byte(`{"foo":{"bar":"third extra"},"init":{"val":"third"}}`),
		[]byte(`{"foo":{"bar":"fourth extra"},"init":{"val":"fourth"}}`),
	}
	if act := message.GetAllBytes(msgs[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong output from json functions: %s != %s", act, exp)
	}
}

func TestAWK(t *testing.T) {
	type jTest struct {
		name          string
		metadata      map[string]string
		metadataAfter map[string]string
		codec         string
		program       string
		input         string
		output        string
	}

	tests := []jTest{
		{
			name:    "no print 1",
			codec:   "none",
			program: `{ }`,
			input:   `hello world`,
			output:  `hello world`,
		},
		{
			name:    "empty print 1",
			codec:   "none",
			program: `{ print "" }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name: "metadata get 1",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.foo") }`,
			input:   `hello world`,
			output:  `12`,
		},
		{
			name: "metadata get 2",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.bar") }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name: "metadata set 1",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			metadataAfter: map[string]string{
				"meta.foo": "24",
				"meta.bar": "36",
			},
			codec:   "none",
			program: `{ metadata_set("meta.foo", 24); metadata_set("meta.bar", "36") }`,
			input:   `hello world`,
			output:  `hello world`,
		},
		{
			name:    "json get 1",
			codec:   "none",
			program: `{ print json_get("obj.foo") }`,
			input:   `{"obj":{"foo":12}}`,
			output:  `12`,
		},
		{
			name:    "json get 2",
			codec:   "none",
			program: `{ print json_get("obj.bar") }`,
			input:   `{"obj":{"foo":12}}`,
			output:  `null`,
		},
		{
			name:    "json get array 1",
			codec:   "none",
			program: `{ print json_get("obj.1.foo") }`,
			input:   `{"obj":[{"foo":11},{"foo":12}]}`,
			output:  `12`,
		},
		{
			name:    "json set array 1",
			codec:   "none",
			program: `{ json_set("obj.1.foo", "nope") }`,
			input:   `{"obj":[{"foo":11},{"foo":12}]}`,
			output:  `{"obj":[{"foo":11},{"foo":"nope"}]}`,
		},
		{
			name:    "json get 3",
			codec:   "none",
			program: `{ print json_get("obj.bar") }`,
			input:   `not json content`,
			output:  `not json content`,
		},
		{
			name:    "json get 4",
			codec:   "none",
			program: `{ print json_get("obj.foo") }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello`,
		},
		{
			name:    "json set 1",
			codec:   "none",
			program: `{ json_set("obj.foo", "hello world") }`,
			input:   `{}`,
			output:  `{"obj":{"foo":"hello world"}}`,
		},
		{
			name:    "json set 2",
			codec:   "none",
			program: `{ json_set("obj.foo", "hello world") }`,
			input:   `not json content`,
			output:  `not json content`,
		},
		{
			name:    "json delete 1",
			codec:   "none",
			program: `{ json_delete("obj.foo") }`,
			input:   `{"obj":{"foo":"hello world","bar":"baz"}}`,
			output:  `{"obj":{"bar":"baz"}}`,
		},
		{
			name:    "json delete 2",
			codec:   "none",
			program: `{ json_delete("obj.foo") }`,
			input:   `not json content`,
			output:  `not json content`,
		},
		{
			name:    "json delete 3",
			codec:   "none",
			program: `{ json_delete("obj") }`,
			input:   `{"obj":{"foo":"hello world"}}`,
			output:  `{}`,
		},
		{
			name:  "json set, get and set again",
			codec: "none",
			program: `{
				 json_set("obj.foo", "hello world");
				 json_set("obj.foo", json_get("obj.foo") " 123");
			}`,
			input:  `{"obj":{"foo":"nope"}}`,
			output: `{"obj":{"foo":"hello world 123"}}`,
		},
		{
			name:    "json set int 1",
			codec:   "none",
			program: `{ json_set_int("obj.foo", 5) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":5}}`,
		},
		{
			name:    "json set float 1",
			codec:   "none",
			program: `{ json_set_float("obj.foo", 5.3) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":5.3}}`,
		},
		{
			name:    "json set bool 1",
			codec:   "none",
			program: `{ json_set_bool("obj.foo", "foo" == "foo") }`,
			input:   `{}`,
			output:  `{"obj":{"foo":true}}`,
		},
		{
			name: "metadata get 2",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.bar") }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name:    "json 1",
			codec:   "json",
			program: `{ print obj_foo }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello`,
		},
		{
			name: "metadata 1",
			metadata: map[string]string{
				"meta.foo": "12",
				"meta.bar": "34",
			},
			codec:   "text",
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
			codec:   "text",
			program: `{ foo = foostamp; print timestamp_unix(foo) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 2",
			metadata: map[string]string{
				"foostamp": "2018TOTALLY12CUSTOM18T11:57:32",
			},
			codec:   "text",
			program: `{ foo = foostamp; print timestamp_unix(foo, "2006TOTALLY01CUSTOM02T15:04:05") }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 3",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32",
			},
			codec:   "text",
			program: `{ print timestamp_unix(foostamp) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "format metadata unix custom 1",
			metadata: map[string]string{
				"foostamp": "1545134252",
			},
			codec:   "text",
			program: `{ print timestamp_format(foostamp, "02 Jan 06 15:04") }`,
			input:   `foo`,
			output:  `18 Dec 18 11:57`,
		},
		{
			name: "format metadata unix nano custom 1",
			metadata: map[string]string{
				"foostamp": "1545134252123000064",
			},
			codec:   "text",
			program: `{ print timestamp_format_nano(foostamp, "02 Jan 06 15:04:05.000000000") }`,
			input:   `foo`,
			output:  `18 Dec 18 11:57:32.123000064`,
		},
		{
			name:    "create json object 1",
			codec:   "none",
			program: `{ print create_json_object("foo", "1", "bar", "2", "baz", "3") }`,
			input:   `this is ignored`,
			output:  `{"bar":"2","baz":"3","foo":"1"}`,
		},
		{
			name:    "create json object 2",
			codec:   "none",
			program: `{ print create_json_object("foo", "1", "bar", 2, "baz", "true") }`,
			input:   `this is ignored`,
			output:  `{"bar":"2","baz":"true","foo":"1"}`,
		},
		{
			name:    "create json object 3",
			codec:   "none",
			program: `{ print create_json_object() }`,
			input:   `this is ignored`,
			output:  `{}`,
		},
		{
			name:    "create json array 1",
			codec:   "none",
			program: `{ print create_json_array("1", 2, "3") }`,
			input:   `this is ignored`,
			output:  `["1","2","3"]`,
		},
		{
			name:    "create json array 2",
			codec:   "none",
			program: `{ print create_json_array() }`,
			input:   `this is ignored`,
			output:  `[]`,
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

		if exp := test.metadataAfter; len(exp) > 0 {
			act := map[string]string{}
			msgs[0].Get(0).Metadata().Iter(func(k, v string) error {
				act[k] = v
				return nil
			})
			if !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong metadata contents: %v != %v", act, exp)
			}
		}

		if exp, act := test.output, string(message.GetAllBytes(msgs[0])[0]); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}
