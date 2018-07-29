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
	"testing"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

func TestMetadataSet(t *testing.T) {
	type mTest struct {
		name   string
		key    string
		value  string
		input  string
		output string
	}

	tests := []mTest{
		{
			name:   "set 1",
			key:    "foo.bar",
			value:  `foo bar`,
			input:  `{"foo":{"bar":5}}`,
			output: `foo bar`,
		},
		{
			name:   "set 2",
			key:    "foo.bar",
			value:  `${!json_field:foo.bar}`,
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `hello world`,
		},
		{
			name:   "set 3",
			key:    "foo.bar",
			value:  `hello ${!json_field:foo.bar} world`,
			input:  `{"foo":{"bar":100}}`,
			output: `hello 100 world`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Metadata.Operator = "set"
		conf.Metadata.Key = test.key
		conf.Metadata.Value = test.value

		mSet, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := types.NewMessage(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := mSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, msgs[0].GetMetadata(test.key); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestMetadataDeleteAll(t *testing.T) {
	conf := NewConfig()
	conf.Metadata.Operator = "delete_all"

	mDel, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := types.NewMessage(nil)
	inMsg.SetMetadata("foo", "bar")
	inMsg.SetMetadata("bar", "baz")

	msgs, _ := mDel.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages: %v", len(msgs))
	}

	if err = msgs[0].IterMetadata(func(k, v string) error {
		return fmt.Errorf("Key found: %v", k)
	}); err != nil {
		t.Error(err)
	}
}

func TestMetadataDeletePrefix(t *testing.T) {
	conf := NewConfig()
	conf.Metadata.Operator = "delete_prefix"
	conf.Metadata.Value = "del_"

	mDel, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := types.NewMessage(nil)
	inMsg.SetMetadata("del_foo", "bar")
	inMsg.SetMetadata("foo", "bar2")
	inMsg.SetMetadata("delfoo", "bar3")
	inMsg.SetMetadata("del_bar", "bar4")
	inMsg.SetMetadata("bar", "bar5")
	inMsg.SetMetadata("delbar", "bar6")

	msgs, _ := mDel.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages: %v", len(msgs))
	}

	expMap := map[string]string{
		"foo":    "bar2",
		"delfoo": "bar3",
		"bar":    "bar5",
		"delbar": "bar6",
	}

	if err = msgs[0].IterMetadata(func(k, v string) error {
		if _, exists := expMap[k]; !exists {
			return fmt.Errorf("unexpected key: %v", k)
		}
		if exp, act := expMap[k], v; exp != act {
			return fmt.Errorf("wrong value: %v != %v", act, exp)
		}
		delete(expMap, k)
		return nil
	}); err != nil {
		t.Error(err)
	}

	if len(expMap) > 0 {
		t.Errorf("Lost metadata: %v", expMap)
	}
}
