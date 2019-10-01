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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestMetadataSet(t *testing.T) {
	type mTest struct {
		name     string
		inputKey string
		key      string
		value    string
		input    string
		output   string
	}

	tests := []mTest{
		{
			name:     "set 1",
			inputKey: "foo.bar",
			key:      "foo.bar",
			value:    `foo bar`,
			input:    `{"foo":{"bar":5}}`,
			output:   `foo bar`,
		},
		{
			name:     "set 2",
			inputKey: "foo.bar",
			key:      "foo.bar",
			value:    `${!json_field:foo.bar}`,
			input:    `{"foo":{"bar":"hello world"}}`,
			output:   `hello world`,
		},
		{
			name:     "set 3",
			inputKey: "foo.bar",
			key:      "foo.bar",
			value:    `hello ${!json_field:foo.bar} world`,
			input:    `{"foo":{"bar":100}}`,
			output:   `hello 100 world`,
		},
		{
			name:     "set 4",
			inputKey: "key-${!json_field:key}",
			key:      "key-custom-key",
			value:    `hello world`,
			input:    `{"key":"custom-key"}`,
			output:   `hello world`,
		},
	}

	for _, test := range tests {
		conf := NewConfig()
		conf.Metadata.Operator = "set"
		conf.Metadata.Key = test.inputKey
		conf.Metadata.Value = test.value

		mSet, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
		if err != nil {
			t.Fatalf("Error for test '%v': %v", test.name, err)
		}

		inMsg := message.New(
			[][]byte{
				[]byte(test.input),
			},
		)
		msgs, _ := mSet.ProcessMessage(inMsg)
		if len(msgs) != 1 {
			t.Fatalf("Test '%v' did not succeed", test.name)
		}

		if exp, act := test.output, msgs[0].Get(0).Metadata().Get(test.key); exp != act {
			t.Errorf("Wrong result '%v': %v != %v", test.name, act, exp)
		}
	}
}

func TestMetadataSetParts(t *testing.T) {
	conf := NewConfig()
	conf.Metadata.Parts = []int{1}
	conf.Metadata.Operator = "set"
	conf.Metadata.Key = "foo"
	conf.Metadata.Value = "changed"

	mSet, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	})
	inMsg.Get(0).Metadata().Set("foo", "1")
	inMsg.Get(1).Metadata().Set("foo", "2")
	inMsg.Get(2).Metadata().Set("foo", "3")

	msgs, _ := mSet.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages: %v", len(msgs))
	}

	if exp, act := "1", msgs[0].Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
	if exp, act := "changed", msgs[0].Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
	if exp, act := "3", msgs[0].Get(2).Metadata().Get("foo"); exp != act {
		t.Errorf("Unexpected value: %v != %v", act, exp)
	}
}

func TestMetadataDeleteAll(t *testing.T) {
	conf := NewConfig()
	conf.Metadata.Operator = "delete_all"

	mDel, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.New([][]byte{[]byte("")})
	inMsg.Get(0).Metadata().Set("bar", "baz")
	inMsg.Get(0).Metadata().Set("foo", "bar")

	msgs, _ := mDel.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages: %v", len(msgs))
	}

	if err = msgs[0].Get(0).Metadata().Iter(func(k, v string) error {
		return fmt.Errorf("key found: %v", k)
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

	inMsg := message.New([][]byte{[]byte("")})
	inMsg.Get(0).Metadata().Set("del_foo", "bar")
	inMsg.Get(0).Metadata().Set("foo", "bar2")
	inMsg.Get(0).Metadata().Set("delfoo", "bar3")
	inMsg.Get(0).Metadata().Set("del_bar", "bar4")
	inMsg.Get(0).Metadata().Set("bar", "bar5")
	inMsg.Get(0).Metadata().Set("delbar", "bar6")

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

	if err = msgs[0].Get(0).Metadata().Iter(func(k, v string) error {
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

func TestMetadataDelete(t *testing.T) {
	conf := NewConfig()
	conf.Metadata.Operator = "delete"
	conf.Metadata.Value = "delete_me"

	mDel, err := NewMetadata(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	inMsg := message.New([][]byte{[]byte("")})
	inMsg.Get(0).Metadata().Set("foo", "bar")
	inMsg.Get(0).Metadata().Set("delete_me_not", "bar2")
	inMsg.Get(0).Metadata().Set("delete_me", "bar3")
	inMsg.Get(0).Metadata().Set("delbar", "bar4")

	msgs, _ := mDel.ProcessMessage(inMsg)
	if len(msgs) != 1 {
		t.Fatalf("Wrong count of messages: %v", len(msgs))
	}

	expMap := map[string]string{
		"foo":           "bar",
		"delete_me_not": "bar2",
		"delbar":        "bar4",
	}

	if err = msgs[0].Get(0).Metadata().Iter(func(k, v string) error {
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
