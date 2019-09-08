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

package io

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
)

//------------------------------------------------------------------------------

func TestJSONSerialisation(t *testing.T) {
	msg := message.New([][]byte{
		[]byte("foo"),
		[]byte(`{"bar":"baz"}`),
		[]byte("qux"),
	})

	msg.Get(1).Metadata().Set("meta1", "val1")
	msg.Get(1).Metadata().Set("meta2", "val2")
	msg.Get(2).Metadata().Set("meta3", "val3")

	bytes, err := MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}

	exp := `[{"metadata":{},"value":"foo"},{"metadata":{"meta1":"val1","meta2":"val2"},"value":"{\"bar\":\"baz\"}"},{"metadata":{"meta3":"val3"},"value":"qux"}]`

	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}

	msg = message.New(nil)
	bytes, err = MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}
	exp = `[]`
	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}

	msg.Append(message.NewPart(nil))
	bytes, err = MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}
	exp = `[{"metadata":{},"value":""}]`
	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}
}

func TestJSONBadDeserialisation(t *testing.T) {
	input := `not #%@%$# valuid "" json`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `{"foo":"bar}`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[[]]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[{"value":5}]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[{"value":"5","metadata":[]}]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}
}

func TestJSONDeserialisation(t *testing.T) {
	input := `[]`
	msg, err := MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 0, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}

	input = `[{"metadata":{},"value":""}]`
	msg, err = MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := 0, len(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong size of message part: %v != %v", act, exp)
	}

	input = `[{"metadata":{"meta1":"value1"},"value":"foo"},{"metadata":{"meta2":"value2","meta3":"value3"},"value":"bar"}]`
	msg, err = MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message part value: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msg.Get(1).Get()); exp != act {
		t.Errorf("Wrong message part value: %v != %v", act, exp)
	}
	if exp, act := "value1", msg.Get(0).Metadata().Get("meta1"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
	if exp, act := "value2", msg.Get(1).Metadata().Get("meta2"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
	if exp, act := "value3", msg.Get(1).Metadata().Get("meta3"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
