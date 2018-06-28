// Copyright (c) 2014 Ashley Jeffs
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

package types

import (
	"reflect"
	"testing"
)

func TestLockedMessageGet(t *testing.T) {
	m := LockMessage(NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 0)

	exp := []byte("hello")
	if act := m.Get(0); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}

	m = LockMessage(NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 1)

	exp = []byte("world")
	if act := m.Get(0); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockedMessageSerialisation(t *testing.T) {
	m := LockMessage(NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 0)
	m2 := NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	})

	if act, exp := m.Bytes(), m2.Bytes(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockedMessageGetAll(t *testing.T) {
	m := LockMessage(NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 0)

	exp := [][]byte{
		[]byte("hello"),
	}
	if act := m.GetAll(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}

	m = LockMessage(NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 1)

	exp = [][]byte{
		[]byte("world"),
	}
	if act := m.GetAll(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockNewMessage(t *testing.T) {
	m := LockMessage(NewMessage(nil), 0)
	if act := m.Len(); act > 0 {
		t.Errorf("NewMessage returned more than zero message parts: %v", act)
	}
}

func TestLockedMessageJSONGet(t *testing.T) {
	msg := LockMessage(NewMessage(
		[][]byte{[]byte(`{"foo":{"bar":"baz"}}`)},
	), 0)

	if _, err := msg.GetJSON(1); err != ErrMessagePartNotExist {
		t.Errorf("Wrong error returned on bad part: %v != %v", err, ErrMessagePartNotExist)
	}

	jObj, err := msg.GetJSON(0)
	if err != nil {
		t.Error(err)
	}

	exp := map[string]interface{}{
		"foo": map[string]interface{}{
			"bar": "baz",
		},
	}
	if act := jObj; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong output from jsonGet: %v != %v", act, exp)
	}

	msg.Set(0, []byte(`{"foo":{"bar":"baz2"}}`))

	jObj, err = msg.GetJSON(0)
	if err != nil {
		t.Error(err)
	}
	if act := jObj; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong output from jsonGet: %v != %v", act, exp)
	}
}

func TestLockedMessageConditionCaching(t *testing.T) {
	msg := LockMessage(&messageImpl{
		parts: [][]byte{
			[]byte(`foo`),
		},
	}, 0)

	dummyCond1 := &dummyCond{
		call: func(m Message) bool {
			return string(m.Get(0)) == "foo"
		},
	}
	dummyCond2 := &dummyCond{
		call: func(m Message) bool {
			return string(m.Get(0)) == "bar"
		},
	}

	if !msg.LazyCondition("1", dummyCond1) {
		t.Error("Wrong result from cond 1")
	}
	if !msg.LazyCondition("1", dummyCond1) {
		t.Error("Wrong result from cached cond 1")
	}
	if !msg.LazyCondition("1", dummyCond2) {
		t.Error("Wrong result from cached cond 1 with cond 2")
	}

	if msg.LazyCondition("2", dummyCond2) {
		t.Error("Wrong result from cond 2")
	}
	if msg.LazyCondition("2", dummyCond2) {
		t.Error("Wrong result from cached cond 2")
	}

	if exp, act := 1, dummyCond1.calls; exp != act {
		t.Errorf("Wrong count of calls for cond 1: %v != %v", act, exp)
	}
	if exp, act := 1, dummyCond2.calls; exp != act {
		t.Errorf("Wrong count of calls for cond 2: %v != %v", act, exp)
	}
}
