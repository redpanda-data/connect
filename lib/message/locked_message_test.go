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

package message

import (
	"reflect"
	"testing"
)

func TestLockedMessageGet(t *testing.T) {
	m := Lock(New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 0)

	exp := []byte("hello")
	if act := m.Get(0).Get(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}

	m = Lock(New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 1)

	exp = []byte("world")
	if act := m.Get(0).Get(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockedMessageCopy(t *testing.T) {
	root := New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	})
	m := Lock(root, 1)
	copied := m.Copy()
	root.Get(1).Get()[2] = '@'

	exp := []byte("wo@ld")
	if act := copied.Get(0).Get(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}

	root.Get(1).Set([]byte("world"))
	exp = []byte("wo@ld")
	if act := copied.Get(0).Get(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockedMessageDeepCopy(t *testing.T) {
	root := New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	})
	m := Lock(root, 1)
	copied := m.DeepCopy()
	root.Get(1).Get()[2] = '@'

	exp := []byte("world")
	if act := copied.Get(0).Get(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockedMessageGetAll(t *testing.T) {
	m := Lock(New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 0)

	exp := [][]byte{
		[]byte("hello"),
	}
	if act := GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}

	m = Lock(New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}), 1)

	exp = [][]byte{
		[]byte("world"),
	}
	if act := GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Messages not equal: %s != %s", exp, act)
	}
}

func TestLockNew(t *testing.T) {
	m := Lock(New(nil), 0)
	if act := m.Len(); act > 0 {
		t.Errorf("New returned more than zero message parts: %v", act)
	}
}

func TestLockedMessageJSONGet(t *testing.T) {
	msg := Lock(New(
		[][]byte{[]byte(`{"foo":{"bar":"baz"}}`)},
	), 0)

	if _, err := msg.Get(1).JSON(); err != ErrMessagePartNotExist {
		t.Errorf("Wrong error returned on bad part: %v != %v", err, ErrMessagePartNotExist)
	}

	jObj, err := msg.Get(0).JSON()
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
}

/*
func TestLockedMessageConditionCaching(t *testing.T) {
	msg := Lock(New([][]byte{
		[]byte(`foo`),
	}), 0)

	dummyCond1 := &dummyCond{
		call: func(m types.Message) bool {
			return string(m.Get(0).Get()) == "foo"
		},
	}
	dummyCond2 := &dummyCond{
		call: func(m types.Message) bool {
			return string(m.Get(0).Get()) == "bar"
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
*/
