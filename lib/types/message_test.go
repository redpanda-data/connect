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

func TestMessageSerialization(t *testing.T) {
	m := NewMessage([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	})

	b := m.Bytes()

	m2, err := FromBytes(b)

	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(m.GetAll(), m2.GetAll()) {
		t.Errorf("Messages not equal: %v != %v", m, m2)
	}
}

func TestNewMessage(t *testing.T) {
	m := NewMessage(nil)
	if act := m.Len(); act > 0 {
		t.Errorf("NewMessage returned more than zero message parts: %v", act)
	}
}

func TestMessageInvalidBytesFormat(t *testing.T) {
	cases := [][]byte{
		[]byte(``),
		[]byte(`this is invalid`),
		{0x00, 0x00},
		{0x00, 0x00, 0x00, 0x05},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00},
	}

	for _, c := range cases {
		if _, err := FromBytes(c); err == nil {
			t.Errorf("Received nil error from invalid byte sequence: %s", c)
		}
	}
}

func TestMessageJSONGet(t *testing.T) {
	msg := NewMessage(
		[][]byte{[]byte(`{"foo":{"bar":"baz"}}`)},
	)

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

	exp = map[string]interface{}{
		"foo": map[string]interface{}{
			"bar": "baz2",
		},
	}
	if act := jObj; !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong output from jsonGet: %v != %v", act, exp)
	}
}

func TestMessageJSONSet(t *testing.T) {
	msg := messageImpl{
		parts: [][]byte{[]byte(`hello world`)},
	}

	if err := msg.SetJSON(1, nil); err != ErrMessagePartNotExist {
		t.Errorf("Wrong error returned on bad part: %v != %v", err, ErrMessagePartNotExist)
	}

	p1Obj := map[string]interface{}{
		"foo": map[string]interface{}{
			"bar": "baz",
		},
	}
	p1Str := `{"foo":{"bar":"baz"}}`

	p2Obj := map[string]interface{}{
		"baz": map[string]interface{}{
			"bar": "foo",
		},
	}
	p2Str := `{"baz":{"bar":"foo"}}`

	if err := msg.SetJSON(0, p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	if err := msg.SetJSON(0, p2Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	if err := msg.SetJSON(0, p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
}

func TestMessageJSONSetGet(t *testing.T) {
	msg := messageImpl{
		parts: [][]byte{[]byte(`hello world`)},
	}

	p1Obj := map[string]interface{}{
		"foo": map[string]interface{}{
			"bar": "baz",
		},
	}
	p1Str := `{"foo":{"bar":"baz"}}`

	p2Obj := map[string]interface{}{
		"baz": map[string]interface{}{
			"bar": "foo",
		},
	}
	p2Str := `{"baz":{"bar":"foo"}}`

	var err error
	var jObj interface{}

	if err = msg.SetJSON(0, p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.GetJSON(0); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	if err := msg.SetJSON(0, p2Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.GetJSON(0); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	if err := msg.SetJSON(0, p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.parts[0]); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.GetJSON(0); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}
}

func TestMessageSplitJSON(t *testing.T) {
	msg1 := messageImpl{
		parts: [][]byte{
			[]byte("Foo plain text"),
			[]byte(`nothing here`),
		},
	}

	if err := msg1.SetJSON(1, map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}

	msg2 := msg1.ShallowCopy()

	if exp, act := msg1.parts, msg2.GetAll(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Parts unmatched from shallow copy: %v != %v", act, exp)
	}

	msg2.Set(0, []byte("Bar different text"))

	if exp, act := "Foo plain text", string(msg1.parts[0]); exp != act {
		t.Errorf("Original content was changed from shallow copy: %v != %v", act, exp)
	}

	//------------------

	if err := msg1.SetJSON(1, map[string]interface{}{"foo": "baz"}); err != nil {
		t.Fatal(err)
	}

	jCont, err := msg1.GetJSON(1)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.parts[1]); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}

	jCont, err = msg2.GetJSON(1)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg2.Get(1)); exp != act {
		t.Errorf("Unexpected shallow content: %v != %v", act, exp)
	}

	//------------------

	if err = msg2.SetJSON(1, map[string]interface{}{"foo": "baz2"}); err != nil {
		t.Fatal(err)
	}

	jCont, err = msg2.GetJSON(1)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz2"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz2"}`, string(msg2.Get(1)); exp != act {
		t.Errorf("Unexpected shallow copy content: %v != %v", act, exp)
	}

	jCont, err = msg1.GetJSON(1)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected original json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.parts[1]); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}
}

type dummyCond struct {
	call  func(m Message) bool
	calls int
}

func (d *dummyCond) Check(m Message) bool {
	d.calls++
	return d.call(m)
}

func TestMessageConditionCaching(t *testing.T) {
	msg := messageImpl{
		parts: [][]byte{
			[]byte(`foo`),
		},
	}

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

	msg.Set(0, []byte("bar"))

	if msg.LazyCondition("1", dummyCond1) {
		t.Error("Wrong result from cond 1")
	}
	if msg.LazyCondition("1", dummyCond1) {
		t.Error("Wrong result from cached cond 1")
	}
	if msg.LazyCondition("1", dummyCond2) {
		t.Error("Wrong result from cached cond 1 with cond 2")
	}

	if !msg.LazyCondition("2", dummyCond2) {
		t.Error("Wrong result from cond 2")
	}
	if !msg.LazyCondition("2", dummyCond2) {
		t.Error("Wrong result from cached cond 2")
	}

	if exp, act := 2, dummyCond1.calls; exp != act {
		t.Errorf("Wrong count of calls for cond 1: %v != %v", act, exp)
	}
	if exp, act := 2, dummyCond2.calls; exp != act {
		t.Errorf("Wrong count of calls for cond 2: %v != %v", act, exp)
	}
}

func TestMessageCrossContaminateJSON(t *testing.T) {
	msg1 := messageImpl{
		parts: [][]byte{
			[]byte(`{"foo":"bar"}`),
		},
	}

	var jCont1, jCont2 interface{}
	var err error

	if jCont1, err = msg1.GetJSON(0); err != nil {
		t.Fatal(err)
	}

	msg2 := msg1.ShallowCopy()

	jMap1, ok := jCont1.(map[string]interface{})
	if !ok {
		t.Fatal("Couldnt cast to map")
	}
	jMap1["foo"] = "baz"

	if err = msg1.SetJSON(0, jMap1); err != nil {
		t.Fatal(err)
	}

	if jCont1, err = msg1.GetJSON(0); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont1; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.parts[0]); exp != act {
		t.Errorf("Unexpected raw content: %v != %v", exp, act)
	}

	if jCont2, err = msg2.GetJSON(0); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, jCont2; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg2.Get(0)); exp != act {
		t.Errorf("Unexpected raw content: %v != %v", exp, act)
	}
}

func BenchmarkJSONGet(b *testing.B) {
	sample1 := []byte(`{
	"foo":{
		"bar":"baz",
		"this":{
			"will":{
				"be":{
					"very":{
						"nested":true
					}
				},
				"dont_forget":"me"
			},
			"dont_forget":"me"
		},
		"dont_forget":"me"
	},
	"numbers": [0,1,2,3,4,5,6,7]
}`)
	sample2 := []byte(`{
	"foo2":{
		"bar":"baz2",
		"this":{
			"will":{
				"be":{
					"very":{
						"nested":false
					}
				},
				"dont_forget":"me too"
			},
			"dont_forget":"me too"
		},
		"dont_forget":"me too"
	},
	"numbers": [0,1,2,3,4,5,6,7]
}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := messageImpl{
			parts: [][]byte{sample1},
		}

		jObj, err := msg.GetJSON(0)
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}

		jObj, err = msg.GetJSON(0)
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}

		msg.parts[0] = sample2

		jObj, err = msg.GetJSON(0)
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}
	}
}
