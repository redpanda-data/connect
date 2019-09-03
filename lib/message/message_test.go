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

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMessageSerialization(t *testing.T) {
	m := New([][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	})

	b := ToBytes(m)

	m2, err := FromBytes(b)

	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(GetAllBytes(m), GetAllBytes(m2)) {
		t.Errorf("Messages not equal: %v != %v", m, m2)
	}
}

func TestNew(t *testing.T) {
	m := New(nil)
	if act := m.Len(); act > 0 {
		t.Errorf("New returned more than zero message parts: %v", act)
	}
}

func TestIter(t *testing.T) {
	parts := [][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
		[]byte(`baz`),
	}
	m := New(parts)
	iters := 0
	m.Iter(func(index int, b types.Part) error {
		if exp, act := string(parts[index]), string(b.Get()); exp != act {
			t.Errorf("Unexpected part: %v != %v", act, exp)
		}
		iters++
		return nil
	})
	if exp, act := 3, iters; exp != act {
		t.Errorf("Wrong count of iterations: %v != %v", act, exp)
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
	msg := New(
		[][]byte{[]byte(`{"foo":{"bar":"baz"}}`)},
	)

	if _, err := msg.Get(1).JSON(); err == nil {
		t.Error("Error not returned on bad part")
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

	msg.Get(0).Set([]byte(`{"foo":{"bar":"baz2"}}`))

	jObj, err = msg.Get(0).JSON()
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
	msg := New([][]byte{[]byte(`hello world`)})

	msg.Get(1).SetJSON(nil)

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

	if err := msg.Get(0).SetJSON(p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	if err := msg.Get(0).SetJSON(p2Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	if err := msg.Get(0).SetJSON(p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
}

func TestMessageMetadata(t *testing.T) {
	m := New([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})

	m.Get(0).Metadata().Set("foo", "bar")
	if exp, act := "bar", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Get(0).Metadata().Set("foo", "bar2")
	if exp, act := "bar2", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Get(0).Metadata().Set("bar", "baz")
	m.Get(0).Metadata().Set("baz", "qux")

	exp := map[string]string{
		"foo": "bar2",
		"bar": "baz",
		"baz": "qux",
	}
	act := map[string]string{}
	m.Get(0).Metadata().Iter(func(k, v string) error {
		act[k] = v
		return nil
	})
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}

	newMetadata := metadata.New(map[string]string{
		"foo": "new1",
		"bar": "new2",
		"baz": "new3",
	})
	m.Get(0).SetMetadata(newMetadata)
	if exp, act := "new1", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "new2", m.Get(0).Metadata().Get("bar"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "", m.Get(-1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "", m.Get(100).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Get(0).SetMetadata(newMetadata)
	m.Get(1).SetMetadata(newMetadata)
	if exp, act := "new1", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "new1", m.Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	newMetadata = metadata.New(map[string]string{
		"foo": "more_new",
	})
	m.Iter(func(i int, p types.Part) error {
		p.SetMetadata(newMetadata)
		return nil
	})
	if exp, act := "more_new", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "more_new", m.Get(1).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMessageCopy(t *testing.T) {
	m := New([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
	})
	m.Get(0).Metadata().Set("foo", "bar")

	m2 := m.Copy()
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m2.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m2.Get(0).Metadata().Set("foo", "bar2")
	m2.Get(0).Set([]byte(`baz`))
	if exp, act := [][]byte{[]byte(`baz`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar2", m2.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMessageDeepCopy(t *testing.T) {
	m := New([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
	})
	m.Get(0).Metadata().Set("foo", "bar")

	m2 := m.DeepCopy()
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m2.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m2.Get(0).Metadata().Set("foo", "bar2")
	m2.Get(0).Set([]byte(`baz`))
	if exp, act := [][]byte{[]byte(`baz`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar2", m2.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m.Get(0).Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMessageJSONSetGet(t *testing.T) {
	msg := New([][]byte{[]byte(`hello world`)})

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

	if err = msg.Get(0).SetJSON(p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	if err := msg.Get(0).SetJSON(p2Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	if err := msg.Get(0).SetJSON(p1Obj); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Str, string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}
}

func TestMessageSplitJSON(t *testing.T) {
	msg1 := New([][]byte{
		[]byte("Foo plain text"),
		[]byte(`nothing here`),
	})

	if err := msg1.Get(1).SetJSON(map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}

	msg2 := msg1.Copy()

	if exp, act := GetAllBytes(msg1), GetAllBytes(msg2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Parts unmatched from shallow copy: %s != %s", act, exp)
	}

	msg2.Get(0).Set([]byte("Bar different text"))

	if exp, act := "Foo plain text", string(msg1.Get(0).Get()); exp != act {
		t.Errorf("Original content was changed from shallow copy: %v != %v", act, exp)
	}

	//------------------

	if err := msg1.Get(1).SetJSON(map[string]interface{}{"foo": "baz"}); err != nil {
		t.Fatal(err)
	}

	jCont, err := msg1.Get(1).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.Get(1).Get()); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}

	jCont, err = msg2.Get(1).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg2.Get(1).Get()); exp != act {
		t.Errorf("Unexpected shallow content: %v != %v", act, exp)
	}

	//------------------

	if err = msg2.Get(1).SetJSON(map[string]interface{}{"foo": "baz2"}); err != nil {
		t.Fatal(err)
	}

	jCont, err = msg2.Get(1).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz2"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz2"}`, string(msg2.Get(1).Get()); exp != act {
		t.Errorf("Unexpected shallow copy content: %v != %v", act, exp)
	}

	jCont, err = msg1.Get(1).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected original json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.Get(1).Get()); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}
}

type dummyCond struct {
	call  func(m types.Message) bool
	calls int
}

func (d *dummyCond) Check(m types.Message) bool {
	d.calls++
	return d.call(m)
}

/*
func TestMessageConditionCaching(t *testing.T) {
	msg := New([][]byte{
		[]byte(`foo`),
	})

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

	msg.Get(0).Set([]byte("bar"))

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
*/

func TestMessageCrossContaminateJSON(t *testing.T) {
	msg1 := New([][]byte{
		[]byte(`{"foo":"bar"}`),
	})

	var jCont1, jCont2 interface{}
	var err error

	if jCont1, err = msg1.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}

	msg2 := msg1.DeepCopy()

	jMap1, ok := jCont1.(map[string]interface{})
	if !ok {
		t.Fatal("Couldnt cast to map")
	}
	jMap1["foo"] = "baz"

	if err = msg1.Get(0).SetJSON(jMap1); err != nil {
		t.Fatal(err)
	}

	if jCont1, err = msg1.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "baz"}, jCont1; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.Get(0).Get()); exp != act {
		t.Errorf("Unexpected raw content: %v != %v", exp, act)
	}

	if jCont2, err = msg2.Get(0).JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"foo": "bar"}, jCont2; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg2.Get(0).Get()); exp != act {
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
		msg := New([][]byte{sample1})

		jObj, err := msg.Get(0).JSON()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}

		jObj, err = msg.Get(0).JSON()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}

		msg.Get(0).Set(sample2)

		jObj, err = msg.Get(0).JSON()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]interface{}); !ok {
			b.Error("Couldn't cast to map")
		}
	}
}
