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

package message

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message/metadata"
)

func TestPartBasic(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.Metadata().
		Set("foo", "bar").
		Set("foo2", "bar2")

	if exp, act := `{"hello":"world"}`, string(p.Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", p.Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.Metadata().Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	jObj, err := p.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"hello": "world"}, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	p.data = nil
	if jObj, err = p.JSON(); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"hello": "world"}, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	p.Set([]byte("hello world"))
	if exp, act := `hello world`, string(p.Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if _, err = p.JSON(); err == nil {
		t.Errorf("Expected error from bad JSON")
	}

	p.SetJSON(map[string]interface{}{
		"foo": "bar",
	})
	if exp, act := `{"foo":"bar"}`, string(p.Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", p.Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.Metadata().Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	p.SetMetadata(metadata.New(map[string]string{
		"foo": "new_bar",
	}))
	if exp, act := `{"foo":"bar"}`, string(p.Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "new_bar", p.Metadata().Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "", p.Metadata().Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestPartShallowCopy(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.Metadata().
		Set("foo", "bar").
		Set("foo2", "bar2")

	if _, err := p.JSON(); err != nil {
		t.Fatal(err)
	}

	p2 := p.Copy().(*Part)
	if exp, act := string(p2.data), string(p.data); exp != act {
		t.Error("Part slices diverged")
	}
	if exp, act := p.jsonCache, p2.jsonCache; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched json docs: %v != %v", act, exp)
	}
	if exp, act := p.metadata, p2.metadata; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched metadata types: %v != %v", act, exp)
	}

	p2.Metadata().Set("foo", "new")
	if exp, act := "bar", p.Metadata().Get("foo"); exp != act {
		t.Errorf("Metadata changed after copy: %v != %v", act, exp)
	}
}

func TestPartCopyDirtyJSON(t *testing.T) {
	p := NewPart(nil)
	dirtyObj := map[string]int{
		"foo": 1,
		"bar": 2,
		"baz": 3,
	}
	bytesExp := `{"bar":2,"baz":3,"foo":1}`
	genExp := map[string]interface{}{
		"foo": float64(1),
		"bar": float64(2),
		"baz": float64(3),
	}
	p.SetJSON(dirtyObj)

	p2 := p.Copy()
	p3 := p.DeepCopy()

	p2JSON, err := p2.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := dirtyObj, p2JSON; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
	if exp, act := string(bytesExp), string(p2.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	p3JSON, err := p3.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := genExp, p3JSON; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
	if exp, act := string(bytesExp), string(p3.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
}

func TestPartJSONMarshal(t *testing.T) {
	p := NewPart(nil)
	if err := p.SetJSON(map[string]interface{}{
		"foo": "contains <some> tags & 😊 emojis",
	}); err != nil {
		t.Fatal(err)
	}
	if exp, act := `{"foo":"contains <some> tags & 😊 emojis"}`, string(p.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	if err := p.SetJSON(nil); err != nil {
		t.Fatal(err)
	}
	if exp, act := `null`, string(p.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	if err := p.SetJSON("<foo>"); err != nil {
		t.Fatal(err)
	}
	if exp, act := `"<foo>"`, string(p.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
}

func TestPartDeepCopy(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.Metadata().
		Set("foo", "bar").
		Set("foo2", "bar2")

	if _, err := p.JSON(); err != nil {
		t.Fatal(err)
	}

	p2 := p.DeepCopy().(*Part)
	if exp, act := string(p2.data), string(p.data); exp != act {
		t.Error("Part slices diverged")
	}
	if exp, act := p.jsonCache, p2.jsonCache; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched json docs: %v != %v", act, exp)
	}
	if exp, act := p.metadata, p2.metadata; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched metadata types: %v != %v", act, exp)
	}

	p2.data[0] = '['
	if exp, act := `["hello":"world"}`, string(p2.Get()); exp != act {
		t.Errorf("Byte slice wrong: %v != %v", act, exp)
	}
	if exp, act := `{"hello":"world"}`, string(p.Get()); exp != act {
		t.Errorf("Byte slice changed after deep copy: %v != %v", act, exp)
	}
	p2.Metadata().Set("foo", "new")
	if exp, act := "bar", p.Metadata().Get("foo"); exp != act {
		t.Errorf("Metadata changed after copy: %v != %v", act, exp)
	}
}
