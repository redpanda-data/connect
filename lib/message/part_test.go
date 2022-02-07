package message

import (
	"reflect"
	"testing"
)

func TestPartBasic(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.MetaSet("foo", "bar")
	p.MetaSet("foo2", "bar2")

	if exp, act := `{"hello":"world"}`, string(p.Get()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", p.MetaGet("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.MetaGet("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	jObj, err := p.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]interface{}{"hello": "world"}, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	p.data.rawBytes = nil
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
	if exp, act := "bar", p.MetaGet("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.MetaGet("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestPartShallowCopy(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.MetaSet("foo", "bar")
	p.MetaSet("foo2", "bar2")

	if _, err := p.JSON(); err != nil {
		t.Fatal(err)
	}

	p2 := p.Copy()
	if exp, act := string(p2.data.rawBytes), string(p.data.rawBytes); exp != act {
		t.Error("Part slices diverged")
	}
	if exp, act := p.data.jsonCache, p2.data.jsonCache; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched json docs: %v != %v", act, exp)
	}
	if exp, act := p.data.metadata, p2.data.metadata; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched metadata types: %v != %v", act, exp)
	}

	p2.MetaSet("foo", "new")
	if exp, act := "bar", p.MetaGet("foo"); exp != act {
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
	if exp, act := bytesExp, string(p2.Get()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	p3JSON, err := p3.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := genExp, p3JSON; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
	if exp, act := bytesExp, string(p3.Get()); exp != act {
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
	p.MetaSet("foo", "bar")
	p.MetaSet("foo2", "bar2")

	if _, err := p.JSON(); err != nil {
		t.Fatal(err)
	}

	p2 := p.DeepCopy()
	if exp, act := string(p2.data.rawBytes), string(p.data.rawBytes); exp != act {
		t.Error("Part slices diverged")
	}
	if exp, act := p.data.jsonCache, p2.data.jsonCache; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched json docs: %v != %v", act, exp)
	}
	if exp, act := p.data.metadata, p2.data.metadata; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched metadata types: %v != %v", act, exp)
	}

	p2.data.rawBytes[0] = '['
	if exp, act := `["hello":"world"}`, string(p2.Get()); exp != act {
		t.Errorf("Byte slice wrong: %v != %v", act, exp)
	}
	if exp, act := `{"hello":"world"}`, string(p.Get()); exp != act {
		t.Errorf("Byte slice changed after deep copy: %v != %v", act, exp)
	}
	p2.MetaSet("foo", "new")
	if exp, act := "bar", p.MetaGet("foo"); exp != act {
		t.Errorf("Metadata changed after copy: %v != %v", act, exp)
	}
}
