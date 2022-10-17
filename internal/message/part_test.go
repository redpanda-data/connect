package message

import (
	"reflect"
	"testing"
)

func TestPartBasic(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.MetaSetMut("foo", "bar")
	p.MetaSetMut("foo2", "bar2")

	if exp, act := `{"hello":"world"}`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", p.MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.MetaGetStr("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	jObj, err := p.AsStructuredMut()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"hello": "world"}, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	p.data.rawBytes = nil
	if jObj, err = p.AsStructuredMut(); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"hello": "world"}, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	p.SetBytes([]byte("hello world"))
	if exp, act := `hello world`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if _, err = p.AsStructuredMut(); err == nil {
		t.Errorf("Expected error from bad JSON")
	}

	p.SetStructured(map[string]any{
		"foo": "bar",
	})
	if exp, act := `{"foo":"bar"}`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", p.MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", p.MetaGetStr("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestPartShallowCopy(t *testing.T) {
	p := NewPart([]byte(`{"hello":"world"}`))
	p.MetaSetMut("foo", "bar")
	p.MetaSetMut("foo2", "bar2")

	if _, err := p.AsStructuredMut(); err != nil {
		t.Fatal(err)
	}

	p2 := p.ShallowCopy()
	if exp, act := string(p2.data.rawBytes), string(p.data.rawBytes); exp != act {
		t.Error("Part slices diverged")
	}
	if exp, act := p.data.structured, p2.data.structured; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched json docs: %v != %v", act, exp)
	}
	if exp, act := p.data.metadata, p2.data.metadata; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unmatched metadata types: %v != %v", act, exp)
	}

	p2.MetaSetMut("foo", "new")
	if exp, act := "bar", p.MetaGetStr("foo"); exp != act {
		t.Errorf("Metadata changed after copy: %v != %v", act, exp)
	}
}

func TestPartJSONMarshal(t *testing.T) {
	p := NewPart(nil)
	p.SetStructured(map[string]any{
		"foo": "contains <some> tags & 😊 emojis",
	})
	if exp, act := `{"foo":"contains <some> tags & 😊 emojis"}`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	p.SetStructured(nil)
	if exp, act := `null`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}

	p.SetStructured("<foo>")
	if exp, act := `"<foo>"`, string(p.AsBytes()); exp != act {
		t.Errorf("Wrong marshalled json: %v != %v", act, exp)
	}
}
