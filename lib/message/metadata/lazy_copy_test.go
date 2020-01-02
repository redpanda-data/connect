package metadata

import (
	"reflect"
	"testing"
)

//------------------------------------------------------------------------------

func TestLazyCopyBasic(t *testing.T) {
	expMap := map[string]string{
		"foo":  "bar",
		"foo2": "bar2",
		"foo3": "bar3",
		"foo4": "bar4",
		"foo5": "bar5",
	}
	m := LazyCopy(New(expMap))
	if exp, act := "bar", m.Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", m.Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	actMap := map[string]string{}
	m.Iter(func(k, v string) error {
		actMap[k] = v
		return nil
	})
	if exp, act := expMap, actMap; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Delete("foo")
	if exp, act := "", m.Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar", expMap["foo"]; exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Set("foo2", "woah_new")
	if exp, act := "woah_new", m.Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", expMap["foo2"]; exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
