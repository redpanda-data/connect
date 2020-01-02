package metadata

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestMetadataBasic(t *testing.T) {
	expMap := map[string]string{
		"foo":  "bar",
		"foo2": "bar2",
		"foo3": "bar3",
		"foo4": "bar4",
		"foo5": "bar5",
	}
	var m types.Metadata = New(nil)
	for k, v := range expMap {
		// Carry reference over just to test it.
		m = m.Set(k, v)
	}
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

	m = m.Delete("foo")
	if exp, act := "", m.Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	copied := m.Copy()
	if exp, act := "bar2", copied.Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	copied.Set("foo2", "changed")
	if exp, act := "changed", copied.Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "bar2", m.Get("foo2"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMetadataNil(t *testing.T) {
	m := New(nil)
	if exp, act := "", m.Get("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	m.Iter(func(k, v string) error {
		t.Error("Should not be reached")
		return nil
	})
	m.Set("foo", "bar")

	errTest := errors.New("test err")
	if expErr, actErr := errTest, m.Iter(func(k, v string) error {
		if exp, act := "foo", k; exp != act {
			t.Errorf("Wrong value: %v != %v", act, exp)
		}
		if exp, act := "bar", v; exp != act {
			t.Errorf("Wrong value: %v != %v", act, exp)
		}
		return errTest
	}); expErr != actErr {
		t.Errorf("Wrong err: %v != %v", actErr, expErr)
	}
}

//------------------------------------------------------------------------------
