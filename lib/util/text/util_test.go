package text

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
)

func TestInterpolatedString(t *testing.T) {
	str := NewInterpolatedString("foobar")
	if exp, act := "foobar", str.Get(message.New(nil)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	str = NewInterpolatedString("")
	if exp, act := "", str.Get(message.New(nil)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	str = NewInterpolatedString("foo${!json_field:foo.bar}bar")
	if exp, act := "foonullbar", str.Get(message.New(nil)); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "foobazbar", str.Get(message.New([][]byte{
		[]byte(`{"foo":{"bar":"baz"}}`),
	})); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestInterpolatedBytes(t *testing.T) {
	b := NewInterpolatedBytes([]byte("foobar"))
	if exp, act := "foobar", string(b.Get(message.New(nil))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	b = NewInterpolatedBytes([]byte(""))
	if exp, act := "", string(b.Get(message.New(nil))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	b = NewInterpolatedBytes([]byte(nil))
	if exp, act := "", string(b.Get(message.New(nil))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	b = NewInterpolatedBytes(nil)
	if exp, act := "", string(b.Get(message.New(nil))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	b = NewInterpolatedBytes([]byte("foo${!json_field:foo.bar}bar"))
	if exp, act := "foonullbar", string(b.Get(message.New(nil))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := "foobazbar", string(b.Get(message.New([][]byte{
		[]byte(`{"foo":{"bar":"baz"}}`),
	}))); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}
