package message

import (
	"context"
	"testing"
)

func TestPartWithContext(t *testing.T) {
	p1 := NewPart([]byte(`foobar`))
	if exp, act := context.Background(), GetContext(p1); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}

	type testKey string

	ctx := context.WithValue(context.Background(), testKey("foo"), "bar")
	p2 := WithContext(ctx, p1)

	if exp, act := false, p2.IsEmpty(); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	if exp, act := "foobar", string(p2.AsBytes()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	p2.SetBytes([]byte(`barbaz`))
	if exp, act := "barbaz", string(p1.AsBytes()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}

	if exp, act := ctx, GetContext(p2); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.ShallowCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.DeepCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.ShallowCopy().DeepCopy().ShallowCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}

	ctx = context.WithValue(ctx, testKey("bar"), "baz")
	p3 := WithContext(ctx, p2)

	if exp, act := "barbaz", string(p3.AsBytes()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	p3.SetBytes([]byte(`bazqux`))
	if exp, act := "bazqux", string(p1.AsBytes()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	if exp, act := "bazqux", string(p2.AsBytes()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}

	if exp, act := ctx, GetContext(p3); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.ShallowCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.DeepCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.ShallowCopy().DeepCopy().ShallowCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
}
