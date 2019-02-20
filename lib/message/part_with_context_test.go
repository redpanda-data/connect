// Copyright (c) 2019 Ashley Jeffs
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
	if exp, act := "foobar", string(p2.Get()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	p2.Set([]byte(`barbaz`))
	if exp, act := "barbaz", string(p1.Get()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}

	if exp, act := ctx, GetContext(p2); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.Copy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.DeepCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p2.Copy().DeepCopy().Copy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}

	ctx = context.WithValue(ctx, testKey("bar"), "baz")
	p3 := WithContext(ctx, p2)

	if exp, act := "barbaz", string(p3.Get()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	p3.Set([]byte(`bazqux`))
	if exp, act := "bazqux", string(p1.Get()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
	if exp, act := "bazqux", string(p2.Get()); exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}

	if exp, act := ctx, GetContext(p3); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.Copy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.DeepCopy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
	if exp, act := ctx, GetContext(p3.Copy().DeepCopy().Copy()); exp != act {
		t.Errorf("Wrong context returned: %v != %v", act, exp)
	}
}
