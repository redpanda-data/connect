// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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
