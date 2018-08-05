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
