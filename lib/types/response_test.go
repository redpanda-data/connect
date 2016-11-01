/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package types

import (
	"errors"
	"testing"
)

func TestSimpleResponse(t *testing.T) {
	err := errors.New("test error")
	res := NewSimpleResponse(err)

	if exp, act := err, res.Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", exp, act)
	}
	if act := res.ErrorMap(); act != nil {
		t.Errorf("Wrong error: %v != %v", nil, act)
	}
}

func TestMappedResponse(t *testing.T) {
	m := NewMappedResponse()
	if exp, act := 0, len(m.Errors); exp != act {
		t.Errorf("Invalid new count of error map: %v != %v", exp, act)
	}
	if act := m.Error(); act != nil {
		t.Errorf("Non nil returned from new Error(): %v", act)
	}
	if act := m.ErrorMap(); act != nil {
		t.Errorf("Non nil returned from new ErrorMap(): %v", act)
	}

	m.Errors[0] = errors.New("0")
	m.Errors[1] = errors.New("1")
	m.Errors[2] = errors.New("2")

	if exp, act := 3, len(m.Errors); exp != act {
		t.Errorf("Invalid edit count of error map: %v != %v", exp, act)
	}
	if exp, act := 3, len(m.ErrorMap()); exp != act {
		t.Errorf("Invalid edit count of error map: %v != %v", exp, act)
	}
	if exp, act := "0", m.ErrorMap()[0].Error(); exp != act {
		t.Errorf("Invalid element from error map: %v != %v", exp, act)
	}
	if exp, act := "1", m.ErrorMap()[1].Error(); exp != act {
		t.Errorf("Invalid element from error map: %v != %v", exp, act)
	}
	if exp, act := "2", m.ErrorMap()[2].Error(); exp != act {
		t.Errorf("Invalid element from error map: %v != %v", exp, act)
	}

	if exp, act := len("map[0:0 1:1 2:2]"), len(m.Error().Error()); exp != act {
		t.Errorf("Invalid string of error map: %v != %v", exp, act)
	}
}
