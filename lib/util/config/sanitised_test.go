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

package config

import (
	"encoding/json"
	"testing"

	yaml "gopkg.in/yaml.v2"
)

func TestSanitisedJSON(t *testing.T) {
	exp := `{"type":"foo","a":"a","z":"z"}`
	obj := Sanitised{
		"a":    "a",
		"type": "foo",
		"z":    "z",
	}
	actBytes, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}

	act := string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	exp = `{"type":5,"a":"hello","b":2.3}`
	obj = Sanitised{
		"a":    "hello",
		"b":    2.3,
		"type": 5,
	}
	if actBytes, err = json.Marshal(obj); err != nil {
		t.Fatal(err)
	}

	act = string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	exp = `{"type":"foo"}`
	obj = Sanitised{
		"type": "foo",
	}
	if actBytes, err = json.Marshal(obj); err != nil {
		t.Fatal(err)
	}

	act = string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestSanitisedYAML(t *testing.T) {
	exp := `type: foo
a: a
z: z
`

	obj := Sanitised{
		"a":    "a",
		"type": "foo",
		"z":    "z",
	}
	actBytes, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}

	act := string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}
