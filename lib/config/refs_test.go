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

package config

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

func TestConfigRefs(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_config_ref_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	rootPath := filepath.Join(tmpDir, "root.yaml")
	secondPath := filepath.Join(tmpDir, "./nest/second.yaml")
	thirdPath := filepath.Join(tmpDir, "./third.yaml")

	if err = os.Mkdir(filepath.Join(tmpDir, "nest"), 0777); err != nil {
		t.Fatal(err)
	}

	rootFile := []byte(`{"a":{"bar":"baz"},"b":{"$ref":"nest/second.yaml"},"d":{"$ref":"./nest/second.yaml#/2"}}`)
	secondFile := []byte(`["foo",{"$ref":"../third.yaml"},"bar"]`)
	thirdFile := []byte(`{"c":[9,8,7]}`)

	if err = ioutil.WriteFile(rootPath, rootFile, 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(secondPath, secondFile, 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(thirdPath, thirdFile, 0777); err != nil {
		t.Fatal(err)
	}

	res, err := readWithJSONRefs(rootPath, true)
	if err != nil {
		t.Fatal(err)
	}

	exp := `a:
  bar: baz
b:
- foo
- c:
  - 9
  - 8
  - 7
- bar
d: bar
`
	if act := string(res); exp != act {
		t.Errorf("Wrong config result: %v != %v", act, exp)
	}
}

func TestConfigRefsRootExpansion(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_config_ref_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	rootPath := filepath.Join(tmpDir, "root.yaml")
	secondPath := filepath.Join(tmpDir, "./nest/second.yaml")
	thirdPath := filepath.Join(tmpDir, "./third.yaml")

	if err = os.Mkdir(filepath.Join(tmpDir, "nest"), 0777); err != nil {
		t.Fatal(err)
	}

	rootFile := []byte(`{"a":{"bar":"baz"},"b":{"$ref":"nest/second.yaml"}}`)
	secondFile := []byte(`{"$ref":"../third.yaml"}`)
	thirdFile := []byte(`{"c":[9,8,7]}`)

	if err = ioutil.WriteFile(rootPath, rootFile, 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(secondPath, secondFile, 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(thirdPath, thirdFile, 0777); err != nil {
		t.Fatal(err)
	}

	res, err := readWithJSONRefs(rootPath, true)
	if err != nil {
		t.Fatal(err)
	}

	exp := `a:
  bar: baz
b:
  c:
  - 9
  - 8
  - 7
`
	if act := string(res); exp != act {
		t.Errorf("Wrong config result: %v != %v", act, exp)
	}
}

func TestJSONPointer(t *testing.T) {
	sample := []byte(`{
		"a": {
			"nested1": {
				"value1": 5
			}
		},
		"": {
			"can we access": "this?"
		},
		"what/a/pain": "ouch1",
		"what~a~pain": "ouch2",
		"what~/a/~pain": "ouch3",
		"b": 10,
		"c": [
			"first",
			"second",
			{
				"nested2": {
					"value2": 15
				}
			},
			[
				"fifth",
				"sixth"
			],
			"fourth"
		],
		"d": {
			"": {
				"what about": "this?"
			}
		}
	}`)

	type testCase struct {
		path  string
		value string
		err   string
	}
	tests := []testCase{
		{
			path: "foo",
			err:  "failed to resolve JSON pointer: path must begin with '/'",
		},
		{
			path: "/a/doesnotexist",
			err:  "failed to resolve JSON pointer: index '1' value 'doesnotexist' was not found",
		},
		{
			path: "/a",
			value: `nested1:
  value1: 5
`,
		},
		{
			path: "/what~1a~1pain",
			value: `ouch1
`,
		},
		{
			path: "/what~0a~0pain",
			value: `ouch2
`,
		},
		{
			path: "/what~0~1a~1~0pain",
			value: `ouch3
`,
		},
		{
			path: "/",
			value: `can we access: this?
`,
		},
		{
			path: "//can we access",
			value: `this?
`,
		},
		{
			path: "/d/",
			value: `what about: this?
`,
		},
		{
			path: "/d//what about",
			value: `this?
`,
		},
		{
			path: "/c/1",
			value: `second
`,
		},
		{
			path: "/c/2/nested2/value2",
			value: `15
`,
		},
		{
			path: "/c/notindex/value2",
			err:  `failed to resolve JSON pointer: could not parse index '1' value 'notindex' into array index: strconv.Atoi: parsing "notindex": invalid syntax`,
		},
		{
			path: "/c/10/value2",
			err:  `failed to resolve JSON pointer: index '1' value '10' exceeded target array size of '5'`,
		},
	}

	var root interface{}
	if err := yaml.Unmarshal(sample, &root); err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.path, func(tt *testing.T) {
			result, err := jsonPointer(test.path, root)
			if len(test.err) > 0 {
				if err == nil {
					tt.Errorf("Expected error: %v", test.err)
				} else if exp, act := test.err, err.Error(); exp != act {
					tt.Errorf("Wrong error returned: %v != %v", act, exp)
				}
				return
			} else if err != nil {
				tt.Error(err)
				return
			}
			resBytes, err := yaml.Marshal(result)
			if err != nil {
				tt.Fatal(err)
			}
			if exp, act := test.value, string(resBytes); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
		})
	}
}

func TestLocalRefs(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_config_ref_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	rootPath := filepath.Join(tmpDir, "root.yaml")
	rootFile := []byte(`{"a":{"bar":"baz"},"b":{"$ref":"#/a/bar"},"c":{"$ref":"#/b"}}`)

	if err = ioutil.WriteFile(rootPath, rootFile, 0777); err != nil {
		t.Fatal(err)
	}

	res, err := readWithJSONRefs(rootPath, true)
	if err != nil {
		t.Fatal(err)
	}

	exp := `a:
  bar: baz
b: baz
c: baz
`
	if act := string(res); exp != act {
		t.Errorf("Wrong config result: %v != %v", act, exp)
	}
}

func TestRecursiveRefs(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_config_ref_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	fooPath := filepath.Join(tmpDir, "./foo.yaml")
	barPath := filepath.Join(tmpDir, "./foo.yaml")

	fooFile := []byte(`{"a":{"$ref":"./bar.yaml"}}`)
	barFile := []byte(`{"b":{"$ref":"./foo.yaml"}}`)

	if err = ioutil.WriteFile(fooPath, fooFile, 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(barPath, barFile, 0777); err != nil {
		t.Fatal(err)
	}

	_, err = readWithJSONRefs(fooPath, true)
	if exp, act := ErrExceededRefLimit, err; exp != act {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}
}

func TestConfigNoRefs(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "benthos_config_ref_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(tmpDir); err != nil {
			t.Error(err)
		}
	}()

	rootPath := filepath.Join(tmpDir, "root.yaml")
	rootFile := []byte(`{"foo":{ "bar":"baz"}}`)

	if err = ioutil.WriteFile(rootPath, rootFile, 0777); err != nil {
		t.Fatal(err)
	}

	res, err := readWithJSONRefs(rootPath, true)
	if err != nil {
		t.Fatal(err)
	}

	exp := `{"foo":{ "bar":"baz"}}`
	if act := string(res); exp != act {
		t.Errorf("Wrong config result: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
