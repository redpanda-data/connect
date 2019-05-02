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

	rootFile := []byte(`{"a":{"bar":"baz"},"b":{"$ref":"nest/second.yaml"}}`)
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
`
	if act := string(res); exp != act {
		t.Errorf("Wrong config result: %v != %v", act, exp)
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
