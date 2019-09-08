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

package test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	yaml "gopkg.in/yaml.v3"
)

func TestGenerateDefinitions(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml": `
pipeline:
  processors:
  - text:
      ignored: this field is ignored
      operator: to_upper`,

		"unrelated.json": `{"foo":"bar"}`,

		"bar.yaml": `
pipeline:
  processors:
  - text:
      operator: to_upper`,

		"bar_benthos_test.yaml": `
tests:
  - name: example test
    target_processors: '/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
    output_batches:
      -
        - content_equals: example content`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var defaultDefBytes []byte
	if defaultDefBytes, err = yaml.Marshal(ExampleDefinition()); err != nil {
		t.Fatal(err)
	}
	defaultDef := string(defaultDefBytes)

	if err = Generate(testDir, "_benthos_test"); err != nil {
		t.Fatal(err)
	}

	if _, err = os.Stat(filepath.Join(testDir, "unrelated_benthos_test.json")); !os.IsNotExist(err) {
		t.Errorf("Expected not exist error, got %v", err)
	}

	actBytes, err := ioutil.ReadFile(filepath.Join(testDir, "foo_benthos_test.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := defaultDef, string(actBytes); exp != act {
		t.Errorf("Definition does not match default: %v != %v", act, exp)
	}

	if actBytes, err = ioutil.ReadFile(filepath.Join(testDir, "bar_benthos_test.yaml")); err != nil {
		t.Fatal(err)
	}

	if exp, act := defaultDef, string(actBytes); exp == act {
		t.Error("Existing definition was overridden")
	}
}

func TestGenerateDefinitionFile(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml": `
pipeline:
  processors:
  - text:
      ignored: this field is ignored
      operator: to_upper`,

		"unrelated.json": `{"foo":"bar"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	var defaultDefBytes []byte
	if defaultDefBytes, err = yaml.Marshal(ExampleDefinition()); err != nil {
		t.Fatal(err)
	}
	defaultDef := string(defaultDefBytes)

	if err = Generate(filepath.Join(testDir, "foo.yaml"), "_benthos_test"); err != nil {
		t.Fatal(err)
	}

	actBytes, err := ioutil.ReadFile(filepath.Join(testDir, "foo_benthos_test.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := defaultDef, string(actBytes); exp != act {
		t.Errorf("Definition does not match default: %v != %v", act, exp)
	}
}
