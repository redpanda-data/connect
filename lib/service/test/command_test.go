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
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGetBothPaths(t *testing.T) {
	type testCase struct {
		input  string
		output [2]string
	}

	tests := []testCase{
		{
			input: "/foo/bar/baz.yaml",
			output: [2]string{
				"/foo/bar/baz.yaml",
				"/foo/bar/baz_benthos_test.yaml",
			},
		},
		{
			input: "baz.yaml",
			output: [2]string{
				"baz.yaml",
				"baz_benthos_test.yaml",
			},
		},
		{
			input: "./foo/bar/baz_benthos_test.yaml",
			output: [2]string{
				"foo/bar/baz.yaml",
				"foo/bar/baz_benthos_test.yaml",
			},
		},
		{
			input: "baz_benthos_test.yaml",
			output: [2]string{
				"baz.yaml",
				"baz_benthos_test.yaml",
			},
		},
		{
			input: "/foo/bar/baz.foo",
			output: [2]string{
				"/foo/bar/baz.foo",
				"/foo/bar/baz_benthos_test.foo",
			},
		},
		{
			input: "baz",
			output: [2]string{
				"baz",
				"baz_benthos_test",
			},
		},
		{
			input: "/foo/bar/baz_benthos_test.foo",
			output: [2]string{
				"/foo/bar/baz.foo",
				"/foo/bar/baz_benthos_test.foo",
			},
		},
		{
			input: "baz_benthos_test",
			output: [2]string{
				"baz",
				"baz_benthos_test",
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("Test case %v", i), func(tt *testing.T) {
			cPath, dPath := getBothPaths(test.input)
			if exp, act := test.output[0], cPath; exp != act {
				tt.Errorf("Wrong config path: %v != %v", act, exp)
			}
			if exp, act := test.output[1], dPath; exp != act {
				tt.Errorf("Wrong definition path: %v != %v", act, exp)
			}
		})
	}
}

func TestGetTargetsSingle(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml":              `foobar`,
		"foo_benthos_test.yaml": `foobar`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	paths, err := getTestTargets(filepath.Join(testDir, "foo.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "foo_benthos_test.yaml"), paths[0]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}

	paths, err = getTestTargets(filepath.Join(testDir, "foo_benthos_test.yaml"), false)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "foo_benthos_test.yaml"), paths[0]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}
}

func TestGetTargetsSingleError(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml":              `foobar`,
		"bar_benthos_test.yaml": `foobar`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = getTestTargets(filepath.Join(testDir, "foo.yaml"), false); err == nil {
		t.Error("Expected error")
	}
	if _, err = getTestTargets(filepath.Join(testDir, "bar_benthos_test.yaml"), false); err == nil {
		t.Error("Expected error")
	}
	if _, err = getTestTargets("/does/not/exist/foo.yaml", false); err == nil {
		t.Error("Expected error")
	}
}

func TestGetTargetsDir(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml":                     `foobar`,
		"foo_benthos_test.yaml":        `foobar`,
		"bar.yaml":                     `foobar`,
		"bar_benthos_test.yaml":        `foobar`,
		"nested/baz.yaml":              `foobar`,
		"nested/baz_benthos_test.yaml": `foobar`,
		"ignored.yaml":                 `foobar`,
		"nested/also_ignored.yaml":     `foobar`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	paths, err := getTestTargets(testDir, false)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "bar_benthos_test.yaml"), paths[0]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "foo_benthos_test.yaml"), paths[1]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}

	paths, err = getTestTargets(testDir, true)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 3, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "bar_benthos_test.yaml"), paths[0]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "foo_benthos_test.yaml"), paths[1]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}
	if exp, act := filepath.Join(testDir, "nested/baz_benthos_test.yaml"), paths[2]; exp != act {
		t.Errorf("Wrong path returned: %v != %v", act, exp)
	}
}

func TestGetTargetsDirError(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo_benthos_test.yaml": `foobar`,
		"bar.yaml":              `foobar`,
		"bar_benthos_test.yaml": `foobar`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = getTestTargets(testDir, false); err == nil {
		t.Error("Expected error")
	}
}

func TestGetTargetsDirRecurseError(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml":                     `foobar`,
		"foo_benthos_test.yaml":        `foobar`,
		"bar.yaml":                     `foobar`,
		"bar_benthos_test.yaml":        `foobar`,
		"nested/baz_benthos_test.yaml": `foobar`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = getTestTargets(testDir, true); err == nil {
		t.Error("Expected error")
	}
}

func TestCommandRunHappy(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"foo.yaml": `
pipeline:
  processors:
  - text:
      ignored: this field is ignored
      operator: to_upper`,
		"foo_benthos_test.yaml": `
tests:
  - name: example test
    target_processors: '/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
    output_batches:
      -
        - content_equals: EXAMPLE CONTENT`,
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

	if !Run(filepath.Join(testDir, "foo.yaml"), false) {
		t.Error("Unexpected result")
	}

	if Run(filepath.Join(testDir, "foo.yaml"), true) {
		t.Error("Unexpected result")
	}

	if Run(testDir, true) {
		t.Error("Unexpected result")
	}
}
