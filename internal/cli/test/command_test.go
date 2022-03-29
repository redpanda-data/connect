package test_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
	"github.com/benthosdev/benthos/v4/internal/log"
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

	for i, testDef := range tests {
		t.Run(fmt.Sprintf("Test case %v", i), func(tt *testing.T) {
			cPath, dPath := test.GetPathPair(testDef.input, "_benthos_test")
			if exp, act := testDef.output[0], cPath; exp != act {
				tt.Errorf("Wrong config path: %v != %v", act, exp)
			}
			if exp, act := testDef.output[1], dPath; exp != act {
				tt.Errorf("Wrong definition path: %v != %v", act, exp)
			}
		})
	}
}

func TestGetTargetsSingle(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo.yaml":              `tests: [{}]`,
		"foo_benthos_test.yaml": `tests: [{}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	paths, err := test.GetTestTargets([]string{filepath.Join(testDir, "foo.yaml")}, "_benthos_test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if _, exists := paths[filepath.Join(testDir, "foo.yaml")]; !exists {
		t.Errorf("Wrong path returned: %v does not contain foo.yaml", paths)
	}

	paths, err = test.GetTestTargets([]string{filepath.Join(testDir, "foo_benthos_test.yaml")}, "_benthos_test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if _, exists := paths[filepath.Join(testDir, "foo.yaml")]; !exists {
		t.Errorf("Wrong path returned: %v does not contain foo.yaml", paths)
	}
}

func TestGetTargetsSingleError(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo.yaml":              `foobar: {}`,
		"bar_benthos_test.yaml": `tests: [{}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = test.GetTestTargets([]string{filepath.Join(testDir, "bar_benthos_test.yaml")}, "_benthos_test"); err == nil {
		t.Error("Expected error")
	}
	if _, err = test.GetTestTargets([]string{"/does/not/exist/foo.yaml"}, "_benthos_test"); err == nil {
		t.Error("Expected error")
	}
}

func TestGetTargetsDir(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo.yaml":                     `foobar: {}`,
		"foo_benthos_test.yaml":        `tests: [{}]`,
		"bar.yaml":                     `tests: [{}]`,
		"not_a_yaml.txt":               `foobar this isnt json or yaml`,
		"nested/baz.yaml":              `foobar: {}`,
		"nested/baz_benthos_test.yaml": `tests: [{}]`,
		"ignored.yaml":                 `foobar: {}`,
		"nested/also_ignored.yaml":     `foobar: {}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	paths, err := test.GetTestTargets([]string{testDir + "/..."}, "_benthos_test")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 3, len(paths); exp != act {
		t.Fatalf("Wrong count of paths: %v != %v", act, exp)
	}
	if _, exists := paths[filepath.Join(testDir, "foo.yaml")]; !exists {
		t.Errorf("Wrong path returned: %v does not contain foo.yaml", paths)
	}
	if _, exists := paths[filepath.Join(testDir, "bar.yaml")]; !exists {
		t.Errorf("Wrong path returned: %v does not contain bar.yaml", paths)
	}
	if _, exists := paths[filepath.Join(testDir, "nested", "baz.yaml")]; !exists {
		t.Errorf("Wrong path returned: %v does not contain nested/baz.yaml", paths)
	}
}

func TestGetTargetsDirError(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo_benthos_test.yaml": `tests: [{}]`,
		"bar.yaml":              `foobar: {}`,
		"bar_benthos_test.yaml": `tests: [{}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = test.GetTestTargets([]string{testDir + "/..."}, "_benthos_test"); err == nil {
		t.Error("Expected error")
	}
}

func TestGetTargetsDirRecurseError(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo.yaml":                     `foobar: {}`,
		"foo_benthos_test.yaml":        `tests: [{}]`,
		"bar.yaml":                     `foobar: {}`,
		"bar_benthos_test.yaml":        `tests: [{}]`,
		"nested/baz_benthos_test.yaml": `tests: [{}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = test.GetTestTargets([]string{testDir + "/..."}, "_benthos_test"); err == nil {
		t.Error("Expected error")
	}
}

func TestCommandRunHappy(t *testing.T) {
	testDir, err := initTestFiles(t, map[string]string{
		"foo.yaml": `
pipeline:
  meow: woof
  processors:
  - bloblang: 'root = content().uppercase()'`,
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
  - bloblang: 'root = content().uppercase()'`,
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

	if !test.RunAll([]string{filepath.Join(testDir, "foo.yaml")}, "_benthos_test", false, log.Noop(), nil) {
		t.Error("Unexpected result")
	}

	if test.RunAll([]string{filepath.Join(testDir, "foo.yaml")}, "_benthos_test", true, log.Noop(), nil) {
		t.Error("Unexpected result")
	}

	if test.RunAll([]string{testDir}, "_benthos_test", true, log.Noop(), nil) {
		t.Error("Unexpected result")
	}
}
