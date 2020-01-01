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
