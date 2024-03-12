package test_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func TestNeoRunnerHappy(t *testing.T) {
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

	runner, err := test.GetRunner(test.NeoRunner)
	assert.NoError(t, err)

	if !runner.Run(test.RunConfig{[]string{filepath.Join(testDir, "foo.yaml")}, "_benthos_test", false, log.Noop(), nil, "default"}) {
		t.Error("Unexpected result")
	}

	if runner.Run(test.RunConfig{[]string{filepath.Join(testDir, "foo.yaml")}, "_benthos_test", true, log.Noop(), nil, "default"}) {
		t.Error("Unexpected result")
	}

	if runner.Run(test.RunConfig{[]string{testDir}, "_benthos_test", true, log.Noop(), nil, "default"}) {
		t.Error("Unexpected result")
	}
}
