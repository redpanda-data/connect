package test_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fatih/color"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
	dtest "github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func TestDefinitionFail(t *testing.T) {
	color.NoColor = true

	testDir, err := initTestFiles(t, map[string]string{
		"config1.yaml": `
pipeline:
  processors:
  - bloblang: 'root = content().uppercase()'
`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	def := []dtest.Case{
		{
			Name:             "foo test 1",
			Environment:      map[string]string{},
			TargetProcessors: "/pipeline/processors",
			InputBatches: [][]dtest.InputConfig{
				{
					{
						Content: "foo bar baz",
						Metadata: map[string]any{
							"key1": "value1",
						},
					},
					{
						Content: "one two three",
						Metadata: map[string]any{
							"key1": "value2",
						},
					},
				},
			},
			OutputBatches: [][]dtest.OutputConditionsMap{
				{
					{
						"content_equals": dtest.ContentEqualsCondition("FOO BAR baz"),
						"metadata_equals": dtest.MetadataEqualsCondition{
							"key1": "value1",
						},
					},
					{
						"content_equals": dtest.ContentEqualsCondition("ONE TWO THREE"),
						"metadata_equals": dtest.MetadataEqualsCondition{
							"key1": "value3",
						},
					},
				},
			},
		},
	}

	failures, err := test.Execute(def, filepath.Join(testDir, "config1.yaml"), nil, log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1: batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 1: batch 0 message 1: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", failures[1].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}

func TestDefinitionParallel(t *testing.T) {
	color.NoColor = true

	testDir, err := initTestFiles(t, map[string]string{
		"config1.yaml": `
pipeline:
  processors:
  - bloblang: 'root = content().uppercase()'
`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	def := []dtest.Case{
		{
			Name:             "foo test 1",
			Environment:      map[string]string{},
			TargetProcessors: "/pipeline/processors",
			InputBatches: [][]dtest.InputConfig{
				{
					{
						Content: "foo bar baz",
						Metadata: map[string]any{
							"key1": "value1",
						},
					},
				},
			},
			OutputBatches: [][]dtest.OutputConditionsMap{
				{
					{
						"content_equals": dtest.ContentEqualsCondition("FOO BAR baz"),
						"metadata_equals": dtest.MetadataEqualsCondition{
							"key1": "value1",
						},
					},
				},
			},
		},
		{
			Name:             "foo test 2",
			Environment:      map[string]string{},
			TargetProcessors: "/pipeline/processors",
			InputBatches: [][]dtest.InputConfig{
				{
					{
						Content: "one two three",
						Metadata: map[string]any{
							"key1": "value2",
						},
					},
				},
			},
			OutputBatches: [][]dtest.OutputConditionsMap{
				{
					{
						"content_equals": dtest.ContentEqualsCondition("ONE TWO THREE"),
						"metadata_equals": dtest.MetadataEqualsCondition{
							"key1": "value3",
						},
					},
				},
			},
		},
	}

	failures, err := test.Execute(def, filepath.Join(testDir, "config1.yaml"), nil, log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1: batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 2: batch 0 message 0: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", failures[1].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}
