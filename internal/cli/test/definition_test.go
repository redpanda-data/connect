package test_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fatih/color"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
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

	def := test.Definition{
		Cases: []test.Case{
			(test.Case{
				Name:             "foo test 1",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []test.InputPart{
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
				OutputBatches: [][]test.ConditionsMap{
					{
						{
							"content_equals": test.ContentEqualsCondition("FOO BAR baz"),
							"metadata_equals": test.MetadataEqualsCondition{
								"key1": "value1",
							},
						},
						{
							"content_equals": test.ContentEqualsCondition("ONE TWO THREE"),
							"metadata_equals": test.MetadataEqualsCondition{
								"key1": "value3",
							},
						},
					},
				},
			}).AtLine(10),
		},
	}

	failures, err := def.Execute(filepath.Join(testDir, "config1.yaml"), nil, log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 1: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", failures[1].String(); exp != act {
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

	def := test.Definition{
		Cases: []test.Case{
			(test.Case{
				Name:             "foo test 1",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []test.InputPart{
					{
						Content: "foo bar baz",
						Metadata: map[string]any{
							"key1": "value1",
						},
					},
				},
				OutputBatches: [][]test.ConditionsMap{
					{
						{
							"content_equals": test.ContentEqualsCondition("FOO BAR baz"),
							"metadata_equals": test.MetadataEqualsCondition{
								"key1": "value1",
							},
						},
					},
				},
			}).AtLine(10),
			(test.Case{
				Name:             "foo test 2",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []test.InputPart{
					{
						Content: "one two three",
						Metadata: map[string]any{
							"key1": "value2",
						},
					},
				},
				OutputBatches: [][]test.ConditionsMap{
					{
						{
							"content_equals": test.ContentEqualsCondition("ONE TWO THREE"),
							"metadata_equals": test.MetadataEqualsCondition{
								"key1": "value3",
							},
						},
					},
				},
			}).AtLine(20),
		},
	}

	failures, err := def.Execute(filepath.Join(testDir, "config1.yaml"), nil, log.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 2 [line 20]: batch 0 message 0: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", failures[1].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}
