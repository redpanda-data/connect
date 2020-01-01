package test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefinitionFail(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"config1.yaml": `
pipeline:
  processors:
  - text:
      operator: to_upper`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	def := Definition{
		Parallel: false,
		Cases: []Case{
			{
				Name:             "foo test 1",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []InputPart{
					{
						Content: "foo bar baz",
						Metadata: map[string]string{
							"key1": "value1",
						},
					},
					{
						Content: "one two three",
						Metadata: map[string]string{
							"key1": "value2",
						},
					},
				},
				OutputBatches: [][]ConditionsMap{
					{
						{
							"content_equals": ContentEqualsCondition("FOO BAR baz"),
							"metadata_equals": MetadataEqualsCondition{
								"key1": "value1",
							},
						},
						{
							"content_equals": ContentEqualsCondition("ONE TWO THREE"),
							"metadata_equals": MetadataEqualsCondition{
								"key1": "value3",
							},
						},
					},
				},
				line: 10,
			},
		},
	}

	failures, err := def.Execute(filepath.Join(testDir, "config1.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 0: content_equals: content mismatch, expected 'FOO BAR baz', got 'FOO BAR BAZ'", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 1: metadata_equals: metadata key 'key1' mismatch, expected 'value3', got 'value2'", failures[1].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}

func TestDefinitionParallel(t *testing.T) {
	testDir, err := initTestFiles(map[string]string{
		"config1.yaml": `
pipeline:
  processors:
  - text:
      operator: to_upper`,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	def := Definition{
		Parallel: true,
		Cases: []Case{
			{
				Name:             "foo test 1",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []InputPart{
					{
						Content: "foo bar baz",
						Metadata: map[string]string{
							"key1": "value1",
						},
					},
				},
				OutputBatches: [][]ConditionsMap{
					{
						{
							"content_equals": ContentEqualsCondition("FOO BAR baz"),
							"metadata_equals": MetadataEqualsCondition{
								"key1": "value1",
							},
						},
					},
				},
				line: 10,
			},
			{
				Name:             "foo test 2",
				Environment:      map[string]string{},
				TargetProcessors: "/pipeline/processors",
				InputBatch: []InputPart{
					{
						Content: "one two three",
						Metadata: map[string]string{
							"key1": "value2",
						},
					},
				},
				OutputBatches: [][]ConditionsMap{
					{
						{
							"content_equals": ContentEqualsCondition("ONE TWO THREE"),
							"metadata_equals": MetadataEqualsCondition{
								"key1": "value3",
							},
						},
					},
				},
				line: 20,
			},
		},
	}

	failures, err := def.Execute(filepath.Join(testDir, "config1.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := 2, len(failures); exp != act {
		t.Fatalf("Wrong count of failures: %v != %v", act, exp)
	}
	if exp, act := "foo test 1 [line 10]: batch 0 message 0: content_equals: content mismatch, expected 'FOO BAR baz', got 'FOO BAR BAZ'", failures[0].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "foo test 2 [line 20]: batch 0 message 0: metadata_equals: metadata key 'key1' mismatch, expected 'value3', got 'value2'", failures[1].String(); exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}
