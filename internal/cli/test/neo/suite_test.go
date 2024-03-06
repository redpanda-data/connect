package neo_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/cli/test/neo"
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

	suite := neo.Suite{
		Name: fmt.Sprintf("%s/config1.yaml", testDir),
		Path: fmt.Sprintf("%s/config1.yaml", testDir),
		Cases: map[string]dtest.Case{
			"foo test 1": {
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
		},
	}

	sexec := suite.Run(false, "", log.Noop(), []string{})
	assert.NoError(t, sexec.Err)

	if exp, act := 1, len(sexec.Cases); exp != act {
		t.Fatalf("Wrong count of cases: %v != %v", act, exp)
	}

	assert.NoError(t, sexec.Cases["foo test 1"].Err)
	if exp, act := 2, len(sexec.Cases["foo test 1"].Failures); exp != act {
		t.Fatalf("Wrong count of failures for case 'foo test 1': %v != %v", act, exp)
	}
	if exp, act := "batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", sexec.Cases["foo test 1"].Failures[0]; exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
	if exp, act := "batch 0 message 1: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", sexec.Cases["foo test 1"].Failures[1]; exp != act {
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

	suite := neo.Suite{
		Name: fmt.Sprintf("%s/config1.yaml", testDir),
		Path: fmt.Sprintf("%s/config1.yaml", testDir),
		Cases: map[string]dtest.Case{
			"foo test 1": {
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
			"foo test 2": {
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
		}}

	sexec := suite.Run(false, "", log.Noop(), []string{})
	assert.NoError(t, sexec.Err)

	if exp, act := 2, len(sexec.Cases); exp != act {
		t.Fatalf("Wrong count of cases: %v != %v", act, exp)
	}

	assert.NoError(t, sexec.Cases["foo test 1"].Err)
	if exp, act := 1, len(sexec.Cases["foo test 1"].Failures); exp != act {
		t.Fatalf("Wrong count of failures for case 'foo test 1': %v != %v", act, exp)
	}
	if exp, act := "batch 0 message 0: content_equals: content mismatch\n  expected: FOO BAR baz\n  received: FOO BAR BAZ", sexec.Cases["foo test 1"].Failures[0]; exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}

	assert.NoError(t, sexec.Cases["foo test 2"].Err)
	if exp, act := 1, len(sexec.Cases["foo test 2"].Failures); exp != act {
		t.Fatalf("Wrong count of failures for case 'foo test 2': %v != %v", act, exp)
	}
	if exp, act := "batch 0 message 0: metadata_equals: metadata key 'key1' mismatch\n  expected: value3\n  received: value2", sexec.Cases["foo test 2"].Failures[0]; exp != act {
		t.Errorf("Mismatched fail message: %v != %v", act, exp)
	}
}
