package test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/fatih/color"
	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestConditionUnmarshal(t *testing.T) {
	conf := `
tests:
  content_equals: "foo bar"
  metadata_equals:
    foo: bar`

	tests := struct {
		Tests ConditionsMap
	}{
		Tests: ConditionsMap{},
	}

	if err := yaml.Unmarshal([]byte(conf), &tests); err != nil {
		t.Fatal(err)
	}

	exp := ConditionsMap{
		"content_equals": ContentEqualsCondition("foo bar"),
		"metadata_equals": MetadataEqualsCondition{
			"foo": "bar",
		},
	}

	if act := tests.Tests; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong conditions map: %s != %s", act, exp)
	}
}

func TestBloblangConditionHappy(t *testing.T) {
	conf := `
tests:
  bloblang: 'content() == "foo bar"'`

	tests := struct {
		Tests ConditionsMap
	}{
		Tests: ConditionsMap{},
	}

	require.NoError(t, yaml.Unmarshal([]byte(conf), &tests))

	assert.Empty(t, tests.Tests.CheckAll("", message.NewPart([]byte("foo bar"))))
	assert.NotEmpty(t, tests.Tests.CheckAll("", message.NewPart([]byte("bar baz"))))
}

func TestBloblangConditionSad(t *testing.T) {
	conf := `
tests:
  bloblang: 'content() =='`

	tests := struct {
		Tests ConditionsMap
	}{
		Tests: ConditionsMap{},
	}

	require.EqualError(t, yaml.Unmarshal([]byte(conf), &tests), "line 3: expected query, but reached end of input")
}

func TestConditionUnmarshalUnknownCond(t *testing.T) {
	conf := `
tests:
  this_doesnt_exist: "foo bar"
  metadata_equals:
    key: foo
    value: bar`

	tests := struct {
		Tests ConditionsMap
	}{
		Tests: ConditionsMap{},
	}

	err := yaml.Unmarshal([]byte(conf), &tests)
	if err == nil {
		t.Fatal("Expected error")
	}

	if exp, act := "line 3: message part condition type not recognised: this_doesnt_exist", err.Error(); exp != act {
		t.Errorf("Unexpected error message: %v != %v", act, exp)
	}
}

func TestConditionCheckAll(t *testing.T) {
	color.NoColor = true

	conds := ConditionsMap{
		"content_equals": ContentEqualsCondition("foo bar"),
		"metadata_equals": &MetadataEqualsCondition{
			"foo": "bar",
		},
	}

	part := message.NewPart([]byte("foo bar"))
	part.MetaSetMut("foo", "bar")
	errs := conds.CheckAll("", part)
	require.Len(t, errs, 0)

	part = message.NewPart([]byte("nope"))
	errs = conds.CheckAll("", part)
	require.Len(t, errs, 2)
	assert.Contains(t, "content_equals: content mismatch\n  expected: foo bar\n  received: nope", errs[0].Error())
	assert.Contains(t, "metadata_equals: metadata key 'foo' expected but not found", errs[1].Error())

	part = message.NewPart([]byte("foo bar"))
	part.MetaSetMut("foo", "wrong")
	errs = conds.CheckAll("", part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "metadata_equals: metadata key 'foo' mismatch\n  expected: bar\n  received: wrong", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}

	part = message.NewPart([]byte("wrong"))
	part.MetaSetMut("foo", "bar")
	errs = conds.CheckAll("", part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "content_equals: content mismatch\n  expected: foo bar\n  received: wrong", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}
}

func TestContentCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentEqualsCondition("foo bar")

	type testCase struct {
		name     string
		input    string
		expected error
	}

	tests := []testCase{
		{
			name:     "positive 1",
			input:    "foo bar",
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    "foo",
			expected: errors.New("content mismatch\n  expected: foo bar\n  received: foo"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(message.NewPart([]byte(test.input)))
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestContentMatchesCondition(t *testing.T) {
	color.NoColor = true

	matchPattern := "^foo [a-z]+ bar$"
	cond := ContentMatchesCondition(matchPattern)

	type testCase struct {
		name     string
		input    string
		expected error
	}

	tests := []testCase{
		{
			name:     "positive 1",
			input:    "foo and bar",
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    "foo",
			expected: fmt.Errorf("pattern mismatch\n   pattern: %s\n  received: foo", matchPattern),
		},
		{
			name:     "negative 2",
			input:    "foo & bar",
			expected: fmt.Errorf("pattern mismatch\n   pattern: %s\n  received: foo & bar", matchPattern),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(message.NewPart([]byte(test.input)))
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestMetadataEqualsCondition(t *testing.T) {
	color.NoColor = true

	cond := MetadataEqualsCondition{
		"foo": "bar",
	}

	type testCase struct {
		name     string
		input    map[string]string
		expected error
	}

	tests := []testCase{
		{
			name: "positive 1",
			input: map[string]string{
				"foo": "bar",
			},
			expected: nil,
		},
		{
			name:     "negative 1",
			input:    map[string]string{},
			expected: errors.New("metadata key 'foo' expected but not found"),
		},
		{
			name: "negative 2",
			input: map[string]string{
				"foo": "not bar",
			},
			expected: errors.New("metadata key 'foo' mismatch\n  expected: bar\n  received: not bar"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			part := message.NewPart(nil)
			for k, v := range test.input {
				part.MetaSetMut(k, v)
			}
			actErr := cond.Check(part)
			if test.expected == nil && actErr == nil {
				return
			}
			if test.expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if test.expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", test.expected, actErr)
				return
			}
			if exp, act := test.expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestJSONEqualsCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentJSONEqualsCondition(`{"foo":"bar","bim":"bam"}`)

	type testCase struct {
		name  string
		input string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			input: `{"foo":"bar","bim":"bam"}`,
		},
		{
			name:  "positive 2",
			input: `{ "bim": "bam", "foo": "bar" }`,
		},
		{
			name:  "negative 1",
			input: "foo",
		},
		{
			name:  "negative 2",
			input: `{"foo":"bar"}`,
		},
	}

	jdopts := jsondiff.DefaultConsoleOptions()
	for _, test := range tests {
		var expected error
		diff, explanation := jsondiff.Compare([]byte(test.input), []byte(cond), &jdopts)
		if diff != jsondiff.FullMatch {
			expected = fmt.Errorf("JSON content mismatch\n%v", explanation)
		}

		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(message.NewPart([]byte(test.input)))
			if expected == nil && actErr == nil {
				return
			}
			if expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if exp, act := expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestJSONContainsCondition(t *testing.T) {
	color.NoColor = true

	cond := ContentJSONContainsCondition(`{"foo":"bar","bim":"bam"}`)

	type testCase struct {
		name  string
		input string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			input: `{"foo":"bar","bim":"bam"}`,
		},
		{
			name:  "positive 2",
			input: `{ "bim": "bam", "foo": "bar", "baz": [1, 2, 3] }`,
		},
		{
			name:  "negative 1",
			input: `{"foo":"baz","bim":"bam"}`,
		},
		{
			name:  "negative 2",
			input: `{"foo":"bar"}`,
		},
	}

	jdopts := jsondiff.DefaultConsoleOptions()
	for _, test := range tests {
		var expected error
		diff, explanation := jsondiff.Compare([]byte(test.input), []byte(cond), &jdopts)
		if diff != jsondiff.FullMatch && diff != jsondiff.SupersetMatch {
			expected = fmt.Errorf("JSON superset mismatch\n%v", explanation)
		}

		t.Run(test.name, func(tt *testing.T) {
			actErr := cond.Check(message.NewPart([]byte(test.input)))
			if expected == nil && actErr == nil {
				return
			}
			if expected == nil && actErr != nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if expected != nil && actErr == nil {
				tt.Errorf("Wrong result, expected %v, received %v", expected, actErr)
				return
			}
			if exp, act := expected.Error(), actErr.Error(); exp != act {
				tt.Errorf("Wrong result, expected %v, received %v", exp, act)
			}
		})
	}
}

func TestFileEqualsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/uppercased.txt`,
			input: `FOO BAR BAZ`,
		},
		{
			name:  "positive 2",
			path:  `./not_uppercased.txt`,
			input: `foo bar baz`,
		},
		{
			name:        "negative 1",
			path:        `./inner/uppercased.txt`,
			input:       `foo bar baz`,
			errContains: "content mismatch",
		},
		{
			name:        "negative 2",
			path:        `./not_uppercased.txt`,
			input:       `FOO BAR BAZ`,
			errContains: "content mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			actErr := FileEqualsCondition(test.path).checkFrom(tmpDir, message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}

func TestFileJSONEqualsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	// Contents of both files are unordered.
	unformattedPath := filepath.Join(tmpDir, "inner", "unformatted.json")
	formattedPath := filepath.Join(tmpDir, "formatted.json")

	require.NoError(t, os.MkdirAll(filepath.Dir(unformattedPath), 0o755))
	require.NoError(t, os.WriteFile(unformattedPath, []byte(`{"id":123456,"name":"Benthos"}`), 0o644))
	require.NoError(t, os.WriteFile(formattedPath, []byte(
		`{
    "id": 123456,
    "name": "Benthos"
}`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Benthos","id":123456}`,
		},
		{
			name:  "positive 2",
			path:  `./formatted.json`,
			input: `{"name":"Benthos","id":123456}`,
		},
		{
			name:        "negative 1",
			path:        `./inner/unformatted.json`,
			input:       `{"name":"Benthos"}`,
			errContains: "content mismatch",
		},
		{
			name:        "negative 2",
			path:        `./formatted.json`,
			input:       `{"name":"Benthos"}`,
			errContains: "content mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			testPath := filepath.Join(tmpDir, test.path)
			actErr := FileJSONEqualsCondition(testPath).Check(message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}

func TestFileJSONContainsCondition(t *testing.T) {
	color.NoColor = true

	tmpDir := t.TempDir()

	// Contents of both files are unordered.
	unformattedPath := filepath.Join(tmpDir, "inner", "unformatted.json")
	formattedPath := filepath.Join(tmpDir, "formatted.json")

	require.NoError(t, os.MkdirAll(filepath.Dir(unformattedPath), 0o755))
	require.NoError(t, os.WriteFile(unformattedPath, []byte(`{"id":123456,"name":"Benthos"}`), 0o644))
	require.NoError(t, os.WriteFile(formattedPath, []byte(
		`{
    "id": 123456,
    "name": "Benthos"
}`), 0o644))

	type testCase struct {
		name        string
		path        string
		input       string
		errContains string
	}

	tests := []testCase{
		{
			name:  "positive 1",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Benthos","id":123456}`,
		},
		{
			name:  "positive 2",
			path:  `./formatted.json`,
			input: `{"name":"Benthos","id":123456}`,
		},
		{
			name:  "positive 3",
			path:  `./inner/unformatted.json`,
			input: `{"name":"Benthos","id":123456,"file":"test"}`,
		},
		{
			name:        "negative 1",
			path:        `./inner/unformatted.json`,
			input:       `{"name":"Benthos", "file":"test"}`,
			errContains: "JSON superset mismatch",
		},
		{
			name:        "negative 2",
			path:        `./formatted.json`,
			input:       `{"file":"test"}`,
			errContains: "JSON superset mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			testPath := filepath.Join(tmpDir, test.path)
			actErr := FileJSONContainsCondition(testPath).Check(message.NewPart([]byte(test.input)))
			if test.errContains == "" {
				assert.NoError(t, actErr)
			} else {
				assert.Contains(t, actErr.Error(), test.errContains)
			}
		})
	}
}
