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
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	yaml "gopkg.in/yaml.v3"
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
	conds := ConditionsMap{
		"content_equals": ContentEqualsCondition("foo bar"),
		"metadata_equals": &MetadataEqualsCondition{
			"foo": "bar",
		},
	}

	part := message.NewPart([]byte("foo bar"))
	part.Metadata().Set("foo", "bar")
	errs := conds.CheckAll(part)
	if errs != nil {
		t.Errorf("Unexpected errors: %v", errs)
	}

	part = message.NewPart([]byte("nope"))
	errs = conds.CheckAll(part)
	if exp, act := 2, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "content_equals: content mismatch, expected 'foo bar', got 'nope'", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}
	if exp, act := "metadata_equals: metadata key 'foo' mismatch, expected 'bar', got ''", errs[1].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}

	part = message.NewPart([]byte("foo bar"))
	part.Metadata().Set("foo", "wrong")
	errs = conds.CheckAll(part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "metadata_equals: metadata key 'foo' mismatch, expected 'bar', got 'wrong'", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}

	part = message.NewPart([]byte("wrong"))
	part.Metadata().Set("foo", "bar")
	errs = conds.CheckAll(part)
	if exp, act := 1, len(errs); exp != act {
		t.Fatalf("Wrong count of errors: %v != %v", act, exp)
	}
	if exp, act := "content_equals: content mismatch, expected 'foo bar', got 'wrong'", errs[0].Error(); exp != act {
		t.Errorf("Wrong error: %v != %v", act, exp)
	}
}

func TestContentCondition(t *testing.T) {
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
			expected: errors.New("content mismatch, expected 'foo bar', got 'foo'"),
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
				tt.Errorf("Wrong result, expected %v, received %v", act, exp)
			}
		})
	}
}

func TestContentMatchesCondition(t *testing.T) {

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
			expected: fmt.Errorf("content mismatch, expected '%s', got 'foo'", matchPattern),
		},
		{
			name:     "negative 2",
			input:    "foo & bar",
			expected: fmt.Errorf("content mismatch, expected '%s', got 'foo & bar'", matchPattern),
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
				tt.Errorf("Wrong result, expected %v, received %v", act, exp)
			}
		})
	}
}

func TestMetadataEqualsCondition(t *testing.T) {
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
			expected: errors.New("metadata key 'foo' mismatch, expected 'bar', got ''"),
		},
		{
			name: "negative 2",
			input: map[string]string{
				"foo": "not bar",
			},
			expected: errors.New("metadata key 'foo' mismatch, expected 'bar', got 'not bar'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			part := message.NewPart(nil)
			for k, v := range test.input {
				part.Metadata().Set(k, v)
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
				tt.Errorf("Wrong result, expected %v, received %v", act, exp)
			}
		})
	}
}
