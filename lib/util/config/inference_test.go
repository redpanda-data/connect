package config

import (
	"reflect"
	"testing"
)

func TestInference(t *testing.T) {
	type testCase struct {
		input         interface{}
		ignore        []string
		expCandidates []string
	}

	tests := []testCase{
		{
			input:         nil,
			expCandidates: nil,
		},
		{
			input: map[string]interface{}{
				"type": "foo",
				"bar": map[string]interface{}{
					"baz": "test",
				},
			},
			expCandidates: nil,
		},
		{
			input: map[string]interface{}{
				"bar": map[string]interface{}{
					"baz": "test",
				},
				"ignoreme": "hello",
			},
			ignore:        []string{"ignoreme"},
			expCandidates: []string{"bar"},
		},
		{
			input: map[string]interface{}{
				"bar": map[string]interface{}{
					"baz": "test",
				},
				"qux": "quz",
			},
			expCandidates: []string{"bar", "qux"},
		},
		{
			input: map[string]interface{}{
				"type": 5,
				"bar": map[string]interface{}{
					"baz": "test",
				},
				"qux": "quz",
			},
			expCandidates: nil,
		},
		{
			input: map[interface{}]interface{}{
				"type": "foo",
				"bar": map[interface{}]interface{}{
					"baz": "test",
				},
			},
			expCandidates: nil,
		},
		{
			input: map[interface{}]interface{}{
				"bar": map[interface{}]interface{}{
					"baz": "test",
				},
				"qux": "quz",
				5:     "ignored",
			},
			expCandidates: []string{"bar", "qux"},
		},
		{
			input: map[interface{}]interface{}{
				"type": 5,
				"bar": map[interface{}]interface{}{
					"baz": "test",
				},
				"qux": "quz",
				5:     "ignored",
			},
			expCandidates: nil,
		},
	}

	for i, test := range tests {
		actCandidates := GetInferenceCandidates(test.input, test.ignore...)
		if !reflect.DeepEqual(actCandidates, test.expCandidates) {
			t.Errorf("Wrong candidates inferred '%v': %v != %v", i, actCandidates, test.expCandidates)
		}
	}
}
