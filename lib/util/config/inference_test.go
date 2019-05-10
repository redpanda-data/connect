// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

package config

import (
	"reflect"
	"testing"
)

func TestInference(t *testing.T) {
	type testCase struct {
		input         interface{}
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
		actCandidates := GetInferenceCandidates(test.input)
		if !reflect.DeepEqual(actCandidates, test.expCandidates) {
			t.Errorf("Wrong candidates inferred '%v': %v != %v", i, actCandidates, test.expCandidates)
		}
	}
}
