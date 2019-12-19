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

package writer

import "testing"

func TestSQSHeaderCheck(t *testing.T) {
	type testCase struct {
		k, v     string
		expected bool
	}

	tests := []testCase{
		{
			k: "foo", v: "bar",
			expected: true,
		},
		{
			k: "foo.bar", v: "bar.baz",
			expected: true,
		},
		{
			k: "foo_bar", v: "bar_baz",
			expected: true,
		},
		{
			k: "foo-bar", v: "bar-baz",
			expected: true,
		},
		{
			k: ".foo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: ".bar",
			expected: true,
		},
		{
			k: "f..oo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba..r",
			expected: true,
		},
		{
			k: "aws.foo", v: "bar",
			expected: false,
		},
		{
			k: "amazon.foo", v: "bar",
			expected: false,
		},
		{
			k: "foo.", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "bar.",
			expected: true,
		},
		{
			k: "fo$o", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba$r",
			expected: true,
		},
		{
			k: "foo_with_10_numbers", v: "bar",
			expected: true,
		},
		{
			k: "foo", v: "bar_with_10_numbers and a space",
			expected: true,
		},
		{
			k: "foo with space", v: "bar",
			expected: false,
		},
		{
			k: "iso date", v: "1997-07-16T19:20:30.45+01:00",
			expected: true,
		},
		{
			k: "has a char in the valid range", v: "#x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF - Ѱ",
			expected: true,
		},
	}

	for i, test := range tests {
		if act, exp := isValidSQSAttribute(test.k, test.v), test.expected; act != exp {
			t.Errorf("Unexpected result for test '%v': %v != %v", i, act, exp)
		}
	}
}
