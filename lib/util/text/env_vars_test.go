// Copyright (c) 2017 Ashley Jeffs
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

package text

import (
	"os"
	"testing"
)

func TestEnvVarDetection(t *testing.T) {
	tests := map[string]bool{
		"foo ${BENTHOS_TEST_FOO:bar} baz":           true,
		"foo ${BENTHOS_TEST_FOO:} baz":              false,
		"foo ${BENTHOS_TEST_FOO} baz":               true,
		"foo ${BENTHOS_TEST_FOO} baz ${and_this}":   true,
		"foo $BENTHOS_TEST_FOO} baz $but_not_this}": false,
		"foo ${BENTHOS_TEST_FOO baz ${or_this":      false,
		"nothing $ here boss {}":                    false,
	}

	for in, exp := range tests {
		act := ContainsEnvVariables([]byte(in))
		if act != exp {
			t.Errorf("Wrong result for '%v': %v != %v", in, act, exp)
		}
	}
}

func TestEnvSwapping(t *testing.T) {
	if len(os.Getenv("BENTHOS_TEST_FOO")) != 0 {
		os.Setenv("BENTHOS_TEST_FOO", "")
	}

	tests := map[string]string{
		"foo ${BENTHOS_TEST_FOO:bar} baz":                    "foo bar baz",
		"foo ${BENTHOS_TEST_FOO:http://bar.com} baz":         "foo http://bar.com baz",
		"foo ${BENTHOS_TEST_FOO:http://bar.com?wat=nuh} baz": "foo http://bar.com?wat=nuh baz",
		"foo ${BENTHOS_TEST_FOO:http://bar.com#wat} baz":     "foo http://bar.com#wat baz",
		"foo ${BENTHOS_TEST_FOO:tcp://*:2020} baz":           "foo tcp://*:2020 baz",
		"foo ${BENTHOS_TEST_FOO:bar} http://bar.com baz":     "foo bar http://bar.com baz",
		"foo ${BENTHOS_TEST_FOO} http://bar.com baz":         "foo  http://bar.com baz",
		"foo ${BENTHOS_TEST_FOO:wat@nuh.com} baz":            "foo wat@nuh.com baz",
		"foo ${} baz":                                        "foo ${} baz",
		"foo ${BENTHOS_TEST_FOO:foo,bar} baz":                "foo foo,bar baz",
		"foo ${BENTHOS_TEST_FOO} baz":                        "foo  baz",
	}

	for in, exp := range tests {
		out := ReplaceEnvVariables([]byte(in))
		if act := string(out); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
