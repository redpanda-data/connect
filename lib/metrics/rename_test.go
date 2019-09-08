// Copyright (c) 2019 Daniel Rubenstein
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

package metrics

import (
	"testing"
)

func TestRenamePatterns(t *testing.T) {
	type testCase struct {
		pattern string
		value   string
		paths   map[string]string
	}

	testCases := []testCase{
		{
			pattern: "foo",
			value:   "bar",
			paths: map[string]string{
				"zip.foo.baz": "zip.bar.baz",
				"zap.bar.baz": "zap.bar.baz",
			},
		},
		{
			pattern: `foo\.bar`,
			value:   "bar",
			paths: map[string]string{
				"zip.foo.bar.baz": "zip.bar.baz",
				"zip.foo_bar.baz": "zip.foo_bar.baz",
			},
		},
		{
			pattern: `foo\.([a-z]*)\.baz`,
			value:   "zip.$1.zap",
			paths: map[string]string{
				"foo.bar.baz":     "zip.bar.zap",
				"foo.bar.zip.baz": "foo.bar.zip.baz",
			},
		},
		{
			pattern: `^foo`,
			value:   "zip",
			paths: map[string]string{
				"foo.bar.baz": "zip.bar.baz",
				"foo.foo.baz": "zip.foo.baz",
			},
		},
	}

	for _, test := range testCases {
		childConf := NewConfig()
		childConf.Type = TypeHTTPServer
		childConf.HTTP.Prefix = ""

		conf := NewConfig()
		conf.Type = TypeRename
		conf.Rename.Child = &childConf
		conf.Rename.ByRegexp = []RenameByRegexpConfig{
			{
				Pattern: test.pattern,
				Value:   test.value,
				Labels: map[string]string{
					"testlabel": "$0",
				},
			},
		}

		m, err := New(conf)
		if err != nil {
			t.Fatal(err)
		}

		var child *HTTP
		if rename, ok := m.(*Rename); ok {
			child = rename.s.(*HTTP)
		}
		if child == nil {
			t.Fatal("Failed to cast child")
		}

		for k := range test.paths {
			m.GetCounter(k)
			m.GetGauge(k)
			m.GetTimer(k)
		}
		if exp, act := len(test.paths), len(child.local.flatCounters); exp != act {
			t.Errorf("Wrong count of metrics registered: %v != %v", act, exp)
			continue
		}
		if exp, act := len(test.paths), len(child.local.flatTimings); exp != act {
			t.Errorf("Wrong count of metrics registered: %v != %v", act, exp)
			continue
		}
		for _, exp := range test.paths {
			if _, ok := child.local.flatCounters[exp]; !ok {
				t.Errorf("Path '%v' missing from aggregator: %v", exp, child.local.flatCounters)
			}
		}
		for _, exp := range test.paths {
			if _, ok := child.local.flatTimings[exp]; !ok {
				t.Errorf("Path '%v' missing from aggregator: %v", exp, child.local.flatTimings)
			}
		}

		if err := m.Close(); err != nil {
			t.Error(err)
		}
	}
}
