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
