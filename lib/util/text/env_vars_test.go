package text

import (
	"os"
	"testing"
)

func TestEnvVarDetection(t *testing.T) {
	tests := map[string]bool{
		"foo ${BENTHOS__TEST__FOO:bar} baz":             true,
		"foo ${BENTHOS__TEST__FOO:} baz":                false,
		"foo ${BENTHOS__TEST__FOO} baz":                 true,
		"foo ${BENTHOS__TEST__FOO} baz ${and__this}":    true,
		"foo $BENTHOS__TEST__FOO} baz $but__not__this}": false,
		"foo ${BENTHOS__TEST__FOO baz ${or__this":       false,
		"nothing $ here boss {}":                        false,
		"foo ${BENTHOS__TEST__FOO:barthisdoesntend":     false,
		"foo ${{BENTHOS__TEST__FOO:bar}} baz":           true,
		"foo ${{BENTHOS__TEST__FOO:bar} baz":            false,
		"foo ${foo.bar} baz":                            true,
		"foo ${foo.bar^} baz":                           false,
		"foo ${foo.bar:} baz":                           false,
		"foo ${foo.bar:bar} baz":                        true,
		"foo ${foo*bar} baz":                            false,
		"foo ${foo[0].bar} baz":                         true,
		"foo ${foo[0]} baz":                             true,
		"foo ${foo[0]bar} baz":                          true,
		"foo ${foo.zab\\.rab}":                          true,
	}

	for in, exp := range tests {
		act := ContainsEnvVariables([]byte(in))
		if act != exp {
			t.Errorf("Wrong result for '%v': %v != %v", in, act, exp)
		}
	}
}

func TestEnvSwapping(t *testing.T) {
	if os.Getenv("BENTHOS__TEST__FOO") != "" {
		os.Setenv("BENTHOS__TEST__FOO", "")
	}

	os.Setenv("BENTHOS_TEST_FOO", "testfoo")
	os.Setenv("BENTHOS_TEST_BAR", "test\nbar")
	os.Setenv("BENTHOS_TEST_JSON", "{\"foo\": [{\"bar\": \"baz\"}, {\"zab.rab\": \"yay\"}]}")

	tests := map[string]string{
		"foo ${BENTHOS__TEST__FOO:bar} baz":                    "foo bar baz",
		"foo ${BENTHOS_TEST_FOO:bar} baz":                      "foo testfoo baz",
		"foo ${BENTHOS_TEST_FOO} baz":                          "foo testfoo baz",
		"foo ${BENTHOS__TEST__FOO:http://bar_com} baz":         "foo http://bar_com baz",
		"foo ${BENTHOS__TEST__FOO:http://bar_com?wat=nuh} baz": "foo http://bar_com?wat=nuh baz",
		"foo ${BENTHOS__TEST__FOO:http://bar_com#wat} baz":     "foo http://bar_com#wat baz",
		"foo ${BENTHOS__TEST__FOO:tcp://*:2020} baz":           "foo tcp://*:2020 baz",
		"foo ${BENTHOS__TEST__FOO:bar} http://bar_com baz":     "foo bar http://bar_com baz",
		"foo ${BENTHOS__TEST__FOO} http://bar_com baz":         "foo  http://bar_com baz",
		"foo ${BENTHOS__TEST__FOO:wat@nuh_com} baz":            "foo wat@nuh_com baz",
		"foo ${} baz":                                                                  "foo ${} baz",
		"foo ${BENTHOS__TEST__FOO:foo,bar} baz":                                        "foo foo,bar baz",
		"foo ${BENTHOS__TEST__FOO} baz":                                                "foo  baz",
		"foo ${BENTHOS__TEST__FOO:${!metadata:foo}} baz":                               "foo ${!metadata:foo} baz",
		"foo ${BENTHOS__TEST__FOO:${!metadata:foo}${!metadata:bar}} baz":               "foo ${!metadata:foo}${!metadata:bar} baz",
		"foo ${BENTHOS__TEST__FOO:${!count:foo}-${!timestamp__unix__nano}_tar_gz} baz": "foo ${!count:foo}-${!timestamp__unix__nano}_tar_gz baz",
		"foo ${{BENTHOS__TEST__FOO:bar}} baz":                                          "foo ${BENTHOS__TEST__FOO:bar} baz",
		"foo ${{BENTHOS__TEST__FOO}} baz":                                              "foo ${BENTHOS__TEST__FOO} baz",
		"foo ${BENTHOS_TEST_BAR} baz":                                                  "foo test\\nbar baz",
		"foo ${BENTHOS_TEST_JSON} baz":                                                 "foo {\"foo\": [{\"bar\": \"baz\"}, {\"zab.rab\": \"yay\"}]} baz",
		"foo ${BENTHOS_TEST_JSON.foo} baz":                                             "foo [{\"bar\": \"baz\"}, {\"zab.rab\": \"yay\"}] baz",
		"foo ${BENTHOS_TEST_JSON.foo.0.bar} baz":                                       "foo baz baz",
		"foo ${BENTHOS_TEST_JSON.foo.1.zab\\.rab} baz":                                 "foo yay baz",
		"foo ${BENTHOS_TEST_JSON.foo.2.bar:nope} baz":                                  "foo nope baz",
	}

	for in, exp := range tests {
		out := ReplaceEnvVariables([]byte(in))
		if act := string(out); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
