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
		"foo ${BENTHOS_TEST_FOO:barthisdoesntend":   false,
		"foo ${{BENTHOS_TEST_FOO:bar}} baz":         true,
		"foo ${{BENTHOS_TEST_FOO:bar} baz":          false,
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
		"foo ${} baz":                                                              "foo ${} baz",
		"foo ${BENTHOS_TEST_FOO:foo,bar} baz":                                      "foo foo,bar baz",
		"foo ${BENTHOS_TEST_FOO} baz":                                              "foo  baz",
		"foo ${BENTHOS_TEST_FOO:${!metadata:foo}} baz":                             "foo ${!metadata:foo} baz",
		"foo ${BENTHOS_TEST_FOO:${!metadata:foo}${!metadata:bar}} baz":             "foo ${!metadata:foo}${!metadata:bar} baz",
		"foo ${BENTHOS_TEST_FOO:${!count:foo}-${!timestamp_unix_nano}.tar.gz} baz": "foo ${!count:foo}-${!timestamp_unix_nano}.tar.gz baz",
		"foo ${{BENTHOS_TEST_FOO:bar}} baz":                                        "foo ${BENTHOS_TEST_FOO:bar} baz",
		"foo ${{BENTHOS_TEST_FOO}} baz":                                            "foo ${BENTHOS_TEST_FOO} baz",
	}

	for in, exp := range tests {
		out := ReplaceEnvVariables([]byte(in))
		if act := string(out); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
