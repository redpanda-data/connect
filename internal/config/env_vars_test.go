package config

import (
	"os"
	"testing"
)

func TestEnvSwapping(t *testing.T) {
	if os.Getenv("BENTHOS_TEST_FOO") != "" {
		os.Setenv("BENTHOS_TEST_FOO", "")
	}

	os.Setenv("BENTHOS.TEST.FOO", "testfoo")
	os.Setenv("BENTHOS.TEST.BAR", "test\nbar")

	tests := map[string]string{
		"foo ${BENTHOS_TEST_FOO:bar} baz":                    "foo bar baz",
		"foo ${BENTHOS.TEST.FOO:bar} baz":                    "foo testfoo baz",
		"foo ${BENTHOS.TEST.FOO} baz":                        "foo testfoo baz",
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
		"foo ${BENTHOS.TEST.BAR} baz":                                              "foo test\\nbar baz",
	}

	for in, exp := range tests {
		out := ReplaceEnvVariables([]byte(in))
		if act := string(out); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
