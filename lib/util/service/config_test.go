package service

import (
	"os"
	"testing"
)

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
		"foo ${BENTHOS_TEST_FOO:} baz":                       "foo  baz",
	}

	for in, exp := range tests {
		out := replaceEnvVariables([]byte(in))
		if act := string(out); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
