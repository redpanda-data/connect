package config_test

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/config"
	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

//------------------------------------------------------------------------------

func TestConfigLints(t *testing.T) {
	type testObj struct {
		name  string
		conf  string
		lints []string
	}

	tests := []testObj{
		{
			name:  "empty object",
			conf:  `{}`,
			lints: nil,
		},
		{
			name: "root object type",
			conf: `input:
  type: stdin
  kafka: {}`,
			lints: []string{"line 3: field kafka is invalid when the component type is stdin (input)"},
		},
		{
			name: "lint tests section",
			conf: `input:
  type: stdin
tests:
  this: can just contain anything
  like_this: ["foo","bar"]`,
			lints: []string{"line 4: expected array value"},
		},
		{
			name: "broker object type",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      kafka: {}`,
			lints: []string{"line 6: field kafka is invalid when the component type is stdin (input)"},
		},
		{
			name: "broker object type within resources",
			conf: `resources:
  inputs:
    foo:
      type: broker
      broker:
        inputs:
          - type: stdin
            kafka: {}`,
			lints: []string{"line 8: field kafka is invalid when the component type is stdin (input)"},
		},
		{
			name: "broker object multiple types",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      kafka: {}
    - type: amqp_0_9
      stdin:
        codec: lines
    - type: stdin
      stdin: {}`,
			lints: []string{
				"line 6: field kafka is invalid when the component type is stdin (input)",
				"line 8: field stdin is invalid when the component type is amqp_0_9 (input)",
			},
		},
		{
			name: "broker object made-up field",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      stdin:
        thisismadeup: true
        codec: lines`,
			lints: []string{
				"line 7: field thisismadeup not recognised",
			},
		},
		{
			name: "switch output with reject and implicit retry_until_success",
			conf: `output:
  switch:
    cases:
      - check: errored()
        output:
          reject: ${! error() }
      - output:
          drop: {}
`,
			lints: []string{
				"line 3: a `switch` output with a `reject` case output must have the field `switch.retry_until_success` set to `false` (defaults to `true`), otherwise the `reject` child output will result in infinite retries",
			},
		},
		{
			name: "switch output with reject and explicit retry_until_success",
			conf: `output:
  switch:
    retry_until_success: true
    cases:
      - output:
          drop: {}
      - check: errored()
        output:
          reject: ${! error() }
`,
			lints: []string{
				"line 3: a `switch` output with a `reject` case output must have the field `switch.retry_until_success` set to `false` (defaults to `true`), otherwise the `reject` child output will result in infinite retries",
			},
		},
		{
			name: "switch output with reject and no retry",
			conf: `output:
  switch:
    retry_until_success: false
    cases:
      - output:
          drop: {}
      - check: errored()
        output:
          reject: ${! error() }
`,
			lints: nil,
		},
		{
			name: "switch output without reject and retry",
			conf: `output:
  switch:
    retry_until_success: true
    cases:
      - output:
          drop: {}
      - check: errored()
        output:
          drop: {}
`,
			lints: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			lints, err := config.Lint([]byte(test.conf), config.New())
			if err != nil {
				tt.Fatal(err)
			}
			if exp, act := test.lints, lints; !reflect.DeepEqual(exp, act) {
				tt.Errorf("Wrong lint results: %v != %v", act, exp)
			}
		})
	}
}

//------------------------------------------------------------------------------
