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
			lints: []string{"line 3: field kafka is invalid when the component type is stdin"},
		},
		{
			name: "ignore tests section",
			conf: `input:
  type: stdin
tests:
  this: can just contain anything
  like_this: ["foo","bar"]`,
			lints: nil,
		},
		{
			name: "broker object type",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      kafka: {}`,
			lints: []string{"line 6: field kafka is invalid when the component type is stdin"},
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
			lints: []string{"line 8: field kafka is invalid when the component type is stdin"},
		},
		{
			name: "broker object multiple types",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      kafka: {}
    - type: amqp
      stdin:
        multipart: true
    - type: stdin
      stdin: {}`,
			lints: []string{
				"line 6: field kafka is invalid when the component type is stdin",
				"line 8: field stdin is invalid when the component type is amqp",
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
        multipart: true`,
			lints: []string{
				"line 7: field thisismadeup not recognised",
			},
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
