package config_test

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"

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
			lints: []string{},
		},
		{
			name: "root object type",
			conf: `input:
  type: stdin
  kafka: {}`,
			lints: []string{"line 3: path 'input': Key 'kafka' found but is ignored"},
		},
		{
			name: "ignore tests section",
			conf: `input:
  type: stdin
tests:
  this: can just contain anything
  like_this: ["foo","bar"]`,
			lints: []string{},
		},
		{
			name: "broker object type",
			conf: `input:
  type: broker
  broker:
    inputs:
    - type: stdin
      kafka: {}`,
			lints: []string{"line 6: path 'input.broker.inputs[0]': Key 'kafka' found but is ignored"},
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
				"line 6: path 'input.broker.inputs[0]': Key 'kafka' found but is ignored",
				"line 9: path 'input.broker.inputs[1]': Key 'stdin' found but is ignored",
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
				"line 7: path 'input.broker.inputs[0].stdin': Key 'thisismadeup' found but is ignored",
			},
		},
		{
			name: "batch processor outside of input",
			conf: `input:
  type: amqp
  amqp: {}
pipeline:
  processors:
  - type: batch
    batch: {}`,
			lints: []string{
				"line 6: path 'pipeline.processors[0].type': Type 'batch' is unsafe outside of the 'input' section, for more information read https://benthos.dev/docs/configuration/batching",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			conf := config.New()
			if err := yaml.Unmarshal([]byte(test.conf), &conf); err != nil {
				tt.Fatal(err)
			}
			lints, err := config.Lint([]byte(test.conf), conf)
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
