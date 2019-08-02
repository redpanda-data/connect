// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
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

package config

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
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
				"line 6: path 'pipeline.processors[0].type': Type 'batch' is unsafe outside of the 'input' section, for more information read https://docs.benthos.dev/processors/#batch",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			config := New()
			if err := yaml.Unmarshal([]byte(test.conf), &config); err != nil {
				tt.Fatal(err)
			}
			lints, err := Lint([]byte(test.conf), config)
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
