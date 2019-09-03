// Copyright (c) 2019 Ashley Jeffs
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

package tracer

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

func TestSanitise(t *testing.T) {
	exp := config.Sanitised{
		"type": "none",
		"none": map[string]interface{}{},
	}

	conf := NewConfig()
	conf.Type = TypeNone

	act, err := SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}
}

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"jaeger": {
				"value": "foo"
			},
			"none": {
				"query": "foo"
			}
		}
	]`), &conf); err == nil {
		t.Error("Expected error from multi candidates")
	}

	if err := yaml.Unmarshal([]byte(`[
		{
			"jaeger": {
				"agent_address": "foo"
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := TypeJaeger, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "benthos", conf[0].Jaeger.ServiceName; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Jaeger.AgentAddress; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}
