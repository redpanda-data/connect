package tracer_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/tracer"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []tracer.Config{}

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
	if exp, act := tracer.TypeJaeger, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "benthos", conf[0].Jaeger.ServiceName; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Jaeger.AgentAddress; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}
