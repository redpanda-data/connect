package tracer_test

import (
	"testing"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/tracer"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
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
	if exp, act := "jaeger", conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Jaeger.AgentAddress; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}
