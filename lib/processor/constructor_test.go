package processor_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestConstructorDescription(t *testing.T) {
	if len(processor.Descriptions()) == 0 {
		t.Error("package descriptions were empty")
	}
}

func TestConstructorBadType(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "not_exist"

	if _, err := processor.New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error, received nil for invalid type")
	}
}

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []processor.Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"text": {
				"value": "foo"
			},
			"jmespath": {
				"query": "foo"
			}
		}
	]`), &conf); err == nil {
		t.Error("Expected error from multi candidates")
	}

	if err := yaml.Unmarshal([]byte(`[
		{
			"text": {
				"value": "foo"
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := processor.TypeText, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "trim_space", conf[0].Text.Operator; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Text.Value; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}

func TestConstructorConfigDefaultsYAML(t *testing.T) {
	conf := []processor.Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "bounds_check",
			"bounds_check": {
				"max_part_size": 50
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := 100, conf[0].BoundsCheck.MaxParts; exp != act {
		t.Errorf("Wrong default parts: %v != %v", act, exp)
	}
	if exp, act := 50, conf[0].BoundsCheck.MaxPartSize; exp != act {
		t.Errorf("Wrong overridden part size: %v != %v", act, exp)
	}
}
