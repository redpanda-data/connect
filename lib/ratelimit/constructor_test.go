package ratelimit

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	yaml "gopkg.in/yaml.v3"
)

func TestConstructorDescription(t *testing.T) {
	if len(Descriptions()) == 0 {
		t.Error("package descriptions were empty")
	}
}

func TestConstructorBadType(t *testing.T) {
	conf := NewConfig()
	conf.Type = "not_exist"

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error, received nil for invalid type")
	}
}

func TestConstructorConfigDefaultsYAML(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "local",
			"local": {
				"count": 50
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := "1s", conf[0].Local.Interval; exp != act {
		t.Errorf("Wrong default interval: %v != %v", act, exp)
	}
	if exp, act := 50, conf[0].Local.Count; exp != act {
		t.Errorf("Wrong overridden count: %v != %v", act, exp)
	}
}

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"local": {
				"count": 50
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := TypeLocal, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "1s", conf[0].Local.Interval; exp != act {
		t.Errorf("Wrong default interval: %v != %v", act, exp)
	}
	if exp, act := 50, conf[0].Local.Count; exp != act {
		t.Errorf("Wrong overridden count: %v != %v", act, exp)
	}
}
