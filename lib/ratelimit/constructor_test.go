package ratelimit_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestConstructorBadType(t *testing.T) {
	conf := ratelimit.NewConfig()
	conf.Type = "not_exist"

	if _, err := ratelimit.New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error, received nil for invalid type")
	}
}

func TestConstructorConfigDefaultsYAML(t *testing.T) {
	conf := []ratelimit.Config{}

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
	conf := []ratelimit.Config{}

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
	if exp, act := ratelimit.TypeLocal, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "1s", conf[0].Local.Interval; exp != act {
		t.Errorf("Wrong default interval: %v != %v", act, exp)
	}
	if exp, act := 50, conf[0].Local.Count; exp != act {
		t.Errorf("Wrong overridden count: %v != %v", act, exp)
	}
}
