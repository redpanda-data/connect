package cache_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestConstructorBadType(t *testing.T) {
	conf := cache.NewConfig()
	conf.Type = "not_exist"

	if _, err := cache.New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error, received nil for invalid type")
	}
}

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []cache.Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"aws_dynamodb": {
				"value": "foo"
			},
			"file": {
				"query": "foo"
			}
		}
	]`), &conf); err == nil {
		t.Error("Expected error from multi candidates")
	}

	if err := yaml.Unmarshal([]byte(`[
		{
			"memcached": {
				"prefix": "foo"
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := cache.TypeMemcached, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "500ms", conf[0].Memcached.RetryPeriod; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Memcached.Prefix; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}

func TestConstructorConfigDefaultsYAML(t *testing.T) {
	conf := []cache.Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"type": "memory",
			"memory": {
				"ttl": 16
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := "60s", conf[0].Memory.CompactionInterval; exp != act {
		t.Errorf("Wrong default compaction interval: %v != %v", act, exp)
	}
	if exp, act := 16, conf[0].Memory.TTL; exp != act {
		t.Errorf("Wrong overridden ttl: %v != %v", act, exp)
	}
}
