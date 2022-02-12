package cache_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestConstructorBadType(t *testing.T) {
	conf := cache.NewConfig()
	conf.Type = "not_exist"

	if _, err := cache.New(conf, mock.NewManager(), log.Noop(), metrics.Noop()); err == nil {
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
}
