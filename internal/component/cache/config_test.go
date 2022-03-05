package cache_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

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
