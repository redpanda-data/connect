package cache_test

import (
	"testing"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/cache"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
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
