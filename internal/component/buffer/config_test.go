package buffer_test

import (
	"testing"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/buffer"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []buffer.Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"memory": {
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
			"memory": {
				"limit": 10
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
}
