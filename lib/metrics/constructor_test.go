package metrics

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

func TestSanitise(t *testing.T) {
	exp := config.Sanitised{
		"type": "http_server",
		"http_server": map[string]interface{}{
			"prefix":       "benthos",
			"path_mapping": "",
		},
	}

	conf := NewConfig()
	conf.Type = "http_server"

	act, err := SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}

	exp = config.Sanitised{
		"type": "statsd",
		"statsd": map[string]interface{}{
			"address":      "foo",
			"prefix":       "benthos",
			"path_mapping": "",
			"flush_period": "100ms",
			"network":      "udp",
			"tag_format":   "legacy",
		},
	}

	conf = NewConfig()
	conf.Type = "statsd"
	conf.Statsd.Address = "foo"

	act, err = SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}
}

func TestConstructorConfigYAMLInference(t *testing.T) {
	conf := []Config{}

	if err := yaml.Unmarshal([]byte(`[
		{
			"http_server": {
				"value": "foo"
			},
			"prometheus": {
				"query": "foo"
			}
		}
	]`), &conf); err == nil {
		t.Error("Expected error from multi candidates")
	}

	if err := yaml.Unmarshal([]byte(`[
		{
			"prometheus": {
				"push_interval": "foo"
			}
		}
	]`), &conf); err != nil {
		t.Error(err)
	}

	if exp, act := 1, len(conf); exp != act {
		t.Errorf("Wrong number of config parts: %v != %v", act, exp)
		return
	}
	if exp, act := TypePrometheus, conf[0].Type; exp != act {
		t.Errorf("Wrong inferred type: %v != %v", act, exp)
	}
	if exp, act := "benthos_push", conf[0].Prometheus.PushJobName; exp != act {
		t.Errorf("Wrong default operator: %v != %v", act, exp)
	}
	if exp, act := "foo", conf[0].Prometheus.PushInterval; exp != act {
		t.Errorf("Wrong value: %v != %v", act, exp)
	}
}
