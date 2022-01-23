package input_test

import (
	"encoding/json"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/input"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestBrokerConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"timeout": "1ms"
					}
				},
				{
					"type": "http_server",
					"http_server": {
						"address": "address:2",
						"path": "/2"
					}
				}
			]
		}
	}`)

	var conf input.Config
	check := func() {
		inputConfs := conf.Broker.Inputs

		if exp, actual := 2, len(inputConfs); exp != actual {
			t.Fatalf("unexpected number of input configs: %v != %v", exp, actual)
		}

		if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}
		if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}

		if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}
		if exp, actual := "address:2", inputConfs[1].HTTPServer.Address; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}

		if exp, actual := "/post", inputConfs[0].HTTPServer.Path; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}
		if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}

		if exp, actual := "1ms", inputConfs[0].HTTPServer.Timeout; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}
		if exp, actual := "5s", inputConfs[1].HTTPServer.Timeout; exp != actual {
			t.Errorf("Unexpected value from config: %v != %v", exp, actual)
		}
	}

	conf = input.NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()

	conf = input.NewConfig()
	if err := yaml.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()
}
