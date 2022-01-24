package output_test

import (
	"encoding/json"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/output"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestOutBrokerConfigDefaults(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"outputs": [
				{
					"type": "http_client",
					"http_client": {
						"url": "address:1",
						"timeout": "1ms"
					}
				},
				{
					"type": "http_client",
					"http_client": {
						"url": "address:2",
						"retry_period": "2ms"
					}
				}
			]
		}
	}`)

	conf := output.NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	if exp, actual := 2, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_client", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", outputConfs[0].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:2", outputConfs[1].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "1ms", outputConfs[0].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "5s", outputConfs[1].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "1s", outputConfs[0].HTTPClient.Retry; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "2ms", outputConfs[1].HTTPClient.Retry; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}
