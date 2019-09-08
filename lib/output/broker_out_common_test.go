// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package output

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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

	conf := NewConfig()
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

func TestOutBrokerConfigDitto(t *testing.T) {
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
					"type": "ditto",
					"http_client": {
						"timeout": "2ms"
					}
				},
				{
					"type": "ditto",
					"http_client": {
						"timeout": "3ms"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	if exp, actual := 3, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_client", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", outputConfs[0].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", outputConfs[1].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", outputConfs[2].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "1ms", outputConfs[0].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "2ms", outputConfs[1].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "3ms", outputConfs[2].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

type exampleConfig struct {
	Foo int    `json:"foo" yaml:"foo"`
	Bar string `json:"bar" yaml:"bar"`
	Baz string `json:"baz" yaml:"baz"`
}

func newExampleConfig() *exampleConfig {
	return &exampleConfig{
		Foo: 1000,
		Bar: "bar",
		Baz: "default",
	}
}

func TestBrokerConfigPluginDitto(t *testing.T) {
	t.Parallel()

	RegisterPlugin(
		"example",
		func() interface{} {
			return newExampleConfig()
		},
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Output, error) {
			return nil, errors.New("err not implemented")
		},
	)

	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"outputs": [
				{
					"type": "example",
					"plugin": {
						"foo": 23
					}
				},
				{
					"type": "ditto",
					"plugin": {
						"bar": "baz"
					}
				},
				{
					"type": "ditto",
					"plugin": {
						"foo": 29
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	if exp, actual := 3, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "example", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "example", outputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "example", outputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	plugOneConf, ok := outputConfs[0].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", outputConfs[0].Plugin)
	}
	if exp, actual := 23, plugOneConf.Foo; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "bar", plugOneConf.Bar; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "default", plugOneConf.Baz; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	plugTwoConf, ok := outputConfs[1].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", outputConfs[1].Plugin)
	}
	if exp, actual := 23, plugTwoConf.Foo; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "baz", plugTwoConf.Bar; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "default", plugTwoConf.Baz; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	plugThreeConf, ok := outputConfs[2].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", outputConfs[2].Plugin)
	}
	if exp, actual := 29, plugThreeConf.Foo; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "baz", plugThreeConf.Bar; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "default", plugThreeConf.Baz; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestOutBrokerConfigDittoMulti(t *testing.T) {
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
					"type": "ditto_2",
					"http_client": {
						"timeout": "2ms"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	if exp, actual := 3, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_client", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_client", outputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", outputConfs[0].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", outputConfs[1].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", outputConfs[2].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "1ms", outputConfs[0].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "2ms", outputConfs[1].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "2ms", outputConfs[2].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestOutBrokerConfigDittoZeroed(t *testing.T) {
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
					"type": "ditto_0",
					"http_client": {
						"timeout": "2ms"
					}
				}
			]
		}
	}`)

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	outputConfs := conf.Broker.Outputs

	if exp, actual := 1, len(outputConfs); exp != actual {
		t.Errorf("unexpected number of output configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_client", outputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", outputConfs[0].HTTPClient.URL; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "1ms", outputConfs[0].HTTPClient.Timeout; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}
