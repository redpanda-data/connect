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
	"testing"
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
						"timeout_ms": 1
					}
				},
				{
					"type": "http_client",
					"http_client": {
						"url": "address:2",
						"retry_period_ms": 2
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

	if exp, actual := int64(1), outputConfs[0].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(5000), outputConfs[1].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := int64(1000), outputConfs[0].HTTPClient.RetryMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(2), outputConfs[1].HTTPClient.RetryMS; exp != actual {
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
						"timeout_ms": 1
					}
				},
				{
					"type": "ditto",
					"http_client": {
						"timeout_ms": 2
					}
				},
				{
					"type": "ditto",
					"http_client": {
						"timeout_ms": 3
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

	if exp, actual := int64(1), outputConfs[0].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(2), outputConfs[1].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(3), outputConfs[2].HTTPClient.TimeoutMS; exp != actual {
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
						"timeout_ms": 1
					}
				},
				{
					"type": "ditto_2",
					"http_client": {
						"timeout_ms": 2
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

	if exp, actual := int64(1), outputConfs[0].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(2), outputConfs[1].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(2), outputConfs[2].HTTPClient.TimeoutMS; exp != actual {
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
						"timeout_ms": 1
					}
				},
				{
					"type": "ditto_0",
					"http_client": {
						"timeout_ms": 2
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

	if exp, actual := int64(1), outputConfs[0].HTTPClient.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}
