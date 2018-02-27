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

package input

import (
	"encoding/json"
	"testing"
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
						"timeout_ms": 1
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

	conf := NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Error(err)
		return
	}

	inputConfs := conf.Broker.Inputs

	if exp, actual := 2, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
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

	if exp, actual := int64(1), inputConfs[0].HTTPServer.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := int64(5000), inputConfs[1].HTTPServer.TimeoutMS; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestBrokerConfigDitto(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"path": "/1"
					}
				},
				{
					"type": "ditto",
					"http_server": {
						"path": "/2"
					}
				},
				{
					"type": "ditto",
					"http_server": {
						"path": "/3"
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

	inputConfs := conf.Broker.Inputs

	if exp, actual := 3, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[1].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[2].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/1", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/3", inputConfs[2].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestBrokerConfigDittoMulti(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"path": "/1"
					}
				},
				{
					"type": "ditto_2",
					"http_server": {
						"path": "/2"
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

	inputConfs := conf.Broker.Inputs

	if exp, actual := 3, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "http_server", inputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[1].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "address:1", inputConfs[2].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/1", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[1].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "/2", inputConfs[2].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}

func TestBrokerConfigDittoZeroed(t *testing.T) {
	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
				{
					"type": "http_server",
					"http_server": {
						"address": "address:1",
						"path": "/1"
					}
				},
				{
					"type": "ditto_0",
					"http_server": {
						"path": "/2"
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

	inputConfs := conf.Broker.Inputs

	if exp, actual := 1, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "http_server", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "address:1", inputConfs[0].HTTPServer.Address; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	if exp, actual := "/1", inputConfs[0].HTTPServer.Path; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
}
