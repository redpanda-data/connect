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
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
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

	var conf Config
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

	conf = NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()

	conf = NewConfig()
	if err := yaml.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()
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

	var conf Config

	check := func() {
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

	conf = NewConfig()
	if err := json.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()

	conf = NewConfig()
	if err := yaml.Unmarshal(testConf, &conf); err != nil {
		t.Fatal(err)
	}
	check()
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
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Input, error) {
			return nil, errors.New("err not implemented")
		},
	)

	testConf := []byte(`{
		"type": "broker",
		"broker": {
			"inputs": [
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

	inputConfs := conf.Broker.Inputs

	if exp, actual := 3, len(inputConfs); exp != actual {
		t.Errorf("unexpected number of input configs: %v != %v", exp, actual)
		return
	}

	if exp, actual := "example", inputConfs[0].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "example", inputConfs[1].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}
	if exp, actual := "example", inputConfs[2].Type; exp != actual {
		t.Errorf("Unexpected value from config: %v != %v", exp, actual)
	}

	plugOneConf, ok := inputConfs[0].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", inputConfs[0].Plugin)
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

	plugTwoConf, ok := inputConfs[1].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", inputConfs[1].Plugin)
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

	plugThreeConf, ok := inputConfs[2].Plugin.(*exampleConfig)
	if !ok {
		t.Fatalf("Wrong config type: %T", inputConfs[2].Plugin)
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
