// Copyright (c) 2019 Ashley Jeffs
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

package manager

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

type mockPluginConf struct {
	Foo string `json:"foo" yaml:"foo"`
	Bar string `json:"bar" yaml:"bar"`
	Baz int    `json:"baz" yaml:"baz"`
}

func newMockPluginConf() interface{} {
	return &mockPluginConf{
		Foo: "default",
		Bar: "change this",
		Baz: 10,
	}
}

func TestYAMLPlugin(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", newMockPluginConf,
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (interface{}, error) {
			mConf, ok := conf.(*mockPluginConf)
			if !ok {
				t.Fatalf("failed to cast config: %T", conf)
			}
			if exp, act := "default", mConf.Foo; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			if exp, act := "custom", mConf.Bar; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			if exp, act := 10, mConf.Baz; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			return nil, errTest
		})

	confStr := `plugins:
  foobar:
    type: foo
    plugin:
      bar: custom`

	conf := NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	exp := "failed to create plugin resource 'foobar' of type 'foo': test err"
	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if act := err.Error(); act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}
}

func TestPluginDescriptions(t *testing.T) {
	RegisterPlugin("foo", newMockPluginConf, nil)
	RegisterPlugin("bar", newMockPluginConf, nil)
	DocumentPlugin("bar", "This is a bar plugin.", func(conf interface{}) interface{} {
		mConf, ok := conf.(*mockPluginConf)
		if !ok {
			t.Fatalf("failed to cast config: %T", conf)
		}
		return map[string]interface{}{
			"foo": mConf.Foo,
			"bar": mConf.Bar,
		}
	})

	exp := `Resource Plugins
================

This document has been generated, do not edit it directly.

This document lists any resource plugins that this flavour of Benthos offers.

### Contents

1. [` + "`bar`" + `](#bar)
2. [` + "`foo`" + `](#foo)

## ` + "`bar`" + `

` + "``` yaml" + `
caches: {}
conditions: {}
plugins:
  example:
    type: bar
    plugin:
      bar: change this
      foo: default
processors: {}
rate_limits: {}
` + "```" + `

This is a bar plugin.

## ` + "`foo`" + `

` + "``` yaml" + `
caches: {}
conditions: {}
plugins:
  example:
    type: foo
    plugin:
      foo: default
      bar: change this
      baz: 10
processors: {}
rate_limits: {}
` + "```" + `
`

	act := PluginDescriptions()
	if exp != act {
		t.Logf("Expected:\n%v\n", exp)
		t.Logf("Actual:\n%v\n", act)
		t.Error("Wrong descriptions")
	}
}

func TestJSONPlugin(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", newMockPluginConf,
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (interface{}, error) {
			mConf, ok := conf.(*mockPluginConf)
			if !ok {
				t.Fatalf("failed to cast config: %T", conf)
			}
			if exp, act := "default", mConf.Foo; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			if exp, act := "custom", mConf.Bar; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			if exp, act := 10, mConf.Baz; exp != act {
				t.Errorf("Wrong config value: %v != %v", act, exp)
			}
			return nil, errTest
		})

	confStr := `{
  "plugins": {
    "foobar": {
      "type": "foo",
      "plugin": {
        "bar": "custom"
      }
    }
  }
}`

	conf := NewConfig()
	if err := json.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	exp := "failed to create plugin resource 'foobar' of type 'foo': test err"
	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if act := err.Error(); act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}
}

func TestYAMLPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (interface{}, error) {
			return nil, errTest
		})

	confStr := `plugins:
  foobar:
    type: foo
    plugin:
      foo: this will be ignored`

	conf := NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	exp := "failed to create plugin resource 'foobar' of type 'foo': test err"
	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if act := err.Error(); act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}
}

func TestJSONPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (interface{}, error) {
			return nil, errTest
		})

	confStr := `{
  "plugins": {
    "foobar": {
      "type": "foo",
      "plugin": {
        "foo": "this will be ignored"
      }
    }
  }
}`

	conf := NewConfig()
	if err := json.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	exp := "failed to create plugin resource 'foobar' of type 'foo': test err"
	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if act := err.Error(); act != exp {
		t.Errorf("Wrong error returned: %v != %v", act, exp)
	}
}
