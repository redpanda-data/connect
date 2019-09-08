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

package cache

import (
	"encoding/json"
	"errors"
	"strings"
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
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Cache, error) {
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

	confStr := `type: foo
plugin:
  bar: custom`

	conf := NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
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
	RegisterPlugin("foo_no_conf", nil, nil)
	DocumentPlugin("foo_no_conf", "This is a plugin without config.", nil)
	RegisterPlugin("foo_no_conf_no_desc", nil, nil)

	exp := `Cache Plugins
=============

This document was generated with ` + "`benthos --list-cache-plugins`" + `.

This document lists any cache plugins that this flavour of Benthos offers beyond
the standard set.

### Contents

1. [` + "`bar`" + `](#bar)
2. [` + "`foo`" + `](#foo)
3. [` + "`foo_no_conf`" + `](#foo_no_conf)
4. [` + "`foo_no_conf_no_desc`" + `](#foo_no_conf_no_desc)

## ` + "`bar`" + `

` + "``` yaml" + `
type: bar
plugin:
  bar: change this
  foo: default
` + "```" + `

This is a bar plugin.

## ` + "`foo`" + `

` + "``` yaml" + `
type: foo
plugin:
  bar: change this
  baz: 10
  foo: default
` + "```" + `

## ` + "`foo_no_conf`" + `

This is a plugin without config.

## ` + "`foo_no_conf_no_desc`" + `
`

	act := PluginDescriptions()
	if exp != act {
		t.Logf("Expected:\n%v\n", exp)
		t.Logf("Actual:\n%v\n", act)
		t.Error("Wrong descriptions")
	}
}

func TestYAMLPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Cache, error) {
			return nil, errTest
		})

	confStr := `type: foo
plugin:
  foo: this will be ignored`

	conf := NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}

func TestJSONPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Cache, error) {
			return nil, errTest
		})

	confStr := `{
  "type": "foo",
  "plugin": {
    "foo": "this will be ignored"
  }
}`

	conf := NewConfig()
	if err := json.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}
