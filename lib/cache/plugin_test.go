package cache_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
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

	cache.RegisterPlugin("foo", newMockPluginConf,
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

	conf := cache.NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := cache.New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}

func TestPluginDescriptions(t *testing.T) {
	cache.RegisterPlugin("foo", newMockPluginConf, nil)
	cache.RegisterPlugin("bar", newMockPluginConf, nil)
	cache.DocumentPlugin("bar", "This is a bar plugin.", func(conf interface{}) interface{} {
		mConf, ok := conf.(*mockPluginConf)
		if !ok {
			t.Fatalf("failed to cast config: %T", conf)
		}
		return map[string]interface{}{
			"foo": mConf.Foo,
			"bar": mConf.Bar,
		}
	})
	cache.RegisterPlugin("foo_no_conf", nil, nil)
	cache.DocumentPlugin("foo_no_conf", "This is a plugin without config.", nil)
	cache.RegisterPlugin("foo_no_conf_no_desc", nil, nil)

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

	act := cache.PluginDescriptions()
	if exp != act {
		t.Logf("Expected:\n%v\n", exp)
		t.Logf("Actual:\n%v\n", act)
		t.Error("Wrong descriptions")
	}
}

func TestYAMLPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	cache.RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Cache, error) {
			return nil, errTest
		})

	confStr := `type: foo
plugin:
  foo: this will be ignored`

	conf := cache.NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := cache.New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}

func TestJSONPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	cache.RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Cache, error) {
			return nil, errTest
		})

	confStr := `{
  "type": "foo",
  "plugin": {
    "foo": "this will be ignored"
  }
}`

	conf := cache.NewConfig()
	if err := json.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	_, err := cache.New(conf, nil, log.Noop(), metrics.Noop())
	if !strings.Contains(err.Error(), "test err") {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}
