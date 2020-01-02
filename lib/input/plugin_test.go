package input

import (
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
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Input, error) {
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
	if err != errTest {
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

	exp := `Input Plugins
=============

This document was generated with ` + "`benthos --list-input-plugins`" + `.

This document lists any input plugins that this flavour of Benthos offers beyond
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
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Input, error) {
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
	if err != errTest {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}
