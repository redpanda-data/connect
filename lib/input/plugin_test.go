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
	if !errors.Is(err, errTest) {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
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
	if !errors.Is(err, errTest) {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}
