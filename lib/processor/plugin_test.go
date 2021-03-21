package processor

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
	errTest2 := errors.New("test err 2")

	RegisterPlugin("foo", newMockPluginConf,
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Processor, error) {
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

	RegisterPlugin("foo_no_conf", nil,
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Processor, error) {
			return nil, errTest2
		})

	confStr := `type: foo
plugin:
  bar: custom`

	conf := NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err != errTest {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}

	confStr = `type: foo_no_conf`
	conf = NewConfig()
	if err := yaml.Unmarshal([]byte(confStr), &conf); err != nil {
		t.Fatal(err)
	}

	if _, err := New(conf, nil, log.Noop(), metrics.Noop()); err != errTest2 {
		t.Errorf("Wrong error returned: %v != %v", err, errTest2)
	}
}

func TestYAMLPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Processor, error) {
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

func TestJSONPluginNilConf(t *testing.T) {
	errTest := errors.New("test err")

	RegisterPlugin("foo", func() interface{} { return &struct{}{} },
		func(conf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Processor, error) {
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
	if err != errTest {
		t.Errorf("Wrong error returned: %v != %v", err, errTest)
	}
}
