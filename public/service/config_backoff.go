package service

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	bbackoff "github.com/Jeffail/benthos/v3/lib/util/backoff"
	"github.com/cenkalti/backoff/v4"
	"gopkg.in/yaml.v3"
)

// NewBackoffField defines a new object type config field that describes Backoff
// settings for components. It is then possible to extract a
// github.com/cenkalti/backoff/v4.Backoff from the resulting parsed config
// with the method FieldBackoff.
func NewBackoffField(name string) *ConfigField {
	tf := bbackoff.FieldSpec()
	tf.Name = name
	tf.Type = docs.FieldTypeObject
	var newChildren []docs.FieldSpec
	for _, f := range tf.Children {
		if f.Name != "enabled" {
			newChildren = append(newChildren, f)
		}
	}
	tf.Children = newChildren
	return &ConfigField{field: tf}
}

// FieldBackoff accesses a field from a parsed config that was defined with
// NewBackoffField and returns a github.com/cenkalti/backoff/v4.Backoff, or an
// error if the configuration was invalid.
func (p *ParsedConfig) FieldBackoff(path ...string) (backoff.BackOff, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return nil, err
	}

	conf := bbackoff.NewConfig()
	if err := node.Decode(&conf); err != nil {
		return nil, err
	}

	return conf.Get()
}
