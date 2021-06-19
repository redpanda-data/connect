package service

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"gopkg.in/yaml.v3"
)

// NewBatchPolicyField defines a new object type config field that describes a
// batching policy for batched outputs. It is then possible to extract a
// BatchPolicy from the resulting parsed config with the method
// FieldBatchPolicy.
func NewBatchPolicyField(name string) *ConfigField {
	bs := batch.FieldSpec()
	bs.Name = name
	// Remove the "processors" field (for now) and any deprecated fields.
	var newChildren []docs.FieldSpec
	for _, f := range bs.Children {
		if f.Name == "count" {
			f = f.HasDefault(0)
		}
		if f.Name != "processors" && !f.IsDeprecated {
			newChildren = append(newChildren, f)
		}
	}
	bs.Children = newChildren
	return &ConfigField{field: bs}
}

// FieldBatchPolicy accesses a field from a parsed config that was defined with
// NewBatchPolicyField and returns a BatchPolicy, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldBatchPolicy(path ...string) (BatchPolicy, error) {
	v, exists := p.field(path...)
	if !exists {
		return BatchPolicy{}, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return BatchPolicy{}, err
	}

	var conf BatchPolicy
	err := node.Decode(&conf)

	return conf, err
}
