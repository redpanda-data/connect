package service

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

type fieldUnwrapper struct {
	child docs.FieldSpec
}

func (f fieldUnwrapper) Unwrap() docs.FieldSpec {
	return f.child
}

// XUnwrapper is for internal use only, do not use this.
func (c *ConfigField) XUnwrapper() any {
	return fieldUnwrapper{child: c.field}
}

func extractConfig(
	nm bundle.NewManagement,
	spec *ConfigSpec,
	componentName string,
	pluginConfig, componentConfig any,
) (*ParsedConfig, error) {
	// All nested fields are under the namespace of the component type, and
	// therefore we need to namespace the manager such that metrics and logs
	// from nested core component types are corrected labelled.
	if nm != nil {
		nm = nm.IntoPath(componentName)
	}

	if pluginConfig != nil {
		return spec.configFromAny(nm, pluginConfig)
	}

	// TODO: V4 We won't need the below fallback once it's not possible to
	// instantiate components in code with NewConfig()
	var n yaml.Node
	if err := n.Encode(componentConfig); err != nil {
		return nil, err
	}

	componentsMap := map[string]yaml.Node{}
	if err := n.Decode(&componentsMap); err != nil {
		return nil, err
	}

	pluginNode, exists := componentsMap[componentName]
	if !exists {
		pluginNode = yaml.Node{}
		_ = pluginNode.Encode(nil)
	}

	return spec.configFromNode(nm, &pluginNode)
}
