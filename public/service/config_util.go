package service

import (
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"gopkg.in/yaml.v3"
)

func extractConfig(
	nm bundle.NewManagement,
	spec *ConfigSpec,
	componentName string,
	pluginConfig, componentConfig interface{},
) (*ParsedConfig, error) {
	if pluginConfig != nil {
		return spec.configFromNode(nm, pluginConfig.(*yaml.Node))
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
