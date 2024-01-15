package service

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/value"
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
	pluginConfig any,
) (*ParsedConfig, error) {
	// All nested fields are under the namespace of the component type, and
	// therefore we need to namespace the manager such that metrics and logs
	// from nested core component types are corrected labelled.
	if nm != nil {
		nm = nm.IntoPath(componentName)
	}

	if pluginConfig == nil {
		if spec.component.Config.Default != nil {
			pluginConfig = value.IClone(*spec.component.Config.Default)
		} else if len(spec.component.Config.Children) > 0 {
			pluginConfig = map[string]any{}
		}
	}
	return spec.configFromAny(nm, pluginConfig)
}
