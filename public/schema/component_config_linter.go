// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	metaFieldTags        = "tags"
	mcpFieldSection      = "mcp"
	mcpFieldEnabled      = "enabled"
	mcpFieldDescription  = "description"
	mcpFieldProperties   = "properties"
	mcpFieldPropName     = "name"
	mcpFieldPropType     = "type"
	mcpFieldPropDesc     = "description"
	mcpFieldPropRequired = "required"
)

func mcpMetaSchema(disableProps bool) *service.ConfigField {
	propsField := service.NewObjectListField(mcpFieldProperties,
		service.NewStringField(mcpFieldPropName),
		service.NewStringEnumField(mcpFieldPropType, "string", "bool", "boolean", "number"),
		service.NewStringField(mcpFieldPropDesc).Default(""),
		service.NewBoolField(mcpFieldPropRequired).Default(false),
	).Default([]any{})
	if disableProps {
		propsField = propsField.LintRule(`if this.type() == "array" && this.length() > 0 { "this component type does not support custom properties" }`)
	}

	mcpFields := []*service.ConfigField{
		service.NewBoolField(mcpFieldEnabled).Default(false),
		service.NewStringField(mcpFieldDescription).Default(""),
		propsField,
	}

	return service.NewObjectField(mcpFieldSection, mcpFields...)
}

// ComponentLinter creates a component config linter that includes mcp specific
// meta fields.
func ComponentLinter(env *service.Environment) *service.ComponentConfigLinter {
	l := env.NewComponentConfigLinter()
	l.SetRequireLabels(true)
	l.SetMetaFieldsFn(func(componentType string) []*service.ConfigField {
		_, disableProps := map[string]struct{}{
			"cache": {},
			"input": {},
		}[componentType]

		return []*service.ConfigField{
			service.NewStringListField(metaFieldTags).Default([]any{}),
			mcpMetaSchema(disableProps),
		}
	})
	return l
}
