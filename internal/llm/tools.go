// Copyright 2026 Redpanda Data, Inc.
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

package llm

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	toolFieldName          = "name"
	toolFieldDesc          = "description"
	toolFieldParams        = "parameters"
	toolParamFieldRequired = "required"
	toolParamFieldProps    = "properties"
	toolParamPropFieldType = "type"
	toolParamPropFieldDesc = "description"
	toolParamPropFieldEnum = "enum"
	toolFieldPipeline      = "processors"
)

// ToolParametersField returns the parameters object of a tool definition.
func ToolParametersField() *service.ConfigField {
	return service.NewObjectField(
		toolFieldParams,
		service.NewStringListField(toolParamFieldRequired).Default([]string{}).Description("The names of the parameters the LLM must provide when it invokes this tool."),
		service.NewObjectMapField(
			toolParamFieldProps,
			service.NewStringField(toolParamPropFieldType).Description("The type of this parameter."),
			service.NewStringField(toolParamPropFieldDesc).Description("A description of this parameter."),
			service.NewStringListField(toolParamPropFieldEnum).Default([]string{}).Description("The values this parameter is limited to. Leave empty to accept any value."),
		).Description("The parameters the LLM can provide when it invokes this tool, keyed by parameter name."),
	).Description("The parameters the LLM needs to provide to invoke this tool.")
}

// ToolFields returns the fields of one tool definition in a chat processor's
// tools list, using params as its parameters field.
func ToolFields(params *service.ConfigField) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(toolFieldName).Description("The name of this tool."),
		service.NewStringField(toolFieldDesc).Description("A description of this tool. The LLM uses it to decide whether to invoke the tool."),
		params,
		service.NewProcessorListField(toolFieldPipeline).Description("The processors to run when the LLM invokes this tool. They receive a message whose payload is the tool call arguments as a JSON object, and their output is returned to the LLM as the tool result.").Optional(),
	}
}
