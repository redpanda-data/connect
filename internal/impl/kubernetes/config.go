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

package kubernetes

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// CommonFields returns config fields shared across all Kubernetes inputs.
func CommonFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("namespaces").
			Description("Namespaces to watch. Empty list means all namespaces.").
			Default([]any{}).
			Example([]string{"default"}).
			Example([]string{"production", "staging"}),
		service.NewStringField("label_selector").
			Description("Kubernetes label selector to filter resources (e.g., 'app=myapp,env=prod').").
			Default("").
			Example("app=myapp").
			Example("app in (frontend,backend)"),
		service.NewStringField("field_selector").
			Description("Kubernetes field selector to filter resources (e.g., 'status.phase=Running').").
			Default("").
			Optional().
			Advanced(),
		service.NewStringField("request_timeout").
			Description("Timeout for Kubernetes API requests such as list calls. Use \"0s\" to disable.").
			Default("30s").
			Advanced(),
	}
}

// MetadataDescription returns the standard metadata documentation block.
func MetadataDescription(fields []string) string {
	result := `

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
`
	for _, f := range fields {
		result += "- " + f + "\n"
	}
	result += "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).
`
	return result
}
