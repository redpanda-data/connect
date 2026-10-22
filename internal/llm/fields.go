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

// Package llm provides config fields shared by the processors that call large
// language model (LLM) APIs.
package llm

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const payloadDefault = "By default, the processor submits the entire payload of each message as a string."

// PromptField returns the interpolated user prompt field of a chat processor.
func PromptField(name string) *service.ConfigField {
	return service.NewInterpolatedStringField(name).
		Description("The user prompt you want to generate a response for. " + payloadDefault)
}

// SystemPromptField returns the interpolated system prompt field of a chat processor.
func SystemPromptField(name string) *service.ConfigField {
	return service.NewInterpolatedStringField(name).
		Description("The system prompt to submit along with the user prompt.")
}

// EmbeddingTextField returns the interpolated input text field of an embeddings processor.
func EmbeddingTextField(name string) *service.ConfigField {
	return service.NewInterpolatedStringField(name).
		Description("The text you want to generate vector embeddings for. " + payloadDefault)
}

// EmbeddingTextMappingField returns the Bloblang input text field of an embeddings processor.
func EmbeddingTextMappingField(name string) *service.ConfigField {
	return service.NewBloblangField(name).
		Description("A Bloblang mapping that returns the text you want to generate vector embeddings for. " + payloadDefault)
}
