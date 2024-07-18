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

package openai

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/ai/azopenai"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	opFieldServerAddress = "server_address"
	opFieldAPIKey        = "api_key"
	opFieldModel         = "model"
)

func baseConfigFieldsWithModels(modelExamples ...any) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(opFieldServerAddress).
			Description("The endpoint used to make API requests. This can be changed to use another OpenAI compatible service.").
			Advanced().
			Default("https://api.openai.com/v1"),
		service.NewStringField(opFieldAPIKey).
			Secret().
			Description("The API Key for OpenAI."),
		service.NewStringField(opFieldModel).
			Description("The OpenAI model to use.").
			Examples(modelExamples...),
	}
}

type baseProcessor struct {
	client client
	model  string
}

func (b *baseProcessor) Close(ctx context.Context) error {
	return nil
}

func newBaseProcessor(conf *service.ParsedConfig) (*baseProcessor, error) {
	sa, err := conf.FieldString(opFieldServerAddress)
	if err != nil {
		return nil, err
	}
	k, err := conf.FieldString(opFieldAPIKey)
	if err != nil {
		return nil, err
	}
	kc := azcore.NewKeyCredential(k)
	c, err := azopenai.NewClientForOpenAI(sa, kc, nil)
	if err != nil {
		return nil, err
	}
	m, err := conf.FieldString(opFieldModel)
	if err != nil {
		return nil, err
	}
	return &baseProcessor{c, m}, nil
}
