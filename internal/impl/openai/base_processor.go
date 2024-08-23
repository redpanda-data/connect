// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package openai

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"
	oai "github.com/sashabaranov/go-openai"
)

const (
	opFieldServerAddress = "server_address"
	opFieldAPIKey        = "api_key"
	opFieldModel         = "model"
)

func baseConfigFieldsWithModels(modelExamples ...any) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(opFieldServerAddress).
			Description("The Open API endpoint that the processor sends requests to. Update the default value to use another OpenAI compatible service.").
			Default("https://api.openai.com/v1"),
		service.NewStringField(opFieldAPIKey).
			Secret().
			Description("The API key for OpenAI API."),
		service.NewStringField(opFieldModel).
			Description("The name of the OpenAI model to use.").
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
	cfg := oai.DefaultConfig(k)
	cfg.BaseURL = sa
	c := oai.NewClientWithConfig(cfg)
	m, err := conf.FieldString(opFieldModel)
	if err != nil {
		return nil, err
	}
	return &baseProcessor{c, m}, nil
}
