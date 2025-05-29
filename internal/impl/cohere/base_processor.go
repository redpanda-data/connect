// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cohere

import (
	"context"

	coopt "github.com/cohere-ai/cohere-go/v2/option"
	coherev2 "github.com/cohere-ai/cohere-go/v2/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	cpFieldBaseURL = "base_url"
	cpFieldAPIKey  = "api_key"
	cpFieldModel   = "model"
)

func baseConfigFieldsWithModels(modelExamples ...any) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(cpFieldBaseURL).
			Description("The base URL to use for API requests.").
			Default("https://api.cohere.com"),
		service.NewStringField(cpFieldAPIKey).
			Secret().
			Description("The API key for the Cohere API."),
		service.NewStringField(cpFieldModel).
			Description("The name of the Cohere model to use.").
			Examples(modelExamples...),
	}
}

type baseProcessor struct {
	client *coherev2.Client
	model  string
}

func (*baseProcessor) Close(context.Context) error {
	return nil
}

func newBaseProcessor(conf *service.ParsedConfig) (*baseProcessor, error) {
	bu, err := conf.FieldString(cpFieldBaseURL)
	if err != nil {
		return nil, err
	}
	k, err := conf.FieldString(cpFieldAPIKey)
	if err != nil {
		return nil, err
	}
	c := coherev2.NewClient(
		coopt.WithBaseURL(bu),
		coopt.WithToken(k),
	)
	m, err := conf.FieldString(cpFieldModel)
	if err != nil {
		return nil, err
	}
	return &baseProcessor{c, m}, nil
}
