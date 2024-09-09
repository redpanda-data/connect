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
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/redpanda-data/benthos/v4/public/service"
	oai "github.com/sashabaranov/go-openai"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockChatClient struct {
	stubClient
}

func (m *mockChatClient) CreateChatCompletion(ctx context.Context, body oai.ChatCompletionRequest) (resp oai.ChatCompletionResponse, err error) {
	resp.ID = faker.UUIDHyphenated()
	resp.Model = body.Model
	resp.Choices = []oai.ChatCompletionChoice{
		{
			Message: oai.ChatCompletionMessage{
				Role:    "assistant",
				Content: faker.Paragraph(),
			},
		},
	}
	return
}

func TestChat(t *testing.T) {
	p := chatProcessor{
		baseProcessor: &baseProcessor{
			client: &mockChatClient{},
			model:  "gpt-4o",
		},
	}
	input := service.NewMessage([]byte(faker.Paragraph()))
	output, err := p.Process(context.Background(), input)
	assert.NoError(t, err)
	assert.Len(t, output, 1)
	msg := output[0]
	require.NoError(t, msg.GetError())
}

func TestChatInterpolationError(t *testing.T) {
	text, err := service.NewInterpolatedString(`${!throw("kaboom!")}`)
	assert.NoError(t, err)
	p := chatProcessor{
		baseProcessor: &baseProcessor{
			client: &mockChatClient{},
			model:  "gpt-4o",
		},
		userPrompt: text,
	}
	input := service.NewMessage([]byte(faker.Paragraph()))
	_, err = p.Process(context.Background(), input)
	assert.Error(t, err)
}
